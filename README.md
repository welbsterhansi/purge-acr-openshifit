# ACR Purge — Image Cleanup for Azure Container Registry + OpenShift

Two scripts that work together to safely remove old images from ACR while protecting anything running in OpenShift.

---

## How it works

```
openshift_prune.py          ← prune old ImageStream tags (one-tag-per-release pattern)
        ↓
purge.py                    ← delete old ACR images (protected by cluster check)
        ↓
az acr gc --registry <r>    ← free physical storage in ACR
```

**Safety guarantees:**
- `--dry-run true` is the default in both scripts — nothing is deleted unless you explicitly pass `--dry-run false`
- Images running in pods are never deleted
- Images referenced in workloads (DC, Deployment, StatefulSet, Job, etc.) are never deleted
- Images referenced in ImageStream tags are protected until pruned there first
- `--protected-tags` ensures specific tags are never deleted regardless of age
- Active digests from **all** `prd-*` namespaces are loaded regardless of `--namespace-prefix` — a digest used anywhere in the cluster is always protected

---

## `openshift_prune.py` — ImageStream tag cleanup

Removes old **entire tags** from each ImageStream. Designed for the one-tag-per-release pattern where each release gets its own unique tag (e.g. `dotnet-service-trading:5.6.4`).

Run this **before** `purge.py` so that old tags are removed from OpenShift and `purge.py` can then safely clean ACR.

### Parameters

| Parameter | Default | Description |
|---|---|---|
| `--keep-revisions` | `10` | Most recent tags to keep per ImageStream |
| `--younger-than` | `24h` | Preserve tags newer than this window (h, m, d) |
| `--namespace-prefix` | `prd-` | Only prune namespaces with this prefix. Pass `""` to scan all `prd-*` namespaces |
| `--protected-tags` | `""` | Tags never pruned (e.g. `latest,stable,production`) |
| `--dry-run` | `true` | `true` = simulate \| `false` = delete for real |
| `--in-cluster` | `false` | Load kubeconfig from inside a pod |

### Namespace filtering

```bash
# Single namespace
--namespace-prefix prd-contoso-payments
# → Namespaces: 1  (prd-contoso-payments*)

# All prd-* namespaces
--namespace-prefix ""
# → Namespaces: 15  (all prd-*)
```

### Examples

**Dry run — single namespace:**
```bash
python3 openshift_prune.py \
  --namespace-prefix prd-contoso-payments \
  --keep-revisions 2 \
  --younger-than 24h \
  --dry-run true
```

**Dry run — all prd-* namespaces:**
```bash
python3 openshift_prune.py \
  --namespace-prefix "" \
  --keep-revisions 2 \
  --younger-than 24h \
  --protected-tags latest,stable \
  --dry-run true
```

**Live run — single namespace:**
```bash
python3 openshift_prune.py \
  --namespace-prefix prd-contoso-payments \
  --keep-revisions 2 \
  --younger-than 24h \
  --protected-tags latest,stable \
  --dry-run false
```

**Live run — all prd-* namespaces:**
```bash
python3 openshift_prune.py \
  --namespace-prefix "" \
  --keep-revisions 2 \
  --younger-than 24h \
  --protected-tags latest,stable \
  --dry-run false
```

**Inside a pipeline pod:**
```bash
python3 openshift_prune.py \
  --in-cluster \
  --namespace-prefix "" \
  --keep-revisions 2 \
  --younger-than 24h \
  --dry-run false
```

---

## `purge.py` — ACR image cleanup

### Parameters

| Parameter | Default | Description |
|---|---|---|
| `--registry` | required | ACR name (e.g. `contosoregistry`) |
| `--prefix` | required | Repo prefix to scan (e.g. `payments/`) |
| `--keep` | `2` | Most recent images to keep per repo |
| `--max-age-days` | `15` | Minimum age in days for deletion candidacy |
| `--dry-run` | `true` | `true` = simulate \| `false` = delete for real |
| `--auto-approve` | `false` | Skip interactive confirmation (use in pipelines) |
| `--skip-openshift` | `false` | Skip cluster check (only when cluster is unreachable) |
| `--in-cluster` | `false` | Load kubeconfig from inside a pod |
| `--protected-tags` | `""` | Tags never deleted (e.g. `latest,stable,production`) |

### Examples

**Dry run — full prefix, all namespaces:**
```bash
python3 purge.py \
  --registry contosoregistry \
  --prefix payments/ \
  --keep 2 \
  --max-age-days 15 \
  --protected-tags latest,stable \
  --dry-run true
```

**Dry run — single repository:**
```bash
python3 purge.py \
  --registry contosoregistry \
  --prefix payments/frontend-react \
  --keep 2 \
  --max-age-days 15 \
  --dry-run true
```

**Live run — apply deletions:**
```bash
python3 purge.py \
  --registry contosoregistry \
  --prefix payments/ \
  --keep 2 \
  --max-age-days 15 \
  --protected-tags latest,stable \
  --dry-run false
```

**Inside a pipeline pod (kubeconfig from ServiceAccount):**
```bash
python3 purge.py \
  --registry contosoregistry \
  --prefix payments/ \
  --keep 2 \
  --max-age-days 15 \
  --in-cluster \
  --auto-approve \
  --dry-run false
```

**Skip OpenShift check (cluster unreachable — use with caution):**
```bash
python3 purge.py \
  --registry contosoregistry \
  --prefix payments/ \
  --keep 2 \
  --max-age-days 15 \
  --skip-openshift \
  --dry-run true
```

---

## Recommended execution order

```bash
# 1. Dry run — review which ImageStream tags would be removed (single namespace)
python3 openshift_prune.py \
  --namespace-prefix prd-contoso-payments \
  --keep-revisions 2 \
  --younger-than 24h \
  --dry-run true

# 2. Apply ImageStream tag cleanup
python3 openshift_prune.py \
  --namespace-prefix prd-contoso-payments \
  --keep-revisions 2 \
  --younger-than 24h \
  --dry-run false

# 3. Dry run ACR cleanup — review candidates first
python3 purge.py \
  --registry contosoregistry \
  --prefix payments/ \
  --keep 2 \
  --max-age-days 15 \
  --dry-run true

# 4. Apply ACR deletions
python3 purge.py \
  --registry contosoregistry \
  --prefix payments/ \
  --keep 2 \
  --max-age-days 15 \
  --dry-run false

# 5. Free physical storage in ACR
az acr gc --registry contosoregistry
```

---

## Understanding the output

### `openshift_prune.py` output sections

```
◈  OpenShift ImageStream Prune  ·  mode: DRY_RUN

   Settings:
     namespace-prefix: prd-contoso-payments
     keep-revisions:   2
     younger-than:     24h
     protected-tags:   (none)

   Status:
     Cluster state:   943 active | 983 historical digests loaded.
     Namespaces:      1  (prd-contoso-payments*)

◈  Candidates — tags eligible for deletion

   prd-contoso-payments  dotnet-service-trading   1.0.0
   prd-contoso-payments  dotnet-service-trading   1.1.0
   ...
   ... and N more tags

◈  Summary

   Candidates found: 42

   To apply changes, run:
   python3 openshift_prune.py --namespace-prefix prd-contoso-payments ...
```

> **Note:** Tags not listed as candidates are protected — either within `--keep-revisions`, active in the cluster, newer than `--younger-than`, or in `--protected-tags`.

### `purge.py` PHASE 2 labels

| Label | Meaning |
|---|---|
| `[running in pod]` | Digest found in a running pod — never delete |
| `[referenced in workload]` | Digest in a DC, Deployment, Job, Build, etc. |
| `[in imagestream]` | Digest exists as an ImageStream tag — clean OpenShift first |
| `[not in cluster — safe to delete]` | No cluster reference found — candidate for deletion |

### What `openshift_client.py` scans

**Active (running workloads):** Pods, Deployments, StatefulSets, DaemonSets, ReplicaSets, DeploymentConfigs, ReplicationControllers, Jobs, CronJobs, Builds, BuildConfigs

**Historical (registry catalog):** ImageStreams, ImageStreamTags

> ImageStream entries are `historical` by design — their mere existence does not prove a pod is running the image. Real protection comes from pods and workloads.
