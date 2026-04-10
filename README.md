# ACR Purge — Image Cleanup for Azure Container Registry + OpenShift

Two scripts that work together to safely remove old images from ACR while protecting anything running in OpenShift.

---

## How it works

```
openshift_prune.py          ← prune ImageStream history (old revisions within a tag)
        ↓
purge.py                    ← delete old ACR images (protected by cluster check)
        ↓
az acr gc --registry <r>    ← free physical storage in ACR
```

**Safety guarantees:**
- `--dry-run true` is the default in both scripts — nothing is deleted unless you explicitly pass `--dry-run false`
- Images running in pods are never deleted
- Images referenced in workloads (DC, Deployment, StatefulSet, Job, etc.) are never deleted
- Images in ImageStream history are protected until pruned there first
- `--protected-tags` ensures specific tags are never deleted regardless of age

---

## `purge.py` — ACR image cleanup

### Parameters

| Parameter | Default | Description |
|---|---|---|
| `--registry` | required | ACR name (e.g. `bdsoregistry`) |
| `--prefix` | required | Repo prefix to scan (e.g. `homebuying/`) |
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
  --registry bdsoregistry \
  --prefix homebuying/ \
  --keep 10 \
  --max-age-days 15 \
  --protected-tags latest,stable,production \
  --dry-run true
```

**Dry run — single repository:**
```bash
python3 purge.py \
  --registry bdsoregistry \
  --prefix homebuying/frontend-react \
  --keep 10 \
  --max-age-days 15 \
  --dry-run true
```

**Live run — apply deletions:**
```bash
python3 purge.py \
  --registry bdsoregistry \
  --prefix homebuying/ \
  --keep 10 \
  --max-age-days 15 \
  --protected-tags latest,stable,production \
  --dry-run false
```

**Inside a pipeline pod (kubeconfig from ServiceAccount):**
```bash
python3 purge.py \
  --registry bdsoregistry \
  --prefix homebuying/ \
  --keep 10 \
  --max-age-days 15 \
  --in-cluster \
  --auto-approve \
  --dry-run false
```

**Skip OpenShift check (cluster unreachable — use with caution):**
```bash
python3 purge.py \
  --registry bdsoregistry \
  --prefix homebuying/ \
  --keep 10 \
  --max-age-days 15 \
  --skip-openshift \
  --dry-run true
```

---

## `openshift_prune.py` — ImageStream history cleanup

Removes old revisions **within** each ImageStream tag. Useful when teams push to the same tag repeatedly (e.g. `latest`, `stable`) and history accumulates.

> **Note:** If your team uses one unique tag per release (e.g. `5.0.0-release-20250912-181618`),
> each tag has only one revision — this script will find 0 candidates. Use `oc adm prune images` instead.

### Parameters

| Parameter | Default | Description |
|---|---|---|
| `--keep-revisions` | `10` | Historical revisions to keep per tag |
| `--younger-than` | `24h` | Preserve entries newer than this window (h, m, d) |
| `--namespace-prefix` | `prd-` | Only scan namespaces with this prefix |
| `--protected-tags` | `""` | Tags never pruned (e.g. `latest,stable,production`) |
| `--dry-run` | `true` | `true` = simulate \| `false` = delete for real |
| `--in-cluster` | `false` | Load kubeconfig from inside a pod |

### Examples

**Dry run — all namespaces matching `prd-`:**
```bash
python3 openshift_prune.py \
  --keep-revisions 10 \
  --younger-than 24h \
  --protected-tags latest,stable,production \
  --dry-run true
```

**Dry run — single namespace:**
```bash
python3 openshift_prune.py \
  --namespace-prefix prd-wealthmanagement \
  --keep-revisions 10 \
  --younger-than 24h \
  --dry-run true
```

**Live run — apply deletions:**
```bash
python3 openshift_prune.py \
  --namespace-prefix prd-wealthmanagement \
  --keep-revisions 10 \
  --younger-than 24h \
  --protected-tags latest,stable,production \
  --dry-run false
```

**Inside a pipeline pod:**
```bash
python3 openshift_prune.py \
  --in-cluster \
  --keep-revisions 10 \
  --younger-than 24h \
  --dry-run false
```

---

## Recommended execution order

```bash
# 1. Clean old ImageStream tags (one-tag-per-release pattern)
oc adm prune images --keep-tag-revisions=10 --keep-younger-than=60m --confirm

# 2. Clean ImageStream history within remaining tags
python3 openshift_prune.py \
  --keep-revisions 10 \
  --younger-than 24h \
  --dry-run false

# 3. Dry run ACR cleanup — review candidates first
python3 purge.py \
  --registry bdsoregistry \
  --prefix homebuying/ \
  --keep 10 \
  --max-age-days 15 \
  --dry-run true

# 4. Apply ACR deletions
python3 purge.py \
  --registry bdsoregistry \
  --prefix homebuying/ \
  --keep 10 \
  --max-age-days 15 \
  --dry-run false

# 5. Free physical storage in ACR
az acr gc --registry bdsoregistry
```

---

## Understanding the output

### `purge.py` PHASE 2 labels

| Label | Meaning |
|---|---|
| `[running in pod]` | Digest found in a running pod — never delete |
| `[referenced in workload]` | Digest in a DC, Deployment, Job, Build, etc. |
| `[in imagestream]` | Digest exists as an ImageStream/IST entry — clean ImageStream first |
| `[not in cluster — safe to delete]` | No cluster reference found — candidate for deletion |

### What `openshift_client.py` scans

**Active (running workloads):** Pods, Deployments, StatefulSets, DaemonSets, ReplicaSets, DeploymentConfigs, ReplicationControllers, Jobs, CronJobs, Builds, BuildConfigs

**Historical (registry catalog):** ImageStreams, ImageStreamTags

> ImageStream entries are `historical` by design — their mere existence does not prove a pod is running the image.
