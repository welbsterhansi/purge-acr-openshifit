#!/usr/bin/env python3
"""ACR Purge — safely removes old images from Azure Container Registry."""
import json
import csv
import time
import argparse
import textwrap
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from azure.containerregistry import ContainerRegistryClient
from azure.core.exceptions import HttpResponseError
from azure.identity import DefaultAzureCredential

MAX_RETRIES = 3

# ============================================================
# CLI ARGUMENTS
# ============================================================
def parse_args():
    parser = argparse.ArgumentParser(
        prog="purge.py",
        description="ACR Purge CLI — safely removes old images from Azure Container Registry"
    )

    parser.add_argument("--registry",      required=True,  help="ACR registry name (e.g. bdsoregistry)")
    parser.add_argument("--prefix",        required=True,  help="Repository prefix to scan (e.g. homebuying/users)")
    parser.add_argument("--keep",          type=int, default=2,    help="Number of newest images to keep per repo (default: 2)")
    parser.add_argument("--max-age-days",  type=int, default=15,   help="Minimum age in days for deletion (default: 15)")
    parser.add_argument("--dry-run",       choices=["true", "false"], default="true", help="true = simulate | false = delete for real (default: true)")
    parser.add_argument("--auto-approve",  action="store_true", default=False, help="Skip interactive confirmation — use in pipelines")
    parser.add_argument("--skip-openshift",   action="store_true", default=False, help="Skip OpenShift verification — use when cluster is not accessible")
    parser.add_argument("--in-cluster",       action="store_true", default=False, help="Load kubeconfig from inside the cluster (for pipeline pods)")
    parser.add_argument("--protected-tags",   default="",          help="Comma-separated tags that are never deleted (e.g. latest,stable,production)")

    return parser.parse_args()


# ============================================================
# ACR CLIENT
# ============================================================
def get_acr_client(registry):
    registry_url = f"{registry}.azurecr.io"
    credential   = DefaultAzureCredential()
    return ContainerRegistryClient(
        endpoint=f"https://{registry_url}",
        credential=credential
    )


# ============================================================
# ACR FUNCTIONS — SDK
# ============================================================
def list_repos_by_prefix(client, prefix):
    # Materialise once — avoids duplicate repos from SDK pagination bugs.
    seen = dict.fromkeys(client.list_repository_names())
    if not prefix:
        return list(seen)
    return [r for r in seen if r == prefix or r.startswith(prefix + "/")]


def get_manifests(client, repo):
    """Return a deduplicated list of manifest dicts for the given repository."""
    seen = {}
    for manifest in client.list_manifest_properties(repo):
        digest = manifest.digest
        if digest in seen:
            continue
        tags_list  = list(manifest.tags or [])
        media_type = (
            getattr(manifest, "media_type", None) or
            getattr(manifest, "manifest_media_type", None)
        )
        # Fallback: tagged manifests with no media_type default to docker v2
        if not media_type and tags_list:
            media_type = "application/vnd.docker.distribution.manifest.v2+json"

        seen[digest] = {
            "digest":    digest,
            "tags":      tags_list,
            "mediaType": media_type,
            "createdOn": manifest.created_on,
        }
    return list(seen.values())


def delete_manifest(client, repo, digest, max_retries=MAX_RETRIES):
    """Delete a manifest, retrying up to max_retries times on HTTP 429."""
    for attempt in range(max_retries):
        try:
            client.delete_manifest(repo, digest)
            return
        except HttpResponseError as e:
            if e.status_code == 429 and attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                raise


# ============================================================
# MANIFEST CLASSIFICATION
# ============================================================
def classify_manifest(media_type, tags):
    """Classify a manifest as docker, oci, signature, orphan, manifest-index, or unknown."""
    mt = (media_type or "").lower()

    if "signature" in mt or "notary" in mt:
        return "signature"
    if not tags:
        return "orphan"
    if "docker.distribution.manifest.v2" in mt:
        return "docker"
    if "oci.image.manifest.v1" in mt:
        return "oci"
    if "manifest.list" in mt or "image.index" in mt:
        return "manifest-index"
    return "unknown"


# ============================================================
# REPOSITORY ANALYSIS — ACR ONLY (NO OPENSHIFT)
# ============================================================
def analyze_repo(client, repo, keep, max_age_days, registry_url, protected_tags=None):
    """
    Fetches manifests from ACR via SDK and applies protection rules.
    Does not call OpenShift — that happens once for all repos together.
    """
    result = {"repository": repo, "images": []}

    try:
        manifests = get_manifests(client, repo)
    except Exception as e:
        print(f"  ❌ Failed to fetch ACR data for '{repo}': {e}")
        return result

    now = datetime.now(timezone.utc)

    for manifest in manifests:
        tags       = manifest.get("tags") or []
        digest     = manifest.get("digest")
        if not digest:
            continue

        media_type  = manifest.get("mediaType")
        full_images = [f"{registry_url}/{repo}:{tag}" for tag in tags]
        created_on  = manifest.get("createdOn")

        if created_on:
            age_days  = (now - created_on.astimezone(timezone.utc)).days
            timestamp = created_on.strftime("%Y-%m-%d %H:%M:%S UTC")
        else:
            age_days  = None
            timestamp = None

        kind = classify_manifest(media_type, tags)

        result["images"].append({
            "digest":           digest,
            "tags":             tags,
            "fullImages":       full_images,
            "mediaType":        media_type,
            "timestamp":        timestamp,
            "ageDays":          age_days,
            "type":             kind,
            "canDelete":        False,
            "protectionReason": None,
            "ocLocation":       None,
        })

    # Initial protection rules
    for img in result["images"]:
        if not img["tags"] and img["timestamp"] is None:
            img["protectionReason"] = "no_tag_and_no_timestamp"
        elif not img["tags"]:
            img["protectionReason"] = "no_tag"
        elif img["timestamp"] is None:
            img["protectionReason"] = "no_timestamp"
        elif img["type"] in ("signature", "orphan", "manifest-index", "unknown"):
            img["protectionReason"] = f"protected_type:{img['type']}"

    # KEEP + age logic
    eligible = [
        img for img in result["images"]
        if img["timestamp"] and img["tags"] and img["type"] in ("docker", "oci")
    ]
    eligible.sort(key=lambda x: (x["ageDays"] is None, x["ageDays"]))

    for img in eligible[:keep]:
        img["protectionReason"] = f"keep_newest:{keep}"

    for img in eligible[keep:]:
        if img["ageDays"] is not None and img["ageDays"] >= max_age_days:
            img["canDelete"] = True
        else:
            img["protectionReason"] = f"too_recent:<{max_age_days}_days"

    # Protected tags override canDelete — applied last so they always win.
    if protected_tags:
        for img in result["images"]:
            if not img["canDelete"]:
                continue
            matched = next((t for t in img["tags"] if t in protected_tags), None)
            if matched:
                img["canDelete"]        = False
                img["protectionReason"] = f"protected_tag:{matched}"

    # Re-sort all images newest-first
    non_null = [img for img in result["images"] if img["timestamp"] is not None]
    null_ts  = [img for img in result["images"] if img["timestamp"] is None]
    non_null.sort(key=lambda x: x["timestamp"], reverse=True)
    result["images"] = non_null + null_ts

    return result


def delete_all_candidates(client, all_results, concurrency=10):
    """Delete all candidate images in parallel, bounded by concurrency."""
    candidates = [
        (r["repository"], img)
        for r in all_results
        for img in r["images"]
        if img["canDelete"]
    ]

    def do_delete(repo, img):
        # Errors are caught per-image so one failure never stops the loop.
        try:
            delete_manifest(client, repo, img["digest"])
            img["deleted"] = True
            print(f"  ✅ {repo}@{img['digest'][:19]}...")
        except Exception as e:
            print(f"  ❌ Failed: {repo}@{img['digest'][:19]}...: {e}")

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(do_delete, repo, img) for repo, img in candidates]
        for f in as_completed(futures):
            f.result()


def analyze_all_repos(client, repos, keep, max_age_days, registry_url, protected_tags=None):
    """Analyze all repositories in parallel using ThreadPoolExecutor."""
    if not repos:
        return []
    results = []
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {
            executor.submit(
                analyze_repo, client, repo, keep, max_age_days,
                registry_url, protected_tags=protected_tags or [],
            ): repo
            for repo in repos
        }
        for future in as_completed(futures):
            results.append(future.result())
    return results


# ============================================================
# PHASE 1 TABLE — DELETION CANDIDATES
# ============================================================
def _print_candidates_table(repo, candidates, max_rows=None):
    """Print a borderless table of deletion candidates, truncating at max_rows if set."""
    COL_TAG    = 45
    COL_DAYS   =  9
    COL_DIGEST = 20

    visible   = candidates if max_rows is None else candidates[:max_rows]
    remaining = len(candidates) - len(visible)

    print(f"     {'tag':<{COL_TAG}}  {'age':>{COL_DAYS}}  {'digest':<{COL_DIGEST}}")
    print(f"     {'─'*COL_TAG}  {'─'*COL_DAYS}  {'─'*COL_DIGEST}")

    for img in visible:
        tag    = min(img["tags"], key=len) if img["tags"] else "<no-tag>"
        days   = f"{img['ageDays']}d" if img["ageDays"] is not None else "N/A"
        digest = img["digest"][:COL_DIGEST]

        if len(tag) > COL_TAG:
            tag = tag[:COL_TAG - 3] + "..."

        print(f"     {tag:<{COL_TAG}}  {days:>{COL_DAYS}}  {digest:<{COL_DIGEST}}")

    if remaining > 0:
        print(f"     ... and {remaining} more images")


# ============================================================
# FINAL SUMMARY — contagem por repositório
# ============================================================
CLUSTER_REASONS = ("running_on_cluster", "in_imagestream_history")


def compute_repo_counts(images):
    """Return (manifests, candidates, cluster, protected, action) for a list of images.

    - manifests  : total manifests in the repository
    - candidates : images that were deletion candidates (would_delete + cluster)
    - cluster    : images protected because they are active on the cluster
    - protected  : images protected by other rules (keep, too_recent, no_tag, …)
    - action     : images that will be / were deleted (would_delete / deleted)
    """
    manifests  = len(images)
    candidates = len([i for i in images if i.get("canDelete") or i.get("deleted") or i.get("protectionReason") in CLUSTER_REASONS])
    cluster    = len([i for i in images if i.get("protectionReason") in CLUSTER_REASONS])
    protected  = len([
        i for i in images
        if not i.get("canDelete") and not i.get("deleted")
        and i.get("protectionReason") not in CLUSTER_REASONS
        and i.get("protectionReason") is not None
    ])
    action     = len([i for i in images if i.get("canDelete") or i.get("deleted")])
    return manifests, candidates, cluster, protected, action


# ============================================================
# REPORTS
# ============================================================
def _get_action(img):
    """Map an image dict to its report action string."""
    if img.get("deleted"):
        return "deleted"
    if img.get("canDelete"):
        return "would_delete"
    reason = img.get("protectionReason", "")
    if reason == "running_on_cluster":
        return "in_use_on_cluster"
    if reason == "in_imagestream_history":
        return "in_imagestream_history_only"
    if reason == "openshift_unreachable":
        return "cluster_unreachable"
    if reason == "openshift_check_failed":
        return "cluster_check_failed"
    return "protected"


def save_json_report(all_results, mode, run_timestamp, registry, prefix):
    filename = f"report-{run_timestamp}.json"

    payload = {
        "run_timestamp": run_timestamp,
        "mode":          mode,
        "registry":      registry,
        "prefix":        prefix if prefix else "*",  # FIX: show "*" instead of "" in reports
        "repositories": [
            {
                "repository":      r["repository"],
                "total_manifests": len(r["images"]),
                "images": [
                    {
                        "digest":            img["digest"],
                        "tags":              img["tags"],
                        "age_days":          img["ageDays"],
                        "created_at":        img["timestamp"] or "N/A",
                        "action":            _get_action(img),
                        "protection_reason": img.get("protectionReason"),
                        "oc_location":       img.get("ocLocation"),
                    }
                    for img in r["images"]
                ]
            }
            for r in all_results
        ]
    }

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    print(f"  📄 JSON : {filename}")


def save_csv_report(all_results, mode, run_timestamp):
    filename = f"report-{run_timestamp}.csv"
    fields   = ["repository", "digest", "tags", "age_days", "created_at", "action", "protection_reason", "oc_location"]

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()

        for r in all_results:
            for img in r["images"]:
                writer.writerow({
                    "repository":        r["repository"],
                    "digest":            img["digest"],
                    "tags":              " | ".join(img["tags"]),
                    "age_days":          img["ageDays"],
                    "created_at":        img["timestamp"] or "N/A",
                    "action":            _get_action(img),
                    "protection_reason": img.get("protectionReason") or "",
                    "oc_location":       img.get("ocLocation") or "",
                })

    print(f"  📄 CSV  : {filename}")


# ============================================================
# SAFETY CONFIRMATION — called AFTER analysis is shown
# ============================================================
def confirm_deletion(
    registry, prefix, keep, max_age_days, dry_run, auto_approve, total_to_delete
):
    """Print a safety summary and, when not auto-approved, prompt for confirmation."""
    mode_label = "DRY-RUN (simulation)" if dry_run else "LIVE (will DELETE images)"
    prefix_str = prefix or "*"
    heading    = "auto-approve" if auto_approve else "confirm before proceeding"

    print(textwrap.dedent(f"""
        ◈  Deletion summary  —  {heading}

           Registry     {registry}
           Prefix       {prefix_str}
           Keep         {keep} images
           Max age      {max_age_days} days
           Mode         {mode_label}
           To delete    {total_to_delete} image(s)
    """).rstrip())

    if not dry_run:
        print("\n  ⚠️  This will PERMANENTLY DELETE images from ACR!")

    if auto_approve:
        print("\n  Auto-approved. Proceeding...\n")
        return

    try:
        confirmation = input("\nType 'DELETE' to confirm: ").strip()
    except (EOFError, KeyboardInterrupt):
        print("\n\n❌ Operation cancelled.")
        exit(1)

    if confirmation != "DELETE":
        print(f"\n❌ Invalid confirmation. Expected 'DELETE', got '{confirmation}'.")
        exit(1)

    print("\n✅ Confirmed. Proceeding...\n")


# ============================================================
# ENTRY POINT
# ============================================================
if __name__ == "__main__":
    args = parse_args()

    REGISTRY       = args.registry
    REGISTRY_URL   = f"{REGISTRY}.azurecr.io"
    PREFIX         = args.prefix
    KEEP           = args.keep
    MAX_AGE_DAYS   = args.max_age_days
    DRY_RUN        = args.dry_run == "true"
    AUTO_APPROVE   = args.auto_approve
    SKIP_OPENSHIFT  = args.skip_openshift
    IN_CLUSTER      = args.in_cluster
    PROTECTED_TAGS  = [t.strip() for t in args.protected_tags.split(",") if t.strip()]

    run_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H%M%S")
    mode          = "DRY_RUN" if DRY_RUN else "LIVE"

    # ── 0. OpenShift connectivity check — BEFORE any ACR analysis ────
    # The OpenShift check is mandatory in ALL modes (dry-run and live).
    # Knowing which images are running in production is essential to
    # produce a safe and accurate report — even in simulation mode.
    # Only --skip-openshift bypasses this, and only when the operator
    # explicitly accepts the risk of running without cluster validation.
    print(f"\n◈  PHASE 0 — OpenShift Connectivity Check\n")

    if SKIP_OPENSHIFT:
        oc_client = None
        print("  ⚠️  OpenShift check skipped (--skip-openshift).")
        print("      WARNING: Images running in production will NOT be protected.")
        print("      Only use this flag when the cluster is confirmed unreachable.")
    else:
        from openshift_client import OpenShiftClient
        try:
            oc_client = OpenShiftClient(in_cluster=IN_CLUSTER, namespace_prefix="")
            if not oc_client.namespaces:
                print(f"\n  🚫 ABORT: Connected to cluster but found 0 accessible namespaces.")
                print(f"     You may be logged into the wrong cluster or lack project access.")
                print(f"     Use --skip-openshift only if you accept running without protection.")
                exit(1)
            print(f"  Cluster state loaded: {len(oc_client._active)} active | {len(oc_client._historical)} historical digest(s)")
            print("  ✅ Connected to OpenShift cluster.")
        except Exception as e:
            print(f"\n  🚫 ABORT: Could not connect to OpenShift cluster: {e}")
            print("     Stopping execution to prevent unsafe deletions.")
            print("     Use --skip-openshift only if you are sure the cluster")
            print("     is unreachable and you accept the risk.")
            exit(1)

    # ── 1. Connect to ACR via SDK ─────────────────────────────
    try:
        client = get_acr_client(REGISTRY)
        repos  = list_repos_by_prefix(client, PREFIX)
    except Exception as e:
        print(f"  ❌ Could not connect to ACR: {e}")
        exit(1)

    repo_word = "repository" if len(repos) == 1 else "repositories"
    print(f"\n  Found {len(repos)} {repo_word} matching prefix '{PREFIX or '*'}'")

    # ── 2. Analyze all repos in ACR ──────────────────────────
    print(f"\n◈  PHASE 1 — ACR Analysis\n")

    all_results = analyze_all_repos(client, repos, KEEP, MAX_AGE_DAYS, REGISTRY_URL,
                                    protected_tags=PROTECTED_TAGS)

    # Ordenar pelo nome do repo para output determinístico
    all_results.sort(key=lambda r: r["repository"])

    for result in all_results:
        candidates = [img for img in result["images"] if img["canDelete"]]
        print(f"  🔎 {result['repository']}  →  {len(result['images'])} manifests  ·  {len(candidates)} candidates")
        if candidates:
            _print_candidates_table(result["repository"], candidates, max_rows=50)
            print()

    # ── 3. Collect all candidate digests from all repos ──────
    all_digests = [
        img["digest"]
        for r in all_results
        for img in r["images"]
        if img["canDelete"]
    ]

    # ── 4. Apply OpenShift results to all repos ───────────────
    print(f"\n◈  PHASE 2 — OpenShift Verification  ({len(all_digests)} digest(s))\n")

    if oc_client is not None:
        for r in all_results:
            for img in r["images"]:
                if not img["canDelete"]:
                    continue
                try:
                    status, location = oc_client.check_digest(img["digest"])
                except Exception as e:
                    print(f"  ⚠️  check_digest failed for {img['digest'][:19]}...: {e} → marking as protected")
                    img["canDelete"]        = False
                    img["protectionReason"] = "openshift_check_failed"
                    img["ocLocation"]       = None
                    continue

                if status == "active":
                    img["canDelete"]        = False
                    img["protectionReason"] = "running_on_cluster"
                    img["ocLocation"]       = location
                    label = "[running in pod]" if "/ pod /" in location else "[referenced in workload]"
                    print(f"  ⚠️  {img['digest'][:19]}... → {location}  {label}")
                elif status == "historical":
                    img["canDelete"]        = False
                    img["protectionReason"] = "in_imagestream_history"
                    img["ocLocation"]       = location
                    print(f"  ⚠️  {img['digest'][:19]}... → {location}  [in imagestream]")
                else:
                    print(f"  ✅ {img['digest'][:19]}... [not in cluster — safe to delete]")

        # ── 5. Warn if scan was partial (some namespaces returned 403/401) ────
        if oc_client.permission_incomplete:
            print(
                "  ⚠️  WARNING: Cluster scan was partial — some namespaces had permission errors.\n"
                "      Images not found in accessible namespaces are treated as safe to delete.\n"
                "      Ensure your service account has 'list pods' access to all relevant namespaces."
            )
    else:
        print("  ⚠️  Skipped — no cluster verification performed.")

    # ── 6. Ask for confirmation AFTER showing all analysis ────
    final_to_delete = [
        img
        for r in all_results
        for img in r["images"]
        if img["canDelete"]
    ]

    if not DRY_RUN:
        confirm_deletion(
            REGISTRY, PREFIX, KEEP, MAX_AGE_DAYS, DRY_RUN, AUTO_APPROVE,
            total_to_delete=len(final_to_delete)
        )

    # ── 7. Live deletion ──────────────────────────────────────
    if not DRY_RUN:
        print(f"\n◈  PHASE 3 — Deletion\n")
        delete_all_candidates(client, all_results, concurrency=10)

    # ── 8. Final summary ──────────────────────────────────────
    C = (45, 9, 10, 8, 10, 10)   # column widths

    print(f"\n◈  Final Summary  ·  prefix: {PREFIX or '*'}  ·  mode: {mode}\n")
    print(f"  {'repository':<{C[0]}}  {'manifests':>{C[1]}}  {'candidates':>{C[2]}}  {'cluster':>{C[3]}}  {'protected':>{C[4]}}  {'action':>{C[5]}}")
    print(f"  {'─'*C[0]}  {'─'*C[1]}  {'─'*C[2]}  {'─'*C[3]}  {'─'*C[4]}  {'─'*C[5]}")

    total_manifests  = 0
    total_candidates = 0
    total_cluster    = 0
    total_protected  = 0
    total_action     = 0

    for r in all_results:
        manifests, candidates, cluster, protected, action = compute_repo_counts(r["images"])

        total_manifests  += manifests
        total_candidates += candidates
        total_cluster    += cluster
        total_protected  += protected
        total_action     += action

        print(f"  {r['repository']:<{C[0]}}  {manifests:>{C[1]}}  {candidates:>{C[2]}}  {cluster:>{C[3]}}  {protected:>{C[4]}}  {action:>{C[5]}}")

    print(f"  {'─'*C[0]}  {'─'*C[1]}  {'─'*C[2]}  {'─'*C[3]}  {'─'*C[4]}  {'─'*C[5]}")
    print(f"  {'TOTAL':<{C[0]}}  {total_manifests:>{C[1]}}  {total_candidates:>{C[2]}}  {total_cluster:>{C[3]}}  {total_protected:>{C[4]}}  {total_action:>{C[5]}}")
    print(f"\n  keep={KEEP}  ·  max-age-days={MAX_AGE_DAYS}  ·  mode={mode}")

    # ── 9. Save reports ───────────────────────────────────────
    print(f"\n◈  Reports\n")
    save_json_report(all_results, mode, run_timestamp, REGISTRY, PREFIX)
    save_csv_report(all_results, mode, run_timestamp)

    print(f"\nDone.")
