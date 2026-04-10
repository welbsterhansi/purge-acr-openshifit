#!/usr/bin/env python3
"""
openshift_prune.py — Safely prunes old ImageStream tags in OpenShift.

For the one-tag-per-release pattern (e.g. dotnet-service-trading:5.6.4),
this script removes entire old tags from ImageStreams so that purge.py
can then delete the unreferenced images from ACR.

Preserves:
  1. Rank-0 tag (newest per stream)  — always active
  2. Keep-N most recent tags         — --keep-revisions per stream
  3. Active digests                  — any digest running in pods, DCs, jobs, etc.
  4. Recent tags                     — created within --younger-than window
  5. Protected tag names             — --protected-tags (e.g. latest,stable,production)

Only tags where ALL 5 conditions are false are candidates for deletion.
Always dry-run by default. Zero-downtime guarantee by design.
"""

import argparse
import json
import sys
from datetime import datetime, timezone, timedelta


# ============================================================
# TIME PARSING
# ============================================================

def parse_younger_than(value):
    """Parse a time string like '24h', '60m', '7d' into a timedelta."""
    if value.endswith("h"):
        return timedelta(hours=int(value[:-1]))
    if value.endswith("m"):
        return timedelta(minutes=int(value[:-1]))
    if value.endswith("d"):
        return timedelta(days=int(value[:-1]))
    raise ValueError(f"Invalid time format '{value}'. Use h, m, or d (e.g. 24h, 60m, 7d).")


# ============================================================
# HELPERS
# ============================================================

def _parse_timestamp(ts):
    """Parse creationTimestamp (string or datetime) to a UTC-aware datetime."""
    if isinstance(ts, str):
        return datetime.fromisoformat(ts).astimezone(timezone.utc)
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            return ts.replace(tzinfo=timezone.utc)
        return ts.astimezone(timezone.utc)
    return datetime.min.replace(tzinfo=timezone.utc)


def _extract_digest(docker_image_reference):
    """Extract sha256:... from a dockerImageReference string."""
    if not docker_image_reference or "sha256:" not in docker_image_reference:
        return None
    try:
        sha = docker_image_reference.split("sha256:")[1].split()[0].strip("\"'@")
        return f"sha256:{sha}"
    except Exception:
        return None


# ============================================================
# GROUP TAGS BY STREAM
# ============================================================

def group_tags_by_stream(ist_items):
    """
    Groups ImageStreamTag items by stream name, sorted newest-first.

    Each IST has metadata.name = '{stream}:{tag}'.

    Returns: {"stream-name": [ist_newest, ist_older, ...]}
    """
    groups = {}
    for ist in ist_items:
        parts = ist.metadata.name.split(":", 1)
        if len(parts) != 2:
            continue
        stream = parts[0]
        if stream not in groups:
            groups[stream] = []
        groups[stream].append(ist)

    for stream in groups:
        groups[stream].sort(
            key=lambda i: _parse_timestamp(i.metadata.creationTimestamp),
            reverse=True,
        )

    return groups


# ============================================================
# CANDIDATE COLLECTION
# ============================================================

def collect_tag_candidates(grouped_tags, active_digests,
                           keep_revisions, younger_than, protected_tags):
    """
    For each stream, keep the N most recent tags, return the rest as candidates.

    Safety conditions (any true → skip):
      1. Tag is rank-0 (newest) — always kept
      2. Tag is within keep_revisions (newest N)
      3. Digest is in active_digests
      4. Tag was created within younger_than
      5. Tag name is in protected_tags

    Returns list of: {namespace, stream, tag, digest, reason}
    """
    candidates = []
    now = datetime.now(timezone.utc)

    for stream, ists in grouped_tags.items():
        for rank, ist in enumerate(ists):
            tag    = ist.metadata.name.split(":", 1)[1]
            ns     = ist.metadata.namespace
            digest = _extract_digest(getattr(ist.image, "dockerImageReference", ""))
            age    = now - _parse_timestamp(ist.metadata.creationTimestamp)

            # Condition 1: within keep window (N most recent)
            if rank < keep_revisions:
                continue

            # Condition 3: active digest
            if digest and digest in (active_digests or set()):
                continue

            # Condition 4: recent entry
            if age < younger_than:
                continue

            # Condition 5: protected tag name
            if tag in (protected_tags or []):
                continue

            candidates.append({
                "namespace": ns,
                "stream":    stream,
                "tag":       tag,
                "digest":    digest or "",
                "reason":    "eligible for pruning",
            })

    return candidates


# ============================================================
# DELETION
# ============================================================

def delete_istag(istag_api, namespace, stream, tag):
    """Delete an entire ImageStreamTag (removes the tag from the ImageStream)."""
    istag_api.delete(
        name      = f"{stream}:{tag}",
        namespace = namespace,
    )


# ============================================================
# ORCHESTRATION
# ============================================================

def run_prune(oc_client, keep_revisions, younger_than, protected_tags, dry_run):
    """
    Scan all namespaces in oc_client, collect old tag candidates and
    delete them unless dry_run is True.

    Returns the list of candidates found.
    """
    active_digests = set(oc_client._active.keys())
    all_candidates = []

    for ns in oc_client.namespaces:
        ist_items  = oc_client.istag_api.get(namespace=ns).items
        grouped    = group_tags_by_stream(ist_items)
        candidates = collect_tag_candidates(
            grouped_tags   = grouped,
            active_digests = active_digests,
            keep_revisions = keep_revisions,
            younger_than   = younger_than,
            protected_tags = protected_tags,
        )
        all_candidates.extend(candidates)

    if dry_run:
        return all_candidates

    for c in all_candidates:
        try:
            delete_istag(
                oc_client.istag_api,
                c["namespace"],
                c["stream"],
                c["tag"],
            )
            print(f"  ✅ {c['namespace']} / {c['stream']}:{c['tag']}")
        except Exception as e:
            print(f"  ❌ Failed: {c['namespace']} / {c['stream']}:{c['tag']}: {e}")

    return all_candidates


# ============================================================
# OUTPUT FORMATTING
# ============================================================

def print_settings(mode, namespace_prefix, keep_revisions, younger_than, protected_tags):
    """Print the header and settings section."""
    tags_str = ", ".join(protected_tags) if protected_tags else "(none)"
    print(f"\n◈  OpenShift ImageStream Prune  ·  mode: {mode}\n")
    print(f"   Settings:")
    print(f"     namespace-prefix: {namespace_prefix}")
    print(f"     keep-revisions:   {keep_revisions}")
    print(f"     younger-than:     {younger_than}")
    print(f"     protected-tags:   {tags_str}")


def print_candidates_table(candidates, max_rows=50):
    """Print the table of tags eligible for deletion."""
    if not candidates:
        return

    print(f"\n◈  Candidates — tags eligible for deletion\n")
    col_ns     = max(len(c["namespace"]) for c in candidates)
    col_stream = max(len(c["stream"])    for c in candidates)

    shown = candidates[:max_rows]
    for c in shown:
        print(f"   {c['namespace']:<{col_ns}}  {c['stream']:<{col_stream}}  {c['tag']}")

    remaining = len(candidates) - len(shown)
    if remaining > 0:
        print(f"\n   ... and {remaining} more tags")


def print_stream_summary(stats):
    """Print per-stream breakdown: stream | total | kept | candidates."""
    if not stats:
        return

    sorted_stats = sorted(stats, key=lambda s: s["candidates"], reverse=True)

    col_ns     = max(len(s["namespace"]) for s in sorted_stats)
    col_stream = max(len(s["stream"])    for s in sorted_stats)

    print(f"\n◈  Stream breakdown\n")
    print(f"   {'Namespace':<{col_ns}}  {'Stream':<{col_stream}}  {'Total':>6}  {'Kept':>6}  {'Candidates':>10}")
    print(f"   {'-'*col_ns}  {'-'*col_stream}  {'------':>6}  {'------':>6}  {'----------':>10}")

    for s in sorted_stats:
        kept = s["total"] - s["candidates"]
        print(
            f"   {s['namespace']:<{col_ns}}  {s['stream']:<{col_stream}}"
            f"  {s['total']:>6}  {kept:>6}  {s['candidates']:>10}"
        )


def save_json_report(candidates, stream_stats, mode, run_timestamp,
                     namespace_prefix, keep_revisions, younger_than, protected_tags):
    """Save a JSON report file and return the filename."""
    payload = {
        "run_timestamp":    run_timestamp,
        "mode":             mode,
        "namespace_prefix": namespace_prefix,
        "keep_revisions":   keep_revisions,
        "younger_than":     younger_than,
        "protected_tags":   protected_tags,
        "candidates":       candidates,
        "stream_summary":   stream_stats,
    }
    filename = f"report-prune-{run_timestamp}.json"
    with open(filename, "w") as f:
        json.dump(payload, f, indent=2, default=str)
    return filename


def print_summary(candidates, dry_run, namespace_prefix,
                  keep_revisions, younger_than, protected_tags):
    """Print the summary section after pruning."""
    count    = len(candidates)
    no_match = "  ✅ (No images match the pruning criteria)" if count == 0 else ""
    tags_arg = f" --protected-tags {','.join(protected_tags)}" if protected_tags else ""

    print(f"\n◈  Summary\n")
    print(f"   Candidates found:  {count}{no_match}")

    if dry_run:
        cmd = (
            f"python3 openshift_prune.py"
            f" --namespace-prefix {namespace_prefix}"
            f" --keep-revisions {keep_revisions}"
            f" --younger-than {younger_than}"
            f"{tags_arg}"
            f" --dry-run false"
        )
        print(f"\n   To apply changes, run:")
        print(f"   {cmd}")


# ============================================================
# CLI
# ============================================================

def parse_args():
    parser = argparse.ArgumentParser(
        prog        = "openshift_prune.py",
        description = "Safely prune old ImageStream tags in OpenShift."
    )
    parser.add_argument(
        "--keep-revisions",   type=int, default=10,
        help="Number of most recent tags to keep per ImageStream (default: 10)",
    )
    parser.add_argument(
        "--younger-than",     default="24h",
        help="Preserve tags newer than this window (default: 24h). Supports h, m, d.",
    )
    parser.add_argument(
        "--namespace-prefix", default="prd-",
        help="Only scan namespaces with this prefix (default: prd-)",
    )
    parser.add_argument(
        "--protected-tags",   default="",
        help="Comma-separated tag names never pruned (e.g. latest,stable,production)",
    )
    parser.add_argument(
        "--dry-run",          type=lambda v: v.lower() != "false", default=True,
        help="true = simulate | false = delete for real (default: true)",
    )
    parser.add_argument(
        "--in-cluster",       action="store_true", default=False,
        help="Load kubeconfig from inside the cluster (for pipeline pods)",
    )
    return parser.parse_args()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    args           = parse_args()
    KEEP_REVISIONS = args.keep_revisions
    YOUNGER_THAN   = parse_younger_than(args.younger_than)
    PROTECTED_TAGS = [t.strip() for t in args.protected_tags.split(",") if t.strip()]
    DRY_RUN        = args.dry_run
    IN_CLUSTER     = args.in_cluster
    NS_PREFIX      = args.namespace_prefix
    mode           = "DRY_RUN" if DRY_RUN else "LIVE"

    from openshift_client import OpenShiftClient

    print_settings(
        mode             = mode,
        namespace_prefix = NS_PREFIX,
        keep_revisions   = KEEP_REVISIONS,
        younger_than     = args.younger_than,
        protected_tags   = PROTECTED_TAGS,
    )

    try:
        oc = OpenShiftClient(in_cluster=IN_CLUSTER, namespace_prefix=NS_PREFIX)
    except Exception as e:
        print(f"\n  🚫 ABORT: Could not connect to OpenShift cluster: {e}")
        sys.exit(1)

    # Filter to only the requested namespace prefix ("" = all prd-* namespaces)
    oc.namespaces = [ns for ns in oc.namespaces if ns.startswith(NS_PREFIX)]
    ns_label = f"{NS_PREFIX}*" if NS_PREFIX else "all prd-*"

    if not oc.namespaces:
        print(f"\n  🚫 ABORT: No namespaces found matching prefix '{NS_PREFIX}'.")
        print(f"     Check that the prefix is correct and you have cluster access.")
        sys.exit(1)

    print(f"\n   Status:")
    print(f"     Cluster state:   {len(oc._active)} active | {len(oc._historical)} historical digests loaded.")
    print(f"     Namespaces:      {len(oc.namespaces)}  ({ns_label})")

    candidates = run_prune(
        oc_client      = oc,
        keep_revisions = KEEP_REVISIONS,
        younger_than   = YOUNGER_THAN,
        protected_tags = PROTECTED_TAGS,
        dry_run        = DRY_RUN,
    )

    # Build per-stream stats
    from collections import defaultdict
    stream_totals     = defaultdict(lambda: {"namespace": "", "total": 0})
    stream_candidates = defaultdict(int)

    for ns in oc.namespaces:
        try:
            ist_items = oc.istag_api.get(namespace=ns).items
            for ist in ist_items:
                parts = ist.metadata.name.split(":", 1)
                if len(parts) == 2:
                    key = (ns, parts[0])
                    stream_totals[key]["namespace"] = ns
                    stream_totals[key]["total"]    += 1
        except Exception:
            pass

    for c in candidates:
        stream_candidates[(c["namespace"], c["stream"])] += 1

    stream_stats = [
        {
            "namespace":  ns,
            "stream":     stream,
            "total":      stream_totals[(ns, stream)]["total"],
            "candidates": stream_candidates[(ns, stream)],
        }
        for (ns, stream) in stream_totals
    ]

    print_stream_summary(stream_stats)
    print_candidates_table(candidates)

    run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    report_path = save_json_report(
        candidates       = candidates,
        stream_stats     = stream_stats,
        mode             = mode,
        run_timestamp    = run_timestamp,
        namespace_prefix = NS_PREFIX,
        keep_revisions   = KEEP_REVISIONS,
        younger_than     = args.younger_than,
        protected_tags   = PROTECTED_TAGS,
    )
    print(f"\n   Report saved: {report_path}")

    print_summary(
        candidates       = candidates,
        dry_run          = DRY_RUN,
        namespace_prefix = NS_PREFIX,
        keep_revisions   = KEEP_REVISIONS,
        younger_than     = args.younger_than,
        protected_tags   = PROTECTED_TAGS,
    )
    print(f"\nDone.")
