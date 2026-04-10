#!/usr/bin/env python3
"""
openshift_prune.py — Safely prunes ImageStream history in OpenShift.

Preserves:
  1. items[0]              — current entry for each tag (always active)
  2. active digests        — any digest running in pods, DCs, jobs, etc.
  3. recent entries        — created within --younger-than window
  4. last N revisions      — --keep-revisions per tag
  5. protected tag names   — --protected-tags (e.g. latest,stable,production)

Only entries where ALL 5 conditions are false are candidates for pruning.
Always dry-run by default. Zero-downtime guarantee by design.
"""

import argparse
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
# CORE DECISION
# ============================================================

def should_prune(entry, tag_name, position, active_digests,
                 keep_revisions, younger_than, protected_tags):
    """
    Decide whether a single ImageStream history entry is safe to prune.

    Returns (can_prune: bool, reason: str).
    can_prune is True only when ALL 5 safety conditions are false.
    """
    digest = entry.image

    # Condition 1 — current entry (position 0) is always preserved.
    if position == 0:
        return False, "current entry for tag"

    # Condition 2 — digest is actively running in the cluster.
    if digest in active_digests:
        return False, f"active in cluster: {digest}"

    # Condition 3 — entry is within the younger-than safety window.
    created    = datetime.fromisoformat(entry.created).astimezone(timezone.utc)
    age        = datetime.now(timezone.utc) - created
    if age < younger_than:
        return False, f"recent entry (age={age})"

    # Condition 4 — entry is within the keep-revisions window.
    # position 0 is already protected by condition 1.
    # keep_revisions=2 protects positions 1 and 2.
    if 0 < position <= keep_revisions:
        return False, f"within keep-revisions={keep_revisions} (position={position})"

    # Condition 5 — tag name is in the protected list.
    if tag_name in (protected_tags or []):
        return False, f"protected_tag:{tag_name}"

    return True, "eligible for pruning"


# ============================================================
# CANDIDATE COLLECTION
# ============================================================

def collect_prune_candidates(imagestreams, active_digests,
                              keep_revisions, younger_than, protected_tags):
    """
    Scan a list of ImageStream objects and return all history entries
    that are safe to prune.

    Each candidate is a dict:
      { namespace, imagestream, tag, digest, dockerImageReference, reason }
    """
    candidates = []

    for is_obj in imagestreams:
        ns   = is_obj.metadata.namespace
        name = is_obj.metadata.name

        for tag in (getattr(is_obj.status, "tags", None) or []):
            tag_name = tag.tag
            items    = getattr(tag, "items", None) or []

            for position, entry in enumerate(items):
                can_prune, reason = should_prune(
                    entry          = entry,
                    tag_name       = tag_name,
                    position       = position,
                    active_digests = active_digests,
                    keep_revisions = keep_revisions,
                    younger_than   = younger_than,
                    protected_tags = protected_tags,
                )
                if can_prune:
                    candidates.append({
                        "namespace":            ns,
                        "imagestream":          name,
                        "tag":                  tag_name,
                        "digest":               entry.image,
                        "dockerImageReference": entry.dockerImageReference,
                        "reason":               reason,
                    })

    return candidates


# ============================================================
# DELETION
# ============================================================

def delete_istag_entry(istag_api, namespace, is_name, tag, digest):
    """Delete one ImageStreamTag history entry via the OpenShift API."""
    istag_api.delete(
        name      = f"{is_name}:{tag}@{digest}",
        namespace = namespace,
    )


# ============================================================
# ORCHESTRATION
# ============================================================

def run_prune(oc_client, keep_revisions, younger_than, protected_tags, dry_run):
    """
    Scan all namespaces in oc_client, collect prune candidates and
    delete them unless dry_run is True.

    Returns the list of candidates found.
    """
    active_digests = set(oc_client._active.keys())
    all_candidates = []

    for ns in oc_client.namespaces:
        imagestreams = oc_client.is_api.get(namespace=ns).items
        candidates   = collect_prune_candidates(
            imagestreams   = imagestreams,
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
            delete_istag_entry(
                oc_client.istag_api,
                c["namespace"],
                c["imagestream"],
                c["tag"],
                c["digest"],
            )
            print(f"  ✅ {c['namespace']} / {c['imagestream']}:{c['tag']}@{c['digest'][:19]}...")
        except Exception as e:
            print(f"  ❌ Failed: {c['namespace']} / {c['imagestream']}:{c['tag']}@{c['digest'][:19]}...: {e}")

    return all_candidates


# ============================================================
# OUTPUT FORMATTING
# ============================================================

def print_settings(mode, keep_revisions, younger_than, protected_tags):
    """Print the header and settings section."""
    tags_str = ", ".join(protected_tags) if protected_tags else "(none)"
    print(f"\n◈  OpenShift ImageStream Prune  ·  mode: {mode}\n")
    print(f"   Settings:")
    print(f"     keep-revisions:  {keep_revisions}")
    print(f"     younger-than:    {younger_than}")
    print(f"     protected-tags:  {tags_str}")


def print_summary(candidates, dry_run, namespace_prefix,
                  keep_revisions, younger_than, protected_tags):
    """Print the summary section after pruning."""
    count     = len(candidates)
    no_match  = "  ✅ (No images match the pruning criteria)" if count == 0 else ""
    tags_arg  = f" --protected-tags {','.join(protected_tags)}" if protected_tags else ""

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
        description = "Safely prune ImageStream history in OpenShift."
    )
    parser.add_argument(
        "--keep-revisions",   type=int, default=10,
        help="Number of historical entries to keep per tag (default: 10)",
    )
    parser.add_argument(
        "--younger-than",     default="24h",
        help="Preserve entries newer than this window (default: 24h). Supports h, m, d.",
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
        mode           = mode,
        keep_revisions = KEEP_REVISIONS,
        younger_than   = args.younger_than,
        protected_tags = PROTECTED_TAGS,
    )

    try:
        oc = OpenShiftClient(in_cluster=IN_CLUSTER)
    except Exception as e:
        print(f"\n  🚫 ABORT: Could not connect to OpenShift cluster: {e}")
        sys.exit(1)

    print(f"\n   Status:")
    print(f"     Cluster state:   {len(oc._active)} active | {len(oc._historical)} historical digests loaded.")

    candidates = run_prune(
        oc_client      = oc,
        keep_revisions = KEEP_REVISIONS,
        younger_than   = YOUNGER_THAN,
        protected_tags = PROTECTED_TAGS,
        dry_run        = DRY_RUN,
    )

    print_summary(
        candidates       = candidates,
        dry_run          = DRY_RUN,
        namespace_prefix = NS_PREFIX,
        keep_revisions   = KEEP_REVISIONS,
        younger_than     = args.younger_than,
        protected_tags   = PROTECTED_TAGS,
    )
    print(f"\nDone.")
