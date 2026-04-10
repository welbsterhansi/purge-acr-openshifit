#!/usr/bin/env python3
"""
test_openshift_prune.py — TDD suite for openshift_prune.py

Coverage:
  - should_prune         : all 5 safety conditions
  - collect_prune_candidates : full ImageStream scan logic
  - parse_younger_than   : time string parsing (24h, 60m)
"""

import sys
import unittest
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

from openshift_prune import (
    should_prune,
    collect_prune_candidates,
    parse_younger_than,
    delete_istag_entry,
    run_prune,
    parse_args,
    print_settings,
    print_summary,
)


# ══════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════

def _entry(digest, created_days_ago=30):
    """Builds a minimal ImageStream history entry."""
    created = datetime.now(timezone.utc) - timedelta(days=created_days_ago)
    e = MagicMock()
    e.image           = digest
    e.dockerImageReference = f"registry.azurecr.io/repo@{digest}"
    e.created         = created.isoformat()
    return e


def _run_should_prune(
    entry,
    tag_name       = "v1.0.0",
    position       = 5,
    active_digests = None,
    keep_revisions = 3,
    younger_than   = timedelta(hours=24),
    protected_tags = None,
):
    return should_prune(
        entry          = entry,
        tag_name       = tag_name,
        position       = position,
        active_digests = active_digests or set(),
        keep_revisions = keep_revisions,
        younger_than   = younger_than,
        protected_tags = protected_tags or [],
    )


# ══════════════════════════════════════════════════════════════════
# should_prune — condition 1: position 0 is always current
# ══════════════════════════════════════════════════════════════════

class TestShouldPrunePosition(unittest.TestCase):

    def test_position_zero_is_never_pruned(self):
        """items[0] is the current entry — must never be pruned."""
        entry = _entry("sha256:current", created_days_ago=999)
        can_prune, _ = _run_should_prune(entry, position=0, keep_revisions=0)
        self.assertFalse(can_prune)

    def test_position_zero_reason_mentions_current(self):
        """Reason must explain why position 0 is protected."""
        entry = _entry("sha256:current", created_days_ago=999)
        _, reason = _run_should_prune(entry, position=0, keep_revisions=0)
        self.assertIn("current", reason.lower())

    def test_position_one_with_no_other_protection_is_prunable(self):
        """position=1, old, not active, outside keep window → can prune."""
        entry = _entry("sha256:old", created_days_ago=90)
        can_prune, _ = _run_should_prune(
            entry,
            position       = 1,
            keep_revisions = 0,
            younger_than   = timedelta(hours=1),
        )
        self.assertTrue(can_prune)


# ══════════════════════════════════════════════════════════════════
# should_prune — condition 2: active digests
# ══════════════════════════════════════════════════════════════════

class TestShouldPruneActiveDigest(unittest.TestCase):

    def test_active_digest_is_never_pruned(self):
        """Digest currently running in a pod must never be pruned."""
        entry = _entry("sha256:running", created_days_ago=90)
        can_prune, _ = _run_should_prune(
            entry,
            position       = 5,
            active_digests = {"sha256:running"},
            keep_revisions = 0,
            younger_than   = timedelta(hours=1),
        )
        self.assertFalse(can_prune)

    def test_active_digest_reason_mentions_active(self):
        entry = _entry("sha256:running", created_days_ago=90)
        _, reason = _run_should_prune(
            entry,
            position       = 5,
            active_digests = {"sha256:running"},
            keep_revisions = 0,
            younger_than   = timedelta(hours=1),
        )
        self.assertIn("active", reason.lower())

    def test_inactive_digest_not_protected_by_this_condition(self):
        """Digest not in active_digests passes this condition (may still be pruned)."""
        entry = _entry("sha256:old", created_days_ago=90)
        can_prune, _ = _run_should_prune(
            entry,
            position       = 5,
            active_digests = {"sha256:other"},
            keep_revisions = 0,
            younger_than   = timedelta(hours=1),
        )
        self.assertTrue(can_prune)


# ══════════════════════════════════════════════════════════════════
# should_prune — condition 3: younger-than window
# ══════════════════════════════════════════════════════════════════

class TestShouldPruneYoungerThan(unittest.TestCase):

    def test_recent_entry_is_not_pruned(self):
        """Entry created within the younger-than window must be preserved."""
        entry = _entry("sha256:new", created_days_ago=0)
        can_prune, _ = _run_should_prune(
            entry,
            position       = 5,
            keep_revisions = 0,
            younger_than   = timedelta(hours=24),
        )
        self.assertFalse(can_prune)

    def test_recent_entry_reason_mentions_age(self):
        entry = _entry("sha256:new", created_days_ago=0)
        _, reason = _run_should_prune(
            entry,
            position       = 5,
            keep_revisions = 0,
            younger_than   = timedelta(hours=24),
        )
        self.assertIn("recent", reason.lower())

    def test_old_entry_is_not_protected_by_age(self):
        """Entry older than the window passes this condition."""
        entry = _entry("sha256:old", created_days_ago=90)
        can_prune, _ = _run_should_prune(
            entry,
            position       = 5,
            keep_revisions = 0,
            younger_than   = timedelta(hours=24),
        )
        self.assertTrue(can_prune)


# ══════════════════════════════════════════════════════════════════
# should_prune — condition 4: keep-revisions
# ══════════════════════════════════════════════════════════════════

class TestShouldPruneKeepRevisions(unittest.TestCase):

    def test_within_keep_revisions_not_pruned(self):
        """Entry at position < keep_revisions must be preserved."""
        entry = _entry("sha256:recent-enough", created_days_ago=90)
        can_prune, _ = _run_should_prune(
            entry,
            position       = 2,
            keep_revisions = 10,
            younger_than   = timedelta(hours=1),
        )
        self.assertFalse(can_prune)

    def test_within_keep_revisions_reason_mentions_revisions(self):
        entry = _entry("sha256:recent-enough", created_days_ago=90)
        _, reason = _run_should_prune(
            entry,
            position       = 2,
            keep_revisions = 10,
            younger_than   = timedelta(hours=1),
        )
        self.assertIn("revision", reason.lower())

    def test_beyond_keep_revisions_is_prunable(self):
        """Entry at position > keep_revisions passes this condition."""
        entry = _entry("sha256:old", created_days_ago=90)
        can_prune, _ = _run_should_prune(
            entry,
            position       = 11,
            keep_revisions = 10,
            younger_than   = timedelta(hours=1),
        )
        self.assertTrue(can_prune)


# ══════════════════════════════════════════════════════════════════
# should_prune — condition 5: protected tag names
# ══════════════════════════════════════════════════════════════════

class TestShouldPruneProtectedTags(unittest.TestCase):

    def test_protected_tag_name_is_never_pruned(self):
        """Entry under a protected tag name must never be pruned."""
        entry = _entry("sha256:old", created_days_ago=90)
        can_prune, _ = _run_should_prune(
            entry,
            tag_name       = "latest",
            position       = 5,
            keep_revisions = 0,
            younger_than   = timedelta(hours=1),
            protected_tags = ["latest", "stable"],
        )
        self.assertFalse(can_prune)

    def test_protected_tag_reason_mentions_tag_name(self):
        entry = _entry("sha256:old", created_days_ago=90)
        _, reason = _run_should_prune(
            entry,
            tag_name       = "production",
            position       = 5,
            keep_revisions = 0,
            younger_than   = timedelta(hours=1),
            protected_tags = ["production"],
        )
        self.assertIn("production", reason)

    def test_non_protected_tag_not_affected(self):
        """Tag not in protected_tags passes this condition."""
        entry = _entry("sha256:old", created_days_ago=90)
        can_prune, _ = _run_should_prune(
            entry,
            tag_name       = "v1.0.0",
            position       = 5,
            keep_revisions = 0,
            younger_than   = timedelta(hours=1),
            protected_tags = ["latest", "stable"],
        )
        self.assertTrue(can_prune)

    def test_empty_protected_tags_no_effect(self):
        entry = _entry("sha256:old", created_days_ago=90)
        can_prune, _ = _run_should_prune(
            entry,
            position       = 5,
            keep_revisions = 0,
            younger_than   = timedelta(hours=1),
            protected_tags = [],
        )
        self.assertTrue(can_prune)


# ══════════════════════════════════════════════════════════════════
# should_prune — AND semantics: all conditions must be false to prune
# ══════════════════════════════════════════════════════════════════

class TestShouldPruneAndSemantics(unittest.TestCase):

    def test_all_conditions_false_entry_is_pruned(self):
        """Only when ALL 5 conditions are false should the entry be pruned."""
        entry = _entry("sha256:old", created_days_ago=90)
        can_prune, _ = _run_should_prune(
            entry,
            tag_name       = "v0.0.1",
            position       = 99,
            active_digests = set(),
            keep_revisions = 0,
            younger_than   = timedelta(hours=1),
            protected_tags = [],
        )
        self.assertTrue(can_prune)

    def test_one_condition_true_prevents_pruning(self):
        """A single protecting condition is enough to block pruning."""
        entry = _entry("sha256:old", created_days_ago=90)
        # Only active_digests protects — all others would allow pruning
        can_prune, _ = _run_should_prune(
            entry,
            position       = 99,
            active_digests = {"sha256:old"},
            keep_revisions = 0,
            younger_than   = timedelta(hours=1),
        )
        self.assertFalse(can_prune)


# ══════════════════════════════════════════════════════════════════
# parse_younger_than — time string parsing
# ══════════════════════════════════════════════════════════════════

class TestParseYoungerThan(unittest.TestCase):

    def test_hours_suffix(self):
        self.assertEqual(parse_younger_than("24h"), timedelta(hours=24))

    def test_minutes_suffix(self):
        self.assertEqual(parse_younger_than("60m"), timedelta(minutes=60))

    def test_days_suffix(self):
        self.assertEqual(parse_younger_than("7d"), timedelta(days=7))

    def test_invalid_suffix_raises(self):
        with self.assertRaises(ValueError):
            parse_younger_than("10x")

    def test_zero_hours(self):
        self.assertEqual(parse_younger_than("0h"), timedelta(hours=0))


# ══════════════════════════════════════════════════════════════════
# collect_prune_candidates — full ImageStream scan
# ══════════════════════════════════════════════════════════════════

class TestCollectPruneCandidates(unittest.TestCase):

    def _make_is_obj(self, ns, name, tags_dict):
        """
        tags_dict = {"tagname": [digest1, digest2, ...]}
        First digest = items[0] (current), rest = history.
        """
        is_obj = MagicMock()
        is_obj.metadata.namespace = ns
        is_obj.metadata.name      = name

        tag_list = []
        for tag_name, digests in tags_dict.items():
            tag = MagicMock()
            tag.tag   = tag_name
            tag.items = [_entry(d, created_days_ago=90) for d in digests]
            tag_list.append(tag)

        is_obj.status.tags = tag_list
        return is_obj

    def test_current_entry_never_candidate(self):
        """items[0] must never appear in candidates."""
        is_obj = self._make_is_obj("prd-ns", "myapp", {
            "v1": ["sha256:current", "sha256:old1", "sha256:old2"],
        })
        candidates = collect_prune_candidates(
            imagestreams   = [is_obj],
            active_digests = set(),
            keep_revisions = 0,
            younger_than   = timedelta(hours=1),
            protected_tags = [],
        )
        digests = {c["digest"] for c in candidates}
        self.assertNotIn("sha256:current", digests)

    def test_old_history_entries_are_candidates(self):
        """items[1+] that pass all safety checks must appear in candidates."""
        is_obj = self._make_is_obj("prd-ns", "myapp", {
            "v1": ["sha256:current", "sha256:old1", "sha256:old2"],
        })
        candidates = collect_prune_candidates(
            imagestreams   = [is_obj],
            active_digests = set(),
            keep_revisions = 0,
            younger_than   = timedelta(hours=1),
            protected_tags = [],
        )
        digests = {c["digest"] for c in candidates}
        self.assertIn("sha256:old1", digests)
        self.assertIn("sha256:old2", digests)

    def test_active_digest_excluded_from_candidates(self):
        """Active digest in history must not be a candidate."""
        is_obj = self._make_is_obj("prd-ns", "myapp", {
            "v1": ["sha256:current", "sha256:still-running", "sha256:old"],
        })
        candidates = collect_prune_candidates(
            imagestreams   = [is_obj],
            active_digests = {"sha256:still-running"},
            keep_revisions = 0,
            younger_than   = timedelta(hours=1),
            protected_tags = [],
        )
        digests = {c["digest"] for c in candidates}
        self.assertNotIn("sha256:still-running", digests)

    def test_keep_revisions_limits_candidates(self):
        """With keep_revisions=2, only entries beyond position 2 are candidates."""
        is_obj = self._make_is_obj("prd-ns", "myapp", {
            "v1": ["sha256:c", "sha256:h1", "sha256:h2", "sha256:h3", "sha256:h4"],
        })
        candidates = collect_prune_candidates(
            imagestreams   = [is_obj],
            active_digests = set(),
            keep_revisions = 2,
            younger_than   = timedelta(hours=1),
            protected_tags = [],
        )
        digests = {c["digest"] for c in candidates}
        self.assertNotIn("sha256:h1", digests)   # position 1 — within keep
        self.assertNotIn("sha256:h2", digests)   # position 2 — within keep
        self.assertIn("sha256:h3",    digests)   # position 3 — beyond keep
        self.assertIn("sha256:h4",    digests)   # position 4 — beyond keep

    def test_protected_tag_excluded_entirely(self):
        """All entries under a protected tag must be excluded."""
        is_obj = self._make_is_obj("prd-ns", "myapp", {
            "latest": ["sha256:c", "sha256:old1", "sha256:old2"],
        })
        candidates = collect_prune_candidates(
            imagestreams   = [is_obj],
            active_digests = set(),
            keep_revisions = 0,
            younger_than   = timedelta(hours=1),
            protected_tags = ["latest"],
        )
        self.assertEqual(candidates, [])

    def test_candidate_contains_namespace_and_imagestream(self):
        """Each candidate must carry namespace, imagestream name, tag and digest."""
        is_obj = self._make_is_obj("prd-homebuying", "users", {
            "v1": ["sha256:current", "sha256:old"],
        })
        candidates = collect_prune_candidates(
            imagestreams   = [is_obj],
            active_digests = set(),
            keep_revisions = 0,
            younger_than   = timedelta(hours=1),
            protected_tags = [],
        )
        self.assertEqual(len(candidates), 1)
        c = candidates[0]
        self.assertEqual(c["namespace"],   "prd-homebuying")
        self.assertEqual(c["imagestream"], "users")
        self.assertEqual(c["tag"],         "v1")
        self.assertEqual(c["digest"],      "sha256:old")

    def test_multiple_imagestreams_all_scanned(self):
        """Candidates from multiple ImageStream objects must all be collected."""
        is1 = self._make_is_obj("prd-ns", "app1", {"v1": ["sha256:c1", "sha256:o1"]})
        is2 = self._make_is_obj("prd-ns", "app2", {"v1": ["sha256:c2", "sha256:o2"]})
        candidates = collect_prune_candidates(
            imagestreams   = [is1, is2],
            active_digests = set(),
            keep_revisions = 0,
            younger_than   = timedelta(hours=1),
            protected_tags = [],
        )
        digests = {c["digest"] for c in candidates}
        self.assertIn("sha256:o1", digests)
        self.assertIn("sha256:o2", digests)

    def test_no_history_no_candidates(self):
        """ImageStream with only items[0] (no history) produces no candidates."""
        is_obj = self._make_is_obj("prd-ns", "myapp", {
            "v1": ["sha256:current"],
        })
        candidates = collect_prune_candidates(
            imagestreams   = [is_obj],
            active_digests = set(),
            keep_revisions = 0,
            younger_than   = timedelta(hours=1),
            protected_tags = [],
        )
        self.assertEqual(candidates, [])


# ══════════════════════════════════════════════════════════════════
# delete_istag_entry — deletes one history entry via OpenShift API
# ══════════════════════════════════════════════════════════════════

class TestDeleteIstagEntry(unittest.TestCase):

    def test_calls_api_with_correct_name_format(self):
        """Name must be '{imagestream}:{tag}@{digest}' — OpenShift's own format."""
        istag_api = MagicMock()
        delete_istag_entry(istag_api, "prd-ns", "myapp", "v1", "sha256:abc")
        istag_api.delete.assert_called_once_with(
            name="myapp:v1@sha256:abc",
            namespace="prd-ns",
        )

    def test_calls_api_with_correct_namespace(self):
        """Namespace must be forwarded exactly as given."""
        istag_api = MagicMock()
        delete_istag_entry(istag_api, "prd-homebuying", "users", "latest", "sha256:xyz")
        _, kwargs = istag_api.delete.call_args
        self.assertEqual(kwargs["namespace"], "prd-homebuying")

    def test_propagates_api_exception(self):
        """Errors from the API must not be swallowed."""
        istag_api = MagicMock()
        istag_api.delete.side_effect = Exception("API error")
        with self.assertRaises(Exception):
            delete_istag_entry(istag_api, "prd-ns", "myapp", "v1", "sha256:abc")


# ══════════════════════════════════════════════════════════════════
# run_prune — orchestration with dry-run and output
# ══════════════════════════════════════════════════════════════════

def _make_oc_client(active=None, imagestreams_by_ns=None):
    """
    Returns a minimal mock of OpenShiftClient suitable for run_prune tests.
    imagestreams_by_ns: {namespace: [is_obj, ...]}
    """
    oc = MagicMock()
    oc._active    = {d: "loc" for d in (active or [])}
    oc.namespaces = list((imagestreams_by_ns or {}).keys())

    def fake_is_get(namespace):
        result = MagicMock()
        result.items = (imagestreams_by_ns or {}).get(namespace, [])
        return result

    oc.is_api.get.side_effect = fake_is_get
    return oc


def _make_is_with_history(ns, name, tag, digests):
    """ImageStream with items[0]=current + historical entries."""
    is_obj = MagicMock()
    is_obj.metadata.namespace = ns
    is_obj.metadata.name      = name

    tag_mock       = MagicMock()
    tag_mock.tag   = tag
    tag_mock.items = [_entry(d, created_days_ago=90) for d in digests]

    is_obj.status.tags = [tag_mock]
    return is_obj


class TestRunPrune(unittest.TestCase):

    def _prune(self, oc, dry_run=True, keep_revisions=0,
               younger_than="1h", protected_tags=None):
        return run_prune(
            oc_client      = oc,
            keep_revisions = keep_revisions,
            younger_than   = parse_younger_than(younger_than),
            protected_tags = protected_tags or [],
            dry_run        = dry_run,
        )

    def test_dry_run_never_calls_delete(self):
        """dry_run=True must not call delete_istag_entry."""
        is_obj = _make_is_with_history("prd-ns", "app", "v1",
                                       ["sha256:current", "sha256:old"])
        oc = _make_oc_client(imagestreams_by_ns={"prd-ns": [is_obj]})

        with patch("openshift_prune.delete_istag_entry") as mock_del:
            self._prune(oc, dry_run=True)

        mock_del.assert_not_called()

    def test_dry_run_returns_candidates(self):
        """dry_run=True must return the list of candidates found."""
        is_obj = _make_is_with_history("prd-ns", "app", "v1",
                                       ["sha256:current", "sha256:old"])
        oc = _make_oc_client(imagestreams_by_ns={"prd-ns": [is_obj]})

        candidates = self._prune(oc, dry_run=True)

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["digest"], "sha256:old")

    def test_live_run_calls_delete_for_each_candidate(self):
        """dry_run=False must call delete_istag_entry once per candidate."""
        is_obj = _make_is_with_history("prd-ns", "app", "v1",
                                       ["sha256:c", "sha256:h1", "sha256:h2"])
        oc = _make_oc_client(imagestreams_by_ns={"prd-ns": [is_obj]})

        with patch("openshift_prune.delete_istag_entry") as mock_del:
            self._prune(oc, dry_run=False)

        self.assertEqual(mock_del.call_count, 2)

    def test_live_run_delete_called_with_correct_args(self):
        """delete_istag_entry must receive istag_api, ns, is_name, tag, digest."""
        is_obj = _make_is_with_history("prd-homebuying", "users", "v1",
                                       ["sha256:current", "sha256:old"])
        oc = _make_oc_client(imagestreams_by_ns={"prd-homebuying": [is_obj]})

        with patch("openshift_prune.delete_istag_entry") as mock_del:
            self._prune(oc, dry_run=False)

        mock_del.assert_called_once_with(
            oc.istag_api, "prd-homebuying", "users", "v1", "sha256:old",
        )

    def test_no_candidates_delete_never_called(self):
        """ImageStream with only current entry produces no deletions."""
        is_obj = _make_is_with_history("prd-ns", "app", "v1", ["sha256:only"])
        oc = _make_oc_client(imagestreams_by_ns={"prd-ns": [is_obj]})

        with patch("openshift_prune.delete_istag_entry") as mock_del:
            self._prune(oc, dry_run=False)

        mock_del.assert_not_called()

    def test_active_digest_not_deleted(self):
        """Digest running in cluster must not be deleted even in live mode."""
        is_obj = _make_is_with_history("prd-ns", "app", "v1",
                                       ["sha256:current", "sha256:running"])
        oc = _make_oc_client(
            active                = ["sha256:running"],
            imagestreams_by_ns    = {"prd-ns": [is_obj]},
        )

        with patch("openshift_prune.delete_istag_entry") as mock_del:
            self._prune(oc, dry_run=False)

        mock_del.assert_not_called()

    def test_multiple_namespaces_all_scanned(self):
        """All namespaces in oc.namespaces must be scanned."""
        is1 = _make_is_with_history("prd-ns1", "app", "v1", ["sha256:c1", "sha256:o1"])
        is2 = _make_is_with_history("prd-ns2", "app", "v1", ["sha256:c2", "sha256:o2"])
        oc  = _make_oc_client(imagestreams_by_ns={
            "prd-ns1": [is1],
            "prd-ns2": [is2],
        })

        candidates = self._prune(oc, dry_run=True)

        digests = {c["digest"] for c in candidates}
        self.assertIn("sha256:o1", digests)
        self.assertIn("sha256:o2", digests)

    def test_delete_failure_does_not_stop_remaining(self):
        """A single delete failure must not stop other deletions."""
        is_obj = _make_is_with_history("prd-ns", "app", "v1",
                                       ["sha256:c", "sha256:fail", "sha256:ok"])
        oc = _make_oc_client(imagestreams_by_ns={"prd-ns": [is_obj]})

        calls = []

        def selective_fail(api, ns, is_name, tag, digest):
            calls.append(digest)
            if digest == "sha256:fail":
                raise Exception("API error")

        with patch("openshift_prune.delete_istag_entry", side_effect=selective_fail), \
             patch("sys.stdout"):
            self._prune(oc, dry_run=False)

        self.assertIn("sha256:ok", calls)


# ══════════════════════════════════════════════════════════════════
# parse_args — CLI argument defaults and parsing
# ══════════════════════════════════════════════════════════════════

class TestParseArgs(unittest.TestCase):

    def _parse(self, argv):
        with patch("sys.argv", ["openshift_prune.py"] + argv):
            return parse_args()

    def test_dry_run_default_is_true(self):
        """--dry-run must default to true — safe by default."""
        args = self._parse([])
        self.assertTrue(args.dry_run)

    def test_dry_run_false_when_explicitly_set(self):
        args = self._parse(["--dry-run", "false"])
        self.assertFalse(args.dry_run)

    def test_keep_revisions_default(self):
        """--keep-revisions must default to 10."""
        args = self._parse([])
        self.assertEqual(args.keep_revisions, 10)

    def test_keep_revisions_custom(self):
        args = self._parse(["--keep-revisions", "5"])
        self.assertEqual(args.keep_revisions, 5)

    def test_younger_than_default(self):
        """--younger-than must default to '24h'."""
        args = self._parse([])
        self.assertEqual(args.younger_than, "24h")

    def test_namespace_prefix_default(self):
        """--namespace-prefix must default to 'prd-'."""
        args = self._parse([])
        self.assertEqual(args.namespace_prefix, "prd-")

    def test_protected_tags_default_empty(self):
        """--protected-tags must default to empty string."""
        args = self._parse([])
        self.assertEqual(args.protected_tags, "")

    def test_protected_tags_parsed_as_string(self):
        """--protected-tags accepts a comma-separated string."""
        args = self._parse(["--protected-tags", "latest,stable,production"])
        self.assertEqual(args.protected_tags, "latest,stable,production")

    def test_in_cluster_default_false(self):
        args = self._parse([])
        self.assertFalse(args.in_cluster)

    def test_in_cluster_flag(self):
        args = self._parse(["--in-cluster"])
        self.assertTrue(args.in_cluster)


# ══════════════════════════════════════════════════════════════════
# Output formatting — print_settings / print_summary
# ══════════════════════════════════════════════════════════════════

class TestPrintSettings(unittest.TestCase):

    def _capture(self, mode="DRY_RUN", keep_revisions=10,
                 younger_than="24h", protected_tags=None):
        import io
        buf = io.StringIO()
        with patch("sys.stdout", buf):
            print_settings(
                mode           = mode,
                keep_revisions = keep_revisions,
                younger_than   = younger_than,
                protected_tags = protected_tags or [],
            )
        return buf.getvalue()

    def test_header_contains_mode(self):
        output = self._capture(mode="DRY_RUN")
        self.assertIn("DRY_RUN", output)

    def test_settings_section_label(self):
        output = self._capture()
        self.assertIn("Settings:", output)

    def test_keep_revisions_with_colon(self):
        output = self._capture(keep_revisions=10)
        self.assertIn("keep-revisions:", output)
        self.assertIn("10", output)

    def test_younger_than_with_colon(self):
        output = self._capture(younger_than="24h")
        self.assertIn("younger-than:", output)
        self.assertIn("24h", output)

    def test_protected_tags_none_label(self):
        output = self._capture(protected_tags=[])
        self.assertIn("protected-tags:", output)
        self.assertIn("(none)", output)

    def test_protected_tags_shown_when_set(self):
        output = self._capture(protected_tags=["latest", "stable"])
        self.assertIn("latest", output)
        self.assertIn("stable", output)


class TestPrintSummary(unittest.TestCase):

    def _capture(self, candidates=None, dry_run=True,
                 namespace_prefix="prd-", keep_revisions=10,
                 younger_than="24h", protected_tags=None):
        import io
        buf = io.StringIO()
        with patch("sys.stdout", buf):
            print_summary(
                candidates       = candidates or [],
                dry_run          = dry_run,
                namespace_prefix = namespace_prefix,
                keep_revisions   = keep_revisions,
                younger_than     = younger_than,
                protected_tags   = protected_tags or [],
            )
        return buf.getvalue()

    def test_summary_header_present(self):
        output = self._capture()
        self.assertIn("Summary", output)

    def test_candidates_count_with_colon(self):
        output = self._capture(candidates=[])
        self.assertIn("Candidates found:", output)

    def test_zero_candidates_shows_checkmark(self):
        output = self._capture(candidates=[])
        self.assertIn("✅", output)

    def test_zero_candidates_shows_no_criteria_message(self):
        output = self._capture(candidates=[])
        self.assertIn("No images match the pruning criteria", output)

    def test_nonzero_candidates_no_checkmark(self):
        output = self._capture(candidates=[{"x": 1}, {"x": 2}])
        self.assertNotIn("✅", output)

    def test_dry_run_shows_apply_command(self):
        output = self._capture(
            dry_run=True,
            namespace_prefix="prd-wealthmanagement",
            keep_revisions=10,
            younger_than="24h",
        )
        self.assertIn("--dry-run false", output)
        self.assertIn("prd-wealthmanagement", output)

    def test_live_run_no_apply_command(self):
        output = self._capture(dry_run=False)
        self.assertNotIn("--dry-run false", output)


# ══════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite  = loader.loadTestsFromModule(__import__("__main__"))
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
