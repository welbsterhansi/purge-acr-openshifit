#!/usr/bin/env python3
"""
test_openshift_prune.py — TDD suite for openshift_prune.py

Coverage:
  - group_tags_by_stream    : group ISTs by stream name, sorted newest-first
  - collect_tag_candidates  : keep N most recent per stream, safety checks
  - delete_istag            : delete whole tag (not history entry)
  - run_prune               : orchestration with new tag-level pruning
  - parse_younger_than      : time string parsing (24h, 60m)
  - print_settings / print_summary : output formatting
"""

import sys
import unittest
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

from openshift_prune import (
    group_tags_by_stream,
    collect_tag_candidates,
    delete_istag,
    parse_younger_than,
    run_prune,
    parse_args,
    print_settings,
    print_summary,
)


# ══════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════

def _make_ist(stream, tag, ns="prd-ns", created_days_ago=30, digest_suffix=None):
    """Builds a minimal ImageStreamTag mock."""
    ist = MagicMock()
    ist.metadata.name      = f"{stream}:{tag}"
    ist.metadata.namespace = ns
    created = datetime.now(timezone.utc) - timedelta(days=created_days_ago)
    ist.metadata.creationTimestamp = created.isoformat()
    suffix = digest_suffix or tag.replace(".", "")
    ist.image.dockerImageReference = f"registry.azurecr.io/repo@sha256:{suffix}"
    return ist


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
# group_tags_by_stream
# ══════════════════════════════════════════════════════════════════

class TestGroupTagsByStream(unittest.TestCase):

    def test_single_stream_single_tag(self):
        """One IST produces one group with one entry."""
        ist = _make_ist("myapp", "1.0.0")
        result = group_tags_by_stream([ist])
        self.assertIn("myapp", result)
        self.assertEqual(len(result["myapp"]), 1)

    def test_multiple_tags_same_stream_grouped(self):
        """Two ISTs of same stream → one group with two entries."""
        ists = [_make_ist("trading", "1.0.0"), _make_ist("trading", "2.0.0")]
        result = group_tags_by_stream(ists)
        self.assertIn("trading", result)
        self.assertEqual(len(result["trading"]), 2)

    def test_multiple_streams_separated(self):
        """ISTs from different streams → separate groups."""
        ists = [_make_ist("app1", "1.0.0"), _make_ist("app2", "1.0.0")]
        result = group_tags_by_stream(ists)
        self.assertIn("app1", result)
        self.assertIn("app2", result)
        self.assertEqual(len(result), 2)

    def test_tags_sorted_newest_first(self):
        """Within a group, newest tag must come first."""
        old = _make_ist("trading", "1.0.0", created_days_ago=100)
        new = _make_ist("trading", "2.0.0", created_days_ago=1)
        result = group_tags_by_stream([old, new])
        names = [i.metadata.name for i in result["trading"]]
        self.assertEqual(names[0], "trading:2.0.0")
        self.assertEqual(names[1], "trading:1.0.0")

    def test_empty_list_returns_empty_dict(self):
        self.assertEqual(group_tags_by_stream([]), {})

    def test_stream_name_parsed_from_metadata_name(self):
        """Stream name is the part before ':' in metadata.name."""
        ist = _make_ist("dotnet-service-trading", "5.6.4")
        result = group_tags_by_stream([ist])
        self.assertIn("dotnet-service-trading", result)

    def test_tag_with_colons_in_version_not_split_twice(self):
        """Tags like '1.0.0' must keep full tag after first ':'."""
        ist = _make_ist("myapp", "1.0.0")
        result = group_tags_by_stream([ist])
        ist_back = result["myapp"][0]
        # tag portion of the name must survive
        self.assertEqual(ist_back.metadata.name, "myapp:1.0.0")


# ══════════════════════════════════════════════════════════════════
# collect_tag_candidates
# ══════════════════════════════════════════════════════════════════

class TestCollectTagCandidates(unittest.TestCase):

    def _grouped(self, stream, tags):
        """
        tags: list of (tag_name, created_days_ago, digest_suffix)
        Returns grouped dict sorted newest-first.
        """
        ists = [_make_ist(stream, tag, created_days_ago=days, digest_suffix=dig)
                for tag, days, dig in tags]
        ists.sort(key=lambda i: i.metadata.creationTimestamp, reverse=True)
        return {stream: ists}

    def test_keeps_n_most_recent(self):
        """keep_revisions=2 → 2 newest kept, rest are candidates."""
        grouped = self._grouped("trading", [
            ("5.0.0", 1,   "e"),
            ("4.0.0", 10,  "d"),
            ("3.0.0", 20,  "c"),
            ("2.0.0", 30,  "b"),
            ("1.0.0", 100, "a"),
        ])
        candidates = collect_tag_candidates(
            grouped_tags   = grouped,
            active_digests = set(),
            keep_revisions = 2,
            younger_than   = timedelta(hours=1),
            protected_tags = [],
        )
        tags = {c["tag"] for c in candidates}
        self.assertNotIn("5.0.0", tags)  # rank 0 — keep
        self.assertNotIn("4.0.0", tags)  # rank 1 — keep
        self.assertIn("3.0.0",    tags)  # rank 2 — candidate
        self.assertIn("2.0.0",    tags)
        self.assertIn("1.0.0",    tags)

    def test_active_digest_never_candidate(self):
        """Tag whose digest is active in cluster must not be a candidate."""
        grouped = self._grouped("trading", [
            ("2.0.0", 10,  "new"),
            ("1.0.0", 100, "running"),
        ])
        candidates = collect_tag_candidates(
            grouped_tags   = grouped,
            active_digests = {"sha256:running"},
            keep_revisions = 1,
            younger_than   = timedelta(hours=1),
            protected_tags = [],
        )
        tags = {c["tag"] for c in candidates}
        self.assertNotIn("1.0.0", tags)

    def test_protected_tag_never_candidate(self):
        """Tag name in protected_tags must never be a candidate."""
        grouped = self._grouped("trading", [
            ("latest", 200, "old"),
            ("1.0.0",  100, "also_old"),
        ])
        candidates = collect_tag_candidates(
            grouped_tags   = grouped,
            active_digests = set(),
            keep_revisions = 0,
            younger_than   = timedelta(hours=1),
            protected_tags = ["latest"],
        )
        tags = {c["tag"] for c in candidates}
        self.assertNotIn("latest", tags)
        self.assertIn("1.0.0",    tags)

    def test_recent_tag_never_candidate(self):
        """Tag created within younger_than window must not be a candidate."""
        grouped = self._grouped("trading", [
            ("2.0.0", 0,   "new"),   # created now
            ("1.0.0", 100, "old"),
        ])
        candidates = collect_tag_candidates(
            grouped_tags   = grouped,
            active_digests = set(),
            keep_revisions = 0,
            younger_than   = timedelta(hours=24),
            protected_tags = [],
        )
        tags = {c["tag"] for c in candidates}
        self.assertNotIn("2.0.0", tags)  # too recent
        self.assertIn("1.0.0",    tags)

    def test_candidate_contains_required_fields(self):
        """Each candidate must have namespace, stream, tag, digest."""
        grouped = self._grouped("trading", [
            ("2.0.0", 10,  "newdig"),
            ("1.0.0", 100, "olddig"),
        ])
        candidates = collect_tag_candidates(
            grouped_tags   = grouped,
            active_digests = set(),
            keep_revisions = 1,
            younger_than   = timedelta(hours=1),
            protected_tags = [],
        )
        self.assertEqual(len(candidates), 1)
        c = candidates[0]
        self.assertEqual(c["namespace"], "prd-ns")
        self.assertEqual(c["stream"],    "trading")
        self.assertEqual(c["tag"],       "1.0.0")
        self.assertIn("sha256:olddig",   c["digest"])

    def test_empty_grouped_no_candidates(self):
        candidates = collect_tag_candidates(
            grouped_tags   = {},
            active_digests = set(),
            keep_revisions = 2,
            younger_than   = timedelta(hours=1),
            protected_tags = [],
        )
        self.assertEqual(candidates, [])

    def test_fewer_tags_than_keep_no_candidates(self):
        """Stream with 2 tags and keep_revisions=10 → no candidates."""
        grouped = self._grouped("trading", [
            ("2.0.0", 10,  "new"),
            ("1.0.0", 100, "old"),
        ])
        candidates = collect_tag_candidates(
            grouped_tags   = grouped,
            active_digests = set(),
            keep_revisions = 10,
            younger_than   = timedelta(hours=1),
            protected_tags = [],
        )
        self.assertEqual(candidates, [])

    def test_multiple_streams_all_scanned(self):
        """Candidates from multiple streams are all collected."""
        grouped = {
            "app1": [_make_ist("app1", "2.0.0", created_days_ago=5),
                     _make_ist("app1", "1.0.0", created_days_ago=100)],
            "app2": [_make_ist("app2", "2.0.0", created_days_ago=5),
                     _make_ist("app2", "1.0.0", created_days_ago=100)],
        }
        candidates = collect_tag_candidates(
            grouped_tags   = grouped,
            active_digests = set(),
            keep_revisions = 1,
            younger_than   = timedelta(hours=1),
            protected_tags = [],
        )
        streams = {c["stream"] for c in candidates}
        self.assertIn("app1", streams)
        self.assertIn("app2", streams)


# ══════════════════════════════════════════════════════════════════
# delete_istag — deletes an entire ImageStreamTag
# ══════════════════════════════════════════════════════════════════

class TestDeleteIstag(unittest.TestCase):

    def test_deletes_with_stream_colon_tag_format(self):
        """Must call delete with '{stream}:{tag}' — the whole tag, not a history entry."""
        istag_api = MagicMock()
        delete_istag(istag_api, "prd-ns", "trading", "1.0.0")
        istag_api.delete.assert_called_once_with(
            name      = "trading:1.0.0",
            namespace = "prd-ns",
        )

    def test_namespace_forwarded_exactly(self):
        istag_api = MagicMock()
        delete_istag(istag_api, "prd-wealthmanagement", "myapp", "5.6.4")
        _, kwargs = istag_api.delete.call_args
        self.assertEqual(kwargs["namespace"], "prd-wealthmanagement")

    def test_propagates_api_exception(self):
        istag_api = MagicMock()
        istag_api.delete.side_effect = Exception("API error")
        with self.assertRaises(Exception):
            delete_istag(istag_api, "prd-ns", "trading", "1.0.0")


# ══════════════════════════════════════════════════════════════════
# run_prune — orchestration
# ══════════════════════════════════════════════════════════════════

def _make_oc_client(active=None, istags_by_ns=None):
    """Mock OpenShiftClient for run_prune tests (uses istag_api.get)."""
    oc = MagicMock()
    oc._active    = {d: "loc" for d in (active or [])}
    oc.namespaces = list((istags_by_ns or {}).keys())

    def fake_istag_get(namespace):
        result = MagicMock()
        result.items = (istags_by_ns or {}).get(namespace, [])
        return result

    oc.istag_api.get.side_effect = fake_istag_get
    return oc


class TestRunPrune(unittest.TestCase):

    def _prune(self, oc, dry_run=True, keep_revisions=1,
               younger_than="1h", protected_tags=None):
        return run_prune(
            oc_client      = oc,
            keep_revisions = keep_revisions,
            younger_than   = parse_younger_than(younger_than),
            protected_tags = protected_tags or [],
            dry_run        = dry_run,
        )

    def test_dry_run_never_calls_delete(self):
        """dry_run=True must not call delete_istag."""
        ists = [
            _make_ist("trading", "2.0.0", created_days_ago=10),
            _make_ist("trading", "1.0.0", created_days_ago=100),
        ]
        oc = _make_oc_client(istags_by_ns={"prd-ns": ists})

        with patch("openshift_prune.delete_istag") as mock_del:
            self._prune(oc, dry_run=True, keep_revisions=1)

        mock_del.assert_not_called()

    def test_dry_run_returns_candidates(self):
        """dry_run=True must return the list of candidates."""
        ists = [
            _make_ist("trading", "2.0.0", created_days_ago=10),
            _make_ist("trading", "1.0.0", created_days_ago=100),
        ]
        oc = _make_oc_client(istags_by_ns={"prd-ns": ists})
        candidates = self._prune(oc, dry_run=True, keep_revisions=1)
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["tag"], "1.0.0")

    def test_live_run_calls_delete_for_each_candidate(self):
        """dry_run=False must call delete_istag once per candidate."""
        ists = [
            _make_ist("trading", "3.0.0", created_days_ago=5),
            _make_ist("trading", "2.0.0", created_days_ago=10),
            _make_ist("trading", "1.0.0", created_days_ago=100),
        ]
        oc = _make_oc_client(istags_by_ns={"prd-ns": ists})

        with patch("openshift_prune.delete_istag") as mock_del:
            self._prune(oc, dry_run=False, keep_revisions=1)

        self.assertEqual(mock_del.call_count, 2)

    def test_live_run_delete_called_with_correct_args(self):
        """delete_istag must receive (istag_api, namespace, stream, tag)."""
        ists = [
            _make_ist("trading", "2.0.0", created_days_ago=10, digest_suffix="new"),
            _make_ist("trading", "1.0.0", created_days_ago=100, digest_suffix="old"),
        ]
        oc = _make_oc_client(istags_by_ns={"prd-ns": ists})

        with patch("openshift_prune.delete_istag") as mock_del:
            self._prune(oc, dry_run=False, keep_revisions=1)

        mock_del.assert_called_once_with(oc.istag_api, "prd-ns", "trading", "1.0.0")

    def test_active_digest_not_deleted(self):
        """Tag whose digest is active in cluster must not be deleted."""
        ists = [
            _make_ist("trading", "2.0.0", created_days_ago=10, digest_suffix="new"),
            _make_ist("trading", "1.0.0", created_days_ago=100, digest_suffix="running"),
        ]
        oc = _make_oc_client(
            active        = ["sha256:running"],
            istags_by_ns  = {"prd-ns": ists},
        )

        with patch("openshift_prune.delete_istag") as mock_del:
            self._prune(oc, dry_run=False, keep_revisions=1)

        mock_del.assert_not_called()

    def test_no_candidates_delete_never_called(self):
        """Single tag per stream → no candidates → no deletes."""
        ists = [_make_ist("trading", "1.0.0", created_days_ago=100)]
        oc = _make_oc_client(istags_by_ns={"prd-ns": ists})

        with patch("openshift_prune.delete_istag") as mock_del:
            self._prune(oc, dry_run=False, keep_revisions=1)

        mock_del.assert_not_called()

    def test_multiple_namespaces_all_scanned(self):
        """All namespaces in oc.namespaces must be scanned."""
        ists1 = [_make_ist("app", "2.0.0", ns="prd-ns1", created_days_ago=5),
                 _make_ist("app", "1.0.0", ns="prd-ns1", created_days_ago=100)]
        ists2 = [_make_ist("app", "2.0.0", ns="prd-ns2", created_days_ago=5),
                 _make_ist("app", "1.0.0", ns="prd-ns2", created_days_ago=100)]
        oc = _make_oc_client(istags_by_ns={"prd-ns1": ists1, "prd-ns2": ists2})

        candidates = self._prune(oc, dry_run=True, keep_revisions=1)

        namespaces = {c["namespace"] for c in candidates}
        self.assertIn("prd-ns1", namespaces)
        self.assertIn("prd-ns2", namespaces)

    def test_delete_failure_does_not_stop_remaining(self):
        """One delete failure must not stop the rest."""
        ists = [
            _make_ist("trading", "3.0.0", created_days_ago=5),
            _make_ist("trading", "2.0.0", created_days_ago=50, digest_suffix="fail"),
            _make_ist("trading", "1.0.0", created_days_ago=100, digest_suffix="ok"),
        ]
        oc = _make_oc_client(istags_by_ns={"prd-ns": ists})
        calls = []

        def selective_fail(api, ns, stream, tag):
            calls.append(tag)
            if tag == "2.0.0":
                raise Exception("API error")

        with patch("openshift_prune.delete_istag", side_effect=selective_fail), \
             patch("sys.stdout"):
            self._prune(oc, dry_run=False, keep_revisions=1)

        self.assertIn("1.0.0", calls)


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
            dry_run          = True,
            namespace_prefix = "prd-wealthmanagement",
            keep_revisions   = 10,
            younger_than     = "24h",
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
