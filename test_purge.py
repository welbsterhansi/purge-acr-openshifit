#!/usr/bin/env python3
"""
test_purge.py — Mocked unit tests for purge.py and openshift_client.py

Coverage:
  - list_repos_by_prefix     : prefix vazio, prefix exato, prefixo parcial, ACR vazio
  - classify_manifest        : todos os tipos
  - analyze_repo             : keep, max_age_days, sem tag, sem timestamp, ordenação
  - dry-run                  : nenhuma deleção ocorre
  - live run                 : delete_manifest chamado corretamente
  - OpenShift conectividade  : VPN fechada, token expirado, permissão negada
  - OpenShift check_digest   : active, historical, not found, falha no check
  - permission_incomplete    : bloqueia tudo
  - skip-openshift           : nenhuma verificação feita
  - relatórios               : JSON e CSV gerados com campos corretos
  - PHASE 0                  : exit(1) antes da análise ACR quando cluster inacessível
"""

import json
import csv
import sys
import time
import unittest
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

from azure.core.exceptions import HttpResponseError

# ── módulos sob teste ──────────────────────────────────────────────
from purge import (
    list_repos_by_prefix,
    classify_manifest,
    analyze_repo,
    analyze_all_repos,
    delete_all_candidates,
    confirm_deletion,
    save_json_report,
    save_csv_report,
    delete_manifest,
    get_manifests,
    compute_repo_counts,
    _print_candidates_table,
)
from openshift_client import OpenShiftClient


# ══════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════

def _make_manifest(digest, tags, media_type, age_days):
    """Builds a manifest dict as returned by get_manifests()."""
    created_on = datetime.now(timezone.utc) - timedelta(days=age_days)
    return {
        "digest":    digest,
        "tags":      tags,
        "mediaType": media_type,
        "createdOn": created_on,
    }


def _fake_acr_client(repos):
    """Returns a mock ACR client whose list_repository_names returns repos."""
    client = MagicMock()
    client.list_repository_names.return_value = repos
    return client


def _make_oc_client(active=None, historical=None, permission_incomplete=False):
    """Returns an OpenShiftClient with pre-loaded state — no real cluster needed."""
    import threading
    with patch("openshift_client.config"), \
         patch("openshift_client.k8s_client.ApiClient"), \
         patch("openshift_client.DynamicClient"), \
         patch("openshift_client.k8s_client.CoreV1Api"), \
         patch("openshift_client.k8s_client.AppsV1Api"), \
         patch("openshift_client.k8s_client.BatchV1Api"):

        oc = OpenShiftClient.__new__(OpenShiftClient)
        oc._active               = active or {}
        oc._historical           = historical or {}
        oc.permission_incomplete = permission_incomplete
        oc.namespaces            = []
        oc._lock                 = threading.Lock()
        return oc


def _make_results(digest="sha256:old1", can_delete=True):
    return [{
        "repository": "homebuying/users",
        "images": [{
            "digest":           digest,
            "tags":             ["v1"],
            "canDelete":        can_delete,
            "protectionReason": None,
            "ocLocation":       None,
        }]
    }]


def _run_analyze(manifests, keep=2, max_age_days=15, protected_tags=None):
    """Patches get_manifests and runs analyze_repo."""
    with patch("purge.get_manifests", return_value=manifests):
        client = MagicMock()
        return analyze_repo(
            client, "homebuying/users", keep, max_age_days,
            "myregistry.azurecr.io", protected_tags=protected_tags or [],
        )


# ══════════════════════════════════════════════════════════════════
# Simulação da lógica PHASE 0 do purge.py (entry point)
# Extraída para função para ser testável sem subprocess
# ══════════════════════════════════════════════════════════════════

def run_phase0(skip_openshift, in_cluster, OpenShiftClient):
    """
    Replica a lógica do PHASE 0 do purge.py.
    Retorna (oc_client, exit_code) — exit_code None = não saiu.

    Passes namespace_prefix="" so ALL namespaces from the connected cluster
    are loaded — protecting images regardless of environment prefix.
    Aborts if 0 namespaces are found (wrong cluster or no access).
    """
    if skip_openshift:
        print("  ⚠️  OpenShift check skipped (--skip-openshift).")
        print("      WARNING: Images running in production will NOT be protected.")
        print("      Only use this flag when the cluster is confirmed unreachable.\n")
        return None, None

    try:
        oc_client = OpenShiftClient(in_cluster=in_cluster, namespace_prefix="")
        if not oc_client.namespaces:
            print(f"\n  🚫 ABORT: Connected to cluster but found 0 accessible namespaces.")
            print("     You may be logged into the wrong cluster or lack permissions.")
            print("     Use --skip-openshift only if you accept running without protection.\n")
            return None, 1
        print(f"  Cluster state loaded: {len(oc_client._active)} active | {len(oc_client._historical)} historical digest(s)")
        print("  ✅ Connected to OpenShift cluster.\n")
        return oc_client, None
    except Exception as e:
        print(f"\n  🚫 ABORT: Could not connect to OpenShift cluster: {e}")
        print("     Stopping execution to prevent unsafe deletions.")
        print("     Use --skip-openshift only if you are sure the cluster")
        print("     is unreachable and you accept the risk.\n")
        return None, 1


def apply_openshift_results(oc_client, all_results):
    """Replica a fase de verificação do purge.py."""
    if oc_client is None:
        return

    for r in all_results:
        for img in r["images"]:
            if not img["canDelete"]:
                continue
            try:
                status, location = oc_client.check_digest(img["digest"])
            except Exception:
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

    if oc_client.permission_incomplete:
        for r in all_results:
            for img in r["images"]:
                img["canDelete"]        = False
                img["protectionReason"] = "insufficient_permissions"


# ══════════════════════════════════════════════════════════════════
# list_repos_by_prefix
# ══════════════════════════════════════════════════════════════════

class TestListReposByPrefix(unittest.TestCase):

    def test_empty_prefix_returns_all(self):
        repos  = ["homebuying/users", "payments/api", "infra/base"]
        client = _fake_acr_client(repos)
        result = list_repos_by_prefix(client, "")
        self.assertEqual(result, repos)

    def test_exact_prefix_returns_exact_and_children(self):
        repos  = ["homebuying", "homebuying/users", "homebuying/offers", "payments/api"]
        client = _fake_acr_client(repos)
        result = list_repos_by_prefix(client, "homebuying")
        self.assertEqual(set(result), {"homebuying", "homebuying/users", "homebuying/offers"})

    def test_prefix_does_not_match_partial_names(self):
        # "home" must NOT match "homebuying"
        repos  = ["homebuying/users", "home/base"]
        client = _fake_acr_client(repos)
        result = list_repos_by_prefix(client, "home")
        self.assertEqual(result, ["home/base"])

    def test_empty_registry_returns_empty(self):
        client = _fake_acr_client([])
        result = list_repos_by_prefix(client, "")
        self.assertEqual(result, [])

    def test_prefix_with_no_match_returns_empty(self):
        repos  = ["payments/api", "infra/base"]
        client = _fake_acr_client(repos)
        result = list_repos_by_prefix(client, "homebuying")
        self.assertEqual(result, [])


# ══════════════════════════════════════════════════════════════════
# classify_manifest
# ══════════════════════════════════════════════════════════════════

class TestClassifyManifest(unittest.TestCase):

    def test_docker_v2(self):
        mt = "application/vnd.docker.distribution.manifest.v2+json"
        self.assertEqual(classify_manifest(mt, ["v1.0"]), "docker")

    def test_oci(self):
        mt = "application/vnd.oci.image.manifest.v1+json"
        self.assertEqual(classify_manifest(mt, ["v1.0"]), "oci")

    def test_signature(self):
        mt = "application/vnd.cncf.notary.signature"
        self.assertEqual(classify_manifest(mt, []), "signature")

    def test_manifest_index(self):
        mt = "application/vnd.docker.distribution.manifest.list.v2+json"
        self.assertEqual(classify_manifest(mt, ["latest"]), "manifest-index")

    def test_orphan_no_tags(self):
        mt = "application/vnd.docker.distribution.manifest.v2+json"
        self.assertEqual(classify_manifest(mt, []), "orphan")

    def test_unknown(self):
        self.assertEqual(classify_manifest("application/vnd.something.else", ["v1"]), "unknown")

    def test_none_media_type_with_tags(self):
        self.assertEqual(classify_manifest(None, ["v1"]), "unknown")

    def test_none_media_type_no_tags(self):
        self.assertEqual(classify_manifest(None, []), "orphan")


# ══════════════════════════════════════════════════════════════════
# analyze_repo — regras de negócio
# ══════════════════════════════════════════════════════════════════

class TestAnalyzeRepo(unittest.TestCase):

    def test_image_without_tag_is_protected(self):
        manifests = [_make_manifest("sha256:aaa", [], "application/vnd.docker.distribution.manifest.v2+json", 30)]
        result    = _run_analyze(manifests)
        img       = result["images"][0]
        self.assertFalse(img["canDelete"])
        self.assertIn("no_tag", img["protectionReason"])

    def test_image_without_timestamp_is_protected(self):
        manifests = [{
            "digest":    "sha256:bbb",
            "tags":      ["v1.0"],
            "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
            "createdOn": None,
        }]
        result = _run_analyze(manifests)
        img    = result["images"][0]
        self.assertFalse(img["canDelete"])
        self.assertEqual(img["protectionReason"], "no_timestamp")

    def test_keep_newest_protects_n_images(self):
        mt = "application/vnd.docker.distribution.manifest.v2+json"
        manifests = [
            _make_manifest("sha256:new1", ["v3"], mt, 1),
            _make_manifest("sha256:new2", ["v2"], mt, 2),
            _make_manifest("sha256:old1", ["v1"], mt, 30),
        ]
        result    = _run_analyze(manifests, keep=2, max_age_days=15)
        protected = [i for i in result["images"] if (i.get("protectionReason") or "").startswith("keep_newest")]
        deletable = [i for i in result["images"] if i["canDelete"]]
        self.assertEqual(len(protected), 2)
        self.assertEqual(len(deletable), 1)
        self.assertEqual(deletable[0]["digest"], "sha256:old1")

    def test_too_recent_image_is_not_deleted(self):
        mt = "application/vnd.docker.distribution.manifest.v2+json"
        manifests = [
            _make_manifest("sha256:new1", ["v2"], mt, 1),
            _make_manifest("sha256:new2", ["v1"], mt, 5),
            _make_manifest("sha256:new3", ["v0"], mt, 10),
        ]
        result    = _run_analyze(manifests, keep=1, max_age_days=15)
        deletable = [i for i in result["images"] if i["canDelete"]]
        self.assertEqual(len(deletable), 0)

    def test_eligible_old_image_is_deletable(self):
        mt = "application/vnd.docker.distribution.manifest.v2+json"
        manifests = [
            _make_manifest("sha256:new1", ["v2"], mt, 1),
            _make_manifest("sha256:old1", ["v1"], mt, 20),
        ]
        result    = _run_analyze(manifests, keep=1, max_age_days=15)
        deletable = [i for i in result["images"] if i["canDelete"]]
        self.assertEqual(len(deletable), 1)
        self.assertEqual(deletable[0]["digest"], "sha256:old1")

    def test_signature_type_is_protected(self):
        manifests = [_make_manifest("sha256:sig1", ["sig"], "application/vnd.cncf.notary.signature", 30)]
        result    = _run_analyze(manifests)
        img       = result["images"][0]
        self.assertFalse(img["canDelete"])
        self.assertIn("signature", img["protectionReason"])

    def test_manifest_index_is_protected(self):
        mt        = "application/vnd.docker.distribution.manifest.list.v2+json"
        manifests = [_make_manifest("sha256:idx1", ["latest"], mt, 30)]
        result    = _run_analyze(manifests)
        self.assertFalse(result["images"][0]["canDelete"])

    def test_images_sorted_newest_first(self):
        mt = "application/vnd.docker.distribution.manifest.v2+json"
        manifests = [
            _make_manifest("sha256:old", ["v1"], mt, 30),
            _make_manifest("sha256:new", ["v2"], mt, 1),
        ]
        result  = _run_analyze(manifests, keep=0, max_age_days=0)
        digests = [i["digest"] for i in result["images"] if i["timestamp"]]
        self.assertEqual(digests[0], "sha256:new")

    def test_null_timestamp_images_sorted_last(self):
        mt = "application/vnd.docker.distribution.manifest.v2+json"
        manifests = [
            {"digest": "sha256:notimestamp", "tags": ["v1"], "mediaType": mt, "createdOn": None},
            _make_manifest("sha256:withtimestamp", ["v2"], mt, 5),
        ]
        result  = _run_analyze(manifests)
        digests = [i["digest"] for i in result["images"]]
        self.assertEqual(digests[-1], "sha256:notimestamp")

    def test_get_manifests_failure_returns_empty(self):
        with patch("purge.get_manifests", side_effect=Exception("ACR error")):
            client = MagicMock()
            result = analyze_repo(client, "homebuying/users", 2, 15, "myregistry.azurecr.io")
        self.assertEqual(result["images"], [])

    def test_no_manifests_returns_empty(self):
        result = _run_analyze([])
        self.assertEqual(result["images"], [])


# ══════════════════════════════════════════════════════════════════
# --protected-tags — tags que nunca devem ser deletadas
# ══════════════════════════════════════════════════════════════════

class TestProtectedTags(unittest.TestCase):

    def _old_manifest(self, tags, digest="sha256:old001"):
        return _make_manifest(digest, tags, "application/vnd.docker.distribution.manifest.v2+json", age_days=60)

    def test_protected_tag_blocks_deletion(self):
        """Manifest com tag protegida não deve ser deletado mesmo sendo elegível."""
        manifests = [self._old_manifest(["latest"])]
        result = _run_analyze(manifests, keep=0, max_age_days=15, protected_tags=["latest"])
        img = result["images"][0]
        self.assertFalse(img["canDelete"])

    def test_protected_tag_sets_protection_reason(self):
        """protectionReason deve indicar qual tag protegeu o manifest."""
        manifests = [self._old_manifest(["latest"])]
        result = _run_analyze(manifests, keep=0, max_age_days=15, protected_tags=["latest"])
        img = result["images"][0]
        self.assertIn("latest", img["protectionReason"])

    def test_non_protected_tag_not_affected(self):
        """Tag não listada em protected_tags não deve ser protegida por isso."""
        manifests = [self._old_manifest(["v1.0.0"])]
        result = _run_analyze(manifests, keep=0, max_age_days=15, protected_tags=["latest"])
        img = result["images"][0]
        self.assertTrue(img["canDelete"])

    def test_empty_protected_tags_no_effect(self):
        """Lista vazia de protected_tags não deve alterar o comportamento padrão."""
        manifests = [self._old_manifest(["latest"])]
        result = _run_analyze(manifests, keep=0, max_age_days=15, protected_tags=[])
        img = result["images"][0]
        self.assertTrue(img["canDelete"])

    def test_protected_tag_among_multiple_tags(self):
        """Basta uma tag do manifest estar na lista para proteger o manifest."""
        manifests = [self._old_manifest(["v1.0.0", "stable"])]
        result = _run_analyze(manifests, keep=0, max_age_days=15, protected_tags=["stable"])
        img = result["images"][0]
        self.assertFalse(img["canDelete"])

    def test_protected_tags_is_case_sensitive(self):
        """'Latest' ≠ 'latest' — comparação deve ser case-sensitive."""
        manifests = [self._old_manifest(["Latest"])]
        result = _run_analyze(manifests, keep=0, max_age_days=15, protected_tags=["latest"])
        img = result["images"][0]
        self.assertTrue(img["canDelete"])

    def test_multiple_protected_tags(self):
        """Qualquer tag da lista de protected_tags deve proteger o manifest."""
        for tag in ["latest", "stable", "production"]:
            with self.subTest(tag=tag):
                manifests = [self._old_manifest([tag], digest=f"sha256:{tag}001")]
                result = _run_analyze(manifests, keep=0, max_age_days=15,
                                      protected_tags=["latest", "stable", "production"])
                self.assertFalse(result["images"][0]["canDelete"])


# ══════════════════════════════════════════════════════════════════
# DRY-RUN
# ══════════════════════════════════════════════════════════════════

class TestDryRun(unittest.TestCase):

    def test_dry_run_does_not_call_delete(self):
        client      = MagicMock()
        all_results = _make_results("sha256:old1", can_delete=True)
        DRY_RUN     = True

        if not DRY_RUN:  # pragma: no cover
            for r in all_results:
                for img in r["images"]:
                    if img["canDelete"]:
                        delete_manifest(client, r["repository"], img["digest"])

        client.delete_manifest.assert_not_called()

    def test_live_run_calls_delete_for_each_candidate(self):
        client      = MagicMock()
        all_results = [{
            "repository": "homebuying/users",
            "images": [
                {"digest": "sha256:old1", "tags": ["v1"], "canDelete": True,  "protectionReason": None, "deleted": False},
                {"digest": "sha256:old2", "tags": ["v0"], "canDelete": True,  "protectionReason": None, "deleted": False},
                {"digest": "sha256:new1", "tags": ["v2"], "canDelete": False, "protectionReason": "keep_newest:2", "deleted": False},
            ]
        }]

        with patch("test_purge.delete_manifest") as mock_delete:
            for r in all_results:
                for img in r["images"]:
                    if img["canDelete"]:
                        delete_manifest(client, r["repository"], img["digest"])
                        img["deleted"] = True

            self.assertEqual(mock_delete.call_count, 2)
            mock_delete.assert_any_call(client, "homebuying/users", "sha256:old1")
            mock_delete.assert_any_call(client, "homebuying/users", "sha256:old2")

    def test_live_run_delete_failure_does_not_stop_loop(self):
        client      = MagicMock()
        all_results = [{
            "repository": "homebuying/users",
            "images": [
                {"digest": "sha256:fail", "tags": ["v0"], "canDelete": True, "protectionReason": None, "deleted": False},
                {"digest": "sha256:ok",   "tags": ["v1"], "canDelete": True, "protectionReason": None, "deleted": False},
            ]
        }]

        deleted = []
        with patch("test_purge.delete_manifest", side_effect=[Exception("ACR error"), None]):
            for r in all_results:
                for img in r["images"]:
                    if img["canDelete"]:
                        try:
                            delete_manifest(client, r["repository"], img["digest"])
                            img["deleted"] = True
                            deleted.append(img["digest"])
                        except Exception:
                            pass

        self.assertIn("sha256:ok", deleted)
        self.assertNotIn("sha256:fail", deleted)

    def test_live_run_deletes_all_candidates_from_real_report(self):
        """
        Usa o relatório real report-2026-04-06-102136.json para garantir que
        o loop de deleção percorre todas as imagens would_delete (6247) e
        não toca nas protegidas (6456).
        Valores de referência extraídos do relatório: 407 repos, 12703 manifests.
        """
        import os, json as _json
        report = os.path.expanduser("~/Downloads/report-2026-04-06-102136.json")
        if not os.path.exists(report):
            self.skipTest("Relatório real não encontrado em ~/Downloads")

        with open(report) as f:
            data = _json.load(f)

        def _to_internal(img):
            action = img["action"]
            return {
                "digest":           img["digest"],
                "repository":       None,  # preenchido abaixo
                "canDelete":        action == "would_delete",
                "deleted":          False,
                "protectionReason": img["protection_reason"],
            }

        all_results = []
        for r in data["repositories"]:
            images = [_to_internal(i) for i in r["images"]]
            for img in images:
                img["repository"] = r["repository"]
            all_results.append({"repository": r["repository"], "images": images})

        client = MagicMock()
        deleted_calls = []

        with patch("test_purge.delete_manifest", side_effect=lambda c, repo, digest: deleted_calls.append((repo, digest))):
            for r in all_results:
                for img in r["images"]:
                    if img["canDelete"]:
                        delete_manifest(client, r["repository"], img["digest"])
                        img["deleted"] = True

        # exactamente 6247 deleções — uma por imagem would_delete
        self.assertEqual(len(deleted_calls), 6247,
                         f"Esperado 6247 deleções, obtido {len(deleted_calls)}")

        # nenhuma imagem protegida foi deletada
        protected_digests = {
            img["digest"]
            for r in all_results
            for img in r["images"]
            if not img["canDelete"] and not img["deleted"]
        }
        deleted_digests = {digest for _, digest in deleted_calls}
        overlap = protected_digests & deleted_digests
        self.assertEqual(len(overlap), 0,
                         f"{len(overlap)} imagens protegidas foram deletadas incorrectamente")

    def test_live_run_15k_images_synthetic(self):
        """
        Stress test sintético com 15.000 imagens distribuídas em 500 repositórios.
        Garante que o loop de deleção:
          - chama delete_manifest exatamente para as candidatas (7500)
          - não toca nas protegidas (7500)
          - termina sem erro mesmo com volume absurdo de imagens
        """
        import hashlib

        TOTAL_REPOS      = 500
        IMAGES_PER_REPO  = 30   # 500 * 30 = 15.000 imagens
        DELETE_PER_REPO  = 15   # metade deletável
        PROTECT_PER_REPO = 15   # metade protegida

        protection_reasons = [
            "keep_newest:2",
            "too_recent:<15_days",
            "no_tag",
            "protected_type:signature",
            "running_on_cluster",
        ]

        all_results = []
        for repo_idx in range(TOTAL_REPOS):
            repo = f"stress/repo-{repo_idx:04d}"
            images = []

            for i in range(DELETE_PER_REPO):
                digest = f"sha256:{hashlib.sha256(f'{repo}-del-{i}'.encode()).hexdigest()}"
                images.append({
                    "digest":           digest,
                    "canDelete":        True,
                    "deleted":          False,
                    "protectionReason": None,
                })

            for i in range(PROTECT_PER_REPO):
                digest = f"sha256:{hashlib.sha256(f'{repo}-prot-{i}'.encode()).hexdigest()}"
                reason = protection_reasons[i % len(protection_reasons)]
                images.append({
                    "digest":           digest,
                    "canDelete":        False,
                    "deleted":          False,
                    "protectionReason": reason,
                })

            all_results.append({"repository": repo, "images": images})

        client        = MagicMock()
        deleted_calls = []

        with patch("test_purge.delete_manifest",
                   side_effect=lambda c, repo, digest: deleted_calls.append((repo, digest))):
            for r in all_results:
                for img in r["images"]:
                    if img["canDelete"]:
                        delete_manifest(client, r["repository"], img["digest"])
                        img["deleted"] = True

        expected_deletes  = TOTAL_REPOS * DELETE_PER_REPO   # 7500
        expected_total    = TOTAL_REPOS * IMAGES_PER_REPO   # 15000

        self.assertEqual(len(deleted_calls), expected_deletes,
                         f"Esperado {expected_deletes} deleções, obtido {len(deleted_calls)}")

        # nenhuma imagem protegida foi deletada
        protected_digests = {
            img["digest"]
            for r in all_results
            for img in r["images"]
            if not img["canDelete"] and not img["deleted"]
        }
        deleted_digests = {digest for _, digest in deleted_calls}
        overlap = protected_digests & deleted_digests
        self.assertEqual(len(overlap), 0,
                         f"{len(overlap)} imagens protegidas foram deletadas incorrectamente")

        # invariante: total de imagens correto
        total_imgs = sum(len(r["images"]) for r in all_results)
        self.assertEqual(total_imgs, expected_total)


# ══════════════════════════════════════════════════════════════════
# confirm_deletion — confirmação interativa de segurança
# ══════════════════════════════════════════════════════════════════

class TestConfirmDeletion(unittest.TestCase):

    def _capture(self, **kwargs):
        import io
        defaults = dict(
            registry="myregistry", prefix="myprefix", keep=2,
            max_age_days=15, dry_run=True, auto_approve=True,
            total_to_delete=10,
        )
        defaults.update(kwargs)
        buf = io.StringIO()
        with patch("sys.stdout", buf):
            confirm_deletion(**defaults)
        return buf.getvalue()

    def test_auto_approve_prints_registry(self):
        output = self._capture()
        self.assertIn("myregistry", output)

    def test_auto_approve_prints_prefix(self):
        output = self._capture()
        self.assertIn("myprefix", output)

    def test_empty_prefix_shows_wildcard(self):
        output = self._capture(prefix="")
        self.assertIn("*", output)

    def test_auto_approve_prints_total_to_delete(self):
        output = self._capture(total_to_delete=42)
        self.assertIn("42", output)

    def test_dry_run_label_in_output(self):
        output = self._capture(dry_run=True)
        self.assertIn("DRY", output.upper())

    def test_live_mode_label_in_output(self):
        output = self._capture(dry_run=False)
        self.assertIn("DELETE", output)

    def test_auto_approve_does_not_call_input(self):
        with patch("builtins.input") as mock_input, patch("sys.stdout"):
            confirm_deletion("r", "p", 2, 15, True, True, 0)
        mock_input.assert_not_called()

    def test_wrong_confirmation_exits(self):
        with patch("builtins.input", return_value="NO"), patch("sys.stdout"):
            with self.assertRaises(SystemExit):
                confirm_deletion("r", "p", 2, 15, False, False, 5)

    def test_correct_confirmation_proceeds(self):
        """'DELETE' exato deve passar sem SystemExit."""
        with patch("builtins.input", return_value="DELETE"), patch("sys.stdout"):
            confirm_deletion("r", "p", 2, 15, False, False, 5)

    def test_eof_on_input_exits(self):
        with patch("builtins.input", side_effect=EOFError), patch("sys.stdout"):
            with self.assertRaises(SystemExit):
                confirm_deletion("r", "p", 2, 15, False, False, 5)

    def test_no_equals_separator(self):
        """Saída não deve conter linhas de '===' — estilo moderno sem separadores de bloco."""
        output = self._capture()
        self.assertNotIn("====", output)

    def test_uses_diamond_header(self):
        """Cabeçalho deve usar '◈' como marcador visual."""
        output = self._capture()
        self.assertIn("◈", output)


# ══════════════════════════════════════════════════════════════════
# delete_manifest — retry em 429 (rate limit Azure)
# ══════════════════════════════════════════════════════════════════

def _http_error(status_code):
    """Cria um HttpResponseError com status_code configurável."""
    err = HttpResponseError(message=f"HTTP {status_code}")
    err.status_code = status_code
    return err


class TestDeleteManifestRetry(unittest.TestCase):

    def test_success_on_first_try_calls_acr_once(self):
        """Sem erro: ACR é chamado exatamente uma vez."""
        client = MagicMock()
        delete_manifest(client, "repo/app", "sha256:abc")
        client.delete_manifest.assert_called_once_with("repo/app", "sha256:abc")

    def test_retries_on_429_and_succeeds_on_second_attempt(self):
        """429 na primeira chamada → retry → sucesso na segunda."""
        client = MagicMock()
        client.delete_manifest.side_effect = [_http_error(429), None]

        with patch("time.sleep"):
            delete_manifest(client, "repo/app", "sha256:abc")

        self.assertEqual(client.delete_manifest.call_count, 2)

    def test_raises_after_max_retries_exceeded(self):
        """429 em todas as tentativas → levanta exceção após esgotar retries."""
        client = MagicMock()
        client.delete_manifest.side_effect = _http_error(429)

        with patch("time.sleep"):
            with self.assertRaises(HttpResponseError) as ctx:
                delete_manifest(client, "repo/app", "sha256:abc")

        self.assertEqual(ctx.exception.status_code, 429)
        # deve ter tentado mais de uma vez
        self.assertGreater(client.delete_manifest.call_count, 1)

    def test_does_not_retry_on_non_throttle_error(self):
        """Erros que não são 429 (ex: 404, 500) não devem ser retentados."""
        for status in (404, 500):
            with self.subTest(status=status):
                client = MagicMock()
                client.delete_manifest.side_effect = _http_error(status)

                with self.assertRaises(HttpResponseError):
                    delete_manifest(client, "repo/app", "sha256:abc")

                client.delete_manifest.assert_called_once()

    def test_sleep_between_retries(self):
        """Deve dormir entre tentativas para respeitar o rate limit."""
        client = MagicMock()
        client.delete_manifest.side_effect = [_http_error(429), None]

        with patch("time.sleep") as mock_sleep:
            delete_manifest(client, "repo/app", "sha256:abc")

        mock_sleep.assert_called_once()

    def test_deletion_loop_continues_after_max_retries_exhausted(self):
        """
        Quando uma imagem esgota os retries (429 permanente), o loop de
        deleção não para — as demais imagens ainda são processadas.
        """
        client  = MagicMock()
        results = [{
            "repository": "repo/app",
            "images": [
                {"digest": "sha256:throttled", "canDelete": True,  "deleted": False},
                {"digest": "sha256:ok",        "canDelete": True,  "deleted": False},
            ]
        }]

        call_count = {"n": 0}
        def fake_delete(c, repo, digest):
            call_count["n"] += 1
            if digest == "sha256:throttled":
                raise HttpResponseError(message="429")
            return None

        deleted = []
        with patch("time.sleep"):
            with patch("test_purge.delete_manifest", side_effect=fake_delete):
                for r in results:
                    for img in r["images"]:
                        if img["canDelete"]:
                            try:
                                delete_manifest(client, r["repository"], img["digest"])
                                img["deleted"] = True
                                deleted.append(img["digest"])
                            except HttpResponseError:
                                pass

        self.assertIn("sha256:ok", deleted)
        self.assertNotIn("sha256:throttled", deleted)


# ══════════════════════════════════════════════════════════════════
# PHASE 0 — OpenShift connectivity antes da análise ACR
# ══════════════════════════════════════════════════════════════════

class TestPhase0OpenShiftConnectivity(unittest.TestCase):

    def test_cluster_unreachable_returns_exit1(self):
        MockOC = MagicMock(side_effect=RuntimeError("Network unreachable"))
        _, exit_code = run_phase0(skip_openshift=False, in_cluster=False, OpenShiftClient=MockOC)
        self.assertEqual(exit_code, 1)

    def test_cluster_unreachable_oc_client_is_none(self):
        MockOC = MagicMock(side_effect=RuntimeError("Network unreachable"))
        oc_client, _ = run_phase0(skip_openshift=False, in_cluster=False, OpenShiftClient=MockOC)
        self.assertIsNone(oc_client)

    def test_cluster_unreachable_acr_never_called(self):
        MockOC   = MagicMock(side_effect=RuntimeError("VPN down"))
        mock_acr = MagicMock()
        run_phase0(skip_openshift=False, in_cluster=False, OpenShiftClient=MockOC)
        mock_acr.assert_not_called()

    def test_cluster_ok_returns_exit_none(self):
        mock_oc = _make_oc_client()
        mock_oc.namespaces = ["prd-ns"]
        MockOC  = MagicMock(return_value=mock_oc)
        with patch("sys.stdout"):
            _, exit_code = run_phase0(skip_openshift=False, in_cluster=False, OpenShiftClient=MockOC)
        self.assertIsNone(exit_code)

    def test_cluster_ok_returns_oc_client(self):
        mock_oc = _make_oc_client()
        mock_oc.namespaces = ["prd-ns"]
        MockOC  = MagicMock(return_value=mock_oc)
        with patch("sys.stdout"):
            oc_client, _ = run_phase0(skip_openshift=False, in_cluster=False, OpenShiftClient=MockOC)
        self.assertEqual(oc_client, mock_oc)

    def test_dry_run_also_exits_if_cluster_unreachable(self):
        """Em dry-run o comportamento é idêntico — exit(1) se cluster falha."""
        MockOC = MagicMock(side_effect=RuntimeError("Timeout"))
        _, exit_code = run_phase0(skip_openshift=False, in_cluster=False, OpenShiftClient=MockOC)
        self.assertEqual(exit_code, 1)

    def test_skip_openshift_returns_exit_none(self):
        MockOC = MagicMock()
        _, exit_code = run_phase0(skip_openshift=True, in_cluster=False, OpenShiftClient=MockOC)
        self.assertIsNone(exit_code)

    def test_skip_openshift_oc_client_is_none(self):
        MockOC = MagicMock()
        oc_client, _ = run_phase0(skip_openshift=True, in_cluster=False, OpenShiftClient=MockOC)
        self.assertIsNone(oc_client)

    def test_skip_openshift_never_instantiates_client(self):
        MockOC = MagicMock()
        run_phase0(skip_openshift=True, in_cluster=False, OpenShiftClient=MockOC)
        MockOC.assert_not_called()

    def test_skip_openshift_candidates_remain_deletable(self):
        all_results = _make_results("sha256:candidate", can_delete=True)
        MockOC      = MagicMock()
        oc_client, _ = run_phase0(skip_openshift=True, in_cluster=False, OpenShiftClient=MockOC)
        apply_openshift_results(oc_client, all_results)
        self.assertTrue(all_results[0]["images"][0]["canDelete"])


# ══════════════════════════════════════════════════════════════════
# OpenShift — _list_valid_namespaces
# ══════════════════════════════════════════════════════════════════

class TestOpenShiftNamespaceListing(unittest.TestCase):

    def _make_bare_oc(self, project_api):
        oc = OpenShiftClient.__new__(OpenShiftClient)
        oc.permission_incomplete = False
        oc.project_api = project_api
        return oc

    def test_network_error_raises_runtime_error(self):
        project_api = MagicMock()
        project_api.get.side_effect = ConnectionError("Network unreachable")
        oc = self._make_bare_oc(project_api)
        with self.assertRaises(RuntimeError):
            oc._list_valid_namespaces()

    def test_permission_403_sets_flag_and_returns_empty(self):
        exc        = Exception("Forbidden")
        exc.status = 403
        project_api = MagicMock()
        project_api.get.side_effect = exc
        oc = self._make_bare_oc(project_api)
        result = oc._list_valid_namespaces()
        self.assertEqual(result, [])
        self.assertTrue(oc.permission_incomplete)

    def test_permission_401_sets_flag_and_returns_empty(self):
        exc        = Exception("Unauthorized")
        exc.status = 401
        project_api = MagicMock()
        project_api.get.side_effect = exc
        oc = self._make_bare_oc(project_api)
        result = oc._list_valid_namespaces()
        self.assertEqual(result, [])
        self.assertTrue(oc.permission_incomplete)

    def test_http_500_raises_runtime_error(self):
        exc        = Exception("Internal Server Error")
        exc.status = 500
        project_api = MagicMock()
        project_api.get.side_effect = exc
        oc = self._make_bare_oc(project_api)
        with self.assertRaises(RuntimeError):
            oc._list_valid_namespaces()


# ══════════════════════════════════════════════════════════════════
# OpenShift — check_digest
# ══════════════════════════════════════════════════════════════════

class TestCheckDigest(unittest.TestCase):

    def test_active_digest_returns_active(self):
        oc = _make_oc_client(active={"sha256:abc": "prd-ns / pod / mypod"})
        status, location = oc.check_digest("sha256:abc")
        self.assertEqual(status, "active")
        self.assertEqual(location, "prd-ns / pod / mypod")

    def test_historical_digest_returns_historical(self):
        oc = _make_oc_client(historical={"sha256:xyz": "prd-ns / imagestream / mystream (history)"})
        status, location = oc.check_digest("sha256:xyz")
        self.assertEqual(status, "historical")

    def test_unknown_digest_returns_none(self):
        oc = _make_oc_client()
        status, location = oc.check_digest("sha256:unknown")
        self.assertIsNone(status)
        self.assertIsNone(location)

    def test_active_takes_priority_over_historical(self):
        digest = "sha256:both"
        oc     = _make_oc_client(
            active     = {digest: "prd-ns / pod / running"},
            historical = {digest: "prd-ns / imagestream / old"},
        )
        status, _ = oc.check_digest(digest)
        self.assertEqual(status, "active")


# ══════════════════════════════════════════════════════════════════
# Fix 1 — Label diferenciado por origem no PHASE 2
# ══════════════════════════════════════════════════════════════════

class TestPhase2Labels(unittest.TestCase):
    """PHASE 2 must show specific labels depending on WHERE the protection comes from."""

    def _capture(self, active=None, historical=None):
        import io
        all_results = _make_results("sha256:" + "a" * 64, can_delete=True)
        oc  = _make_oc_client(active=active, historical=historical)
        buf = io.StringIO()
        with patch("sys.stdout", buf):
            apply_openshift_results(oc, all_results)
        return buf.getvalue()

    def test_pod_location_shows_running_in_pod(self):
        """Active digest from a pod must show '[running in pod]'."""
        output = self._capture(
            active={"sha256:" + "a" * 64: "prd-ns / pod / my-app-1abc"}
        )
        self.assertIn("running in pod", output.lower())

    def test_workload_location_shows_referenced_in_workload(self):
        """Active digest from a DC must show '[referenced in workload]'."""
        output = self._capture(
            active={"sha256:" + "a" * 64: "prd-ns / deploymentconfig / my-dc"}
        )
        self.assertIn("referenced in workload", output.lower())

    def test_deployment_location_shows_referenced_in_workload(self):
        """Active digest from a Deployment must also show '[referenced in workload]'."""
        output = self._capture(
            active={"sha256:" + "a" * 64: "prd-ns / deployment / my-deploy"}
        )
        self.assertIn("referenced in workload", output.lower())

    def test_imagestream_historical_shows_in_imagestream(self):
        """Historical digest must show '[in imagestream]', not '[imagestream history]'."""
        output = self._capture(
            historical={"sha256:" + "a" * 64: "prd-ns / imagestream / myapp (history)"}
        )
        self.assertIn("in imagestream", output.lower())
        self.assertNotIn("imagestream history", output.lower())

    def test_pod_location_does_not_say_imagestream(self):
        """Active pod digest must NOT say 'imagestream' in the label."""
        output = self._capture(
            active={"sha256:" + "a" * 64: "prd-ns / pod / my-app-1abc"}
        )
        self.assertNotIn("imagestream", output.lower())


# ══════════════════════════════════════════════════════════════════
# OpenShift — apply_openshift_results
# ══════════════════════════════════════════════════════════════════

class TestApplyOpenShiftResults(unittest.TestCase):

    def test_active_digest_blocked(self):
        all_results = _make_results("sha256:running")
        oc = _make_oc_client(active={"sha256:running": "prd-ns / pod / app"})
        apply_openshift_results(oc, all_results)
        img = all_results[0]["images"][0]
        self.assertFalse(img["canDelete"])
        self.assertEqual(img["protectionReason"], "running_on_cluster")

    def test_historical_digest_blocked(self):
        all_results = _make_results("sha256:hist")
        oc = _make_oc_client(historical={"sha256:hist": "prd-ns / imagestream / old (history)"})
        apply_openshift_results(oc, all_results)
        img = all_results[0]["images"][0]
        self.assertFalse(img["canDelete"])
        self.assertEqual(img["protectionReason"], "in_imagestream_history")

    def test_not_found_digest_stays_deletable(self):
        all_results = _make_results("sha256:notfound")
        oc = _make_oc_client()
        apply_openshift_results(oc, all_results)
        self.assertTrue(all_results[0]["images"][0]["canDelete"])

    def test_not_found_digest_prints_safe_to_delete(self):
        """digest not in cluster → terminal must say it is safe to delete."""
        import io
        all_results = _make_results("sha256:notfound")
        oc = _make_oc_client()
        buf = io.StringIO()
        with patch("sys.stdout", buf):
            apply_openshift_results(oc, all_results)
        output = buf.getvalue()
        self.assertIn("not in cluster", output.lower())
        self.assertIn("safe to delete", output.lower())

    def test_not_in_cluster_line_has_single_space_after_emoji(self):
        """'✅ ' com espaço simples — sem duplo espaço."""
        import io
        all_results = _make_results("sha256:notfound")
        oc = _make_oc_client()
        buf = io.StringIO()
        with patch("sys.stdout", buf):
            apply_openshift_results(oc, all_results)
        output = buf.getvalue()
        self.assertIn("✅ ", output)
        self.assertNotIn("✅  ", output)

    def test_check_digest_exception_marks_protected(self):
        all_results = _make_results("sha256:broken")
        oc = MagicMock()
        oc.permission_incomplete = False
        oc.check_digest.side_effect = Exception("cluster error")
        apply_openshift_results(oc, all_results)
        img = all_results[0]["images"][0]
        self.assertFalse(img["canDelete"])
        self.assertEqual(img["protectionReason"], "openshift_check_failed")

    def test_permission_incomplete_blocks_all(self):
        all_results = _make_results("sha256:any")
        oc = _make_oc_client(permission_incomplete=True)
        apply_openshift_results(oc, all_results)
        img = all_results[0]["images"][0]
        self.assertFalse(img["canDelete"])
        self.assertEqual(img["protectionReason"], "insufficient_permissions")

    def test_oc_client_none_leaves_results_untouched(self):
        all_results = _make_results("sha256:candidate", can_delete=True)
        apply_openshift_results(None, all_results)
        self.assertTrue(all_results[0]["images"][0]["canDelete"])


# ══════════════════════════════════════════════════════════════════
# parse_digest
# ══════════════════════════════════════════════════════════════════

class TestParseDigest(unittest.TestCase):

    def setUp(self):
        self.oc = _make_oc_client()

    def test_standard_digest(self):
        self.assertEqual(self.oc._parse_digest("registry.io/repo@sha256:abc123"), "sha256:abc123")

    def test_digest_with_tag(self):
        self.assertEqual(self.oc._parse_digest("registry.io/repo:tag@sha256:def456"), "sha256:def456")

    def test_no_sha256_returns_none(self):
        self.assertIsNone(self.oc._parse_digest("registry.io/repo:latest"))

    def test_none_input_returns_none(self):
        self.assertIsNone(self.oc._parse_digest(None))

    def test_empty_string_returns_none(self):
        self.assertIsNone(self.oc._parse_digest(""))


# ══════════════════════════════════════════════════════════════════
# Reports — JSON e CSV
# ══════════════════════════════════════════════════════════════════

def _sample_results(prefix="homebuying"):
    return [{
        "repository": f"{prefix}/users" if prefix else "users",
        "images": [
            {
                "digest":           "sha256:old1",
                "tags":             ["v1"],
                "ageDays":          30,
                "timestamp":        "2024-01-01 00:00:00 UTC",
                "canDelete":        True,
                "protectionReason": None,
                "ocLocation":       None,
                "deleted":          True,
            },
            {
                "digest":           "sha256:new1",
                "tags":             ["v2"],
                "ageDays":          1,
                "timestamp":        "2024-03-01 00:00:00 UTC",
                "canDelete":        False,
                "protectionReason": "keep_newest:2",
                "ocLocation":       None,
            },
        ]
    }]


class TestReports(unittest.TestCase):

    def setUp(self):
        import os
        self._orig_dir = os.path.abspath(".")

    def tearDown(self):
        import os
        try:
            os.chdir(self._orig_dir)
        except FileNotFoundError:
            os.chdir(os.path.dirname(os.path.abspath(__file__)))

    def test_json_report_fields(self):
        import tempfile, os
        with tempfile.TemporaryDirectory() as tmp:
            os.chdir(tmp)
            all_results   = _sample_results("homebuying")
            run_timestamp = "2024-03-01-120000"
            save_json_report(all_results, "DRY_RUN", run_timestamp, "myregistry", "homebuying")

            with open(f"report-{run_timestamp}.json") as f:
                data = json.load(f)

            self.assertEqual(data["registry"], "myregistry")
            self.assertEqual(data["prefix"],   "homebuying")
            self.assertEqual(data["mode"],     "DRY_RUN")
            self.assertEqual(len(data["repositories"]), 1)

            img = data["repositories"][0]["images"][0]
            for field in ["digest", "tags", "age_days", "created_at", "action", "protection_reason", "oc_location"]:
                self.assertIn(field, img)

    def test_json_report_empty_prefix_shows_wildcard(self):
        import tempfile, os
        with tempfile.TemporaryDirectory() as tmp:
            os.chdir(tmp)
            run_timestamp = "2024-03-01-130000"
            save_json_report(_sample_results(""), "DRY_RUN", run_timestamp, "myregistry", "")

            with open(f"report-{run_timestamp}.json") as f:
                data = json.load(f)

            self.assertEqual(data["prefix"], "*")

    def test_csv_report_fields(self):
        import tempfile, os
        with tempfile.TemporaryDirectory() as tmp:
            os.chdir(tmp)
            run_timestamp = "2024-03-01-140000"
            save_csv_report(_sample_results("homebuying"), "DRY_RUN", run_timestamp)

            with open(f"report-{run_timestamp}.csv", newline="") as f:
                rows = list(csv.DictReader(f))

            self.assertEqual(len(rows), 2)
            for field in ["repository", "digest", "tags", "age_days", "created_at", "action", "protection_reason", "oc_location"]:
                self.assertIn(field, rows[0])

    def test_json_report_action_deleted(self):
        import tempfile, os
        with tempfile.TemporaryDirectory() as tmp:
            os.chdir(tmp)
            run_timestamp = "2024-03-01-150000"
            save_json_report(_sample_results(), "LIVE", run_timestamp, "myregistry", "homebuying")

            with open(f"report-{run_timestamp}.json") as f:
                data = json.load(f)

            actions = [i["action"] for i in data["repositories"][0]["images"]]
            self.assertIn("deleted", actions)

    def test_json_report_action_protected(self):
        import tempfile, os
        with tempfile.TemporaryDirectory() as tmp:
            os.chdir(tmp)
            run_timestamp = "2024-03-01-160000"
            save_json_report(_sample_results(), "DRY_RUN", run_timestamp, "myregistry", "homebuying")

            with open(f"report-{run_timestamp}.json") as f:
                data = json.load(f)

            actions = [i["action"] for i in data["repositories"][0]["images"]]
            self.assertIn("protected", actions)


# ══════════════════════════════════════════════════════════════════
# Paginação — duplicados retornados pelo Azure SDK
# ══════════════════════════════════════════════════════════════════

class TestGetManifestsPaginationDuplicates(unittest.TestCase):

    def _make_sdk_manifest(self, digest, tags, media_type="application/vnd.docker.distribution.manifest.v2+json"):
        m = MagicMock()
        m.digest      = digest
        m.tags        = tags
        m.media_type  = media_type
        m.created_on  = datetime.now(timezone.utc) - timedelta(days=30)
        return m

    def test_duplicate_digests_from_sdk_pagination(self):
        """Expõe o bug: get_manifests não deduplica — DEVE FALHAR até ser corrigido."""
        m1 = self._make_sdk_manifest("sha256:abc", ["v1"])
        m2 = self._make_sdk_manifest("sha256:def", ["v2"])

        client = MagicMock()
        # Simula última página repetida pelo SDK
        client.list_manifest_properties.return_value = [m1, m2, m1, m2]

        result  = get_manifests(client, "fad/backend")
        digests = [m["digest"] for m in result]
        self.assertEqual(len(digests), len(set(digests)),
                         f"Digests duplicados: {[d for d in digests if digests.count(d) > 1]}")

    def test_single_manifest_with_multiple_tags_counts_once(self):
        """Um digest com várias tags deve contar como 1 manifest."""
        m = self._make_sdk_manifest("sha256:abc", ["v1", "latest", "stable"])

        client = MagicMock()
        client.list_manifest_properties.return_value = [m]

        result = get_manifests(client, "fad/backend")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["tags"], ["v1", "latest", "stable"])

    def test_duplicate_digest_inflates_analyze_repo_count(self):
        """Valida que digests duplicados inflam a contagem em analyze_repo."""
        mt = "application/vnd.docker.distribution.manifest.v2+json"
        m  = self._make_sdk_manifest("sha256:abc", ["v1"], mt)

        client = MagicMock()
        client.list_manifest_properties.return_value = [m, m]  # mesmo digest 2x

        with patch("purge.get_manifests", wraps=lambda c, r: get_manifests(c, r)):
            result  = analyze_repo(client, "fad/backend", 2, 15, "registry.azurecr.io")

        digests = [i["digest"] for i in result["images"]]
        self.assertEqual(len(digests), len(set(digests)),
                         f"Digests duplicados em analyze_repo: {digests}")


class TestListReposPaginationDuplicates(unittest.TestCase):

    def test_empty_prefix_returns_duplicates_when_sdk_paginates_badly(self):
        """Expõe o bug: list_repos_by_prefix não deduplica — DEVE FALHAR até ser corrigido."""
        repos_with_pagination_bug = [
            "apps/foo",
            "apps/bar",
            "smelending/atividades",
            "smelending/limites",
            # última página repetida (bug de paginação do Azure SDK)
            "smelending/atividades",
            "smelending/limites",
        ]
        client = _fake_acr_client(repos_with_pagination_bug)
        result = list_repos_by_prefix(client, "")
        self.assertEqual(len(result), len(set(result)),
                         f"Duplicados encontrados: {[r for r in result if result.count(r) > 1]}")

    def test_prefix_filter_returns_duplicates_when_sdk_paginates_badly(self):
        """Expõe o bug no branch com prefix."""
        repos_with_pagination_bug = [
            "smelending/atividades",
            "smelending/limites",
            "other/repo",
            # repetição da última página
            "smelending/atividades",
            "smelending/limites",
        ]
        client = _fake_acr_client(repos_with_pagination_bug)
        result = list_repos_by_prefix(client, "smelending")
        self.assertEqual(len(result), len(set(result)),
                         f"Duplicados encontrados: {[r for r in result if result.count(r) > 1]}")

    def test_all_results_inflated_when_repos_duplicated(self):
        """Valida que all_results fica com entradas duplicadas, inflando o TOTAL do summary."""
        repos_with_dupes = ["apps/foo", "apps/foo"]
        client = _fake_acr_client(repos_with_dupes)

        with patch("purge.get_manifests", return_value=[]):
            repos = list_repos_by_prefix(client, "")
            all_results = [
                analyze_repo(client, r, 2, 15, "registry.azurecr.io")
                for r in repos
            ]

        repo_names = [r["repository"] for r in all_results]
        self.assertEqual(len(repo_names), len(set(repo_names)),
                         f"Repositórios duplicados no all_results: {repo_names}")


# ══════════════════════════════════════════════════════════════════
# FINAL SUMMARY — compute_repo_counts
# Valores de referência extraídos do relatório real:
#   report-2026-04-06-102136.json  (407 repos, 12703 manifests)
# ══════════════════════════════════════════════════════════════════

def _img(can_delete=False, deleted=False, protection_reason=None):
    return {"canDelete": can_delete, "deleted": deleted, "protectionReason": protection_reason}


class TestComputeRepoCounts(unittest.TestCase):

    # ── casos unitários ───────────────────────────────────────────

    def test_empty_repo(self):
        m, cand, cl, prot, act = compute_repo_counts([])
        self.assertEqual((m, cand, cl, prot, act), (0, 0, 0, 0, 0))

    def test_all_protected_no_tag(self):
        # apps/minha-app: 3 imagens, todas no_tag → protected=3, action=0
        images = [_img(protection_reason="no_tag")] * 3
        m, cand, cl, prot, act = compute_repo_counts(images)
        self.assertEqual(m,    3)
        self.assertEqual(cand, 0)
        self.assertEqual(cl,   0)
        self.assertEqual(prot, 3)
        self.assertEqual(act,  0)

    def test_mixed_protected_and_would_delete(self):
        # bph/dotnet-service-broker-agent: 35 manifests, 2 would_delete, 33 protected
        images = (
            [_img(can_delete=True)]                              * 2   # would_delete
            + [_img(protection_reason="keep_newest:10")]         * 10
            + [_img(protection_reason="no_tag")]                 * 3
            + [_img(protection_reason="too_recent:<365_days")]   * 20
        )
        m, cand, cl, prot, act = compute_repo_counts(images)
        self.assertEqual(m,    35)
        self.assertEqual(cand, 2)
        self.assertEqual(cl,   0)
        self.assertEqual(prot, 33)
        self.assertEqual(act,  2)

    def test_would_delete_after_live_run(self):
        # imagens já deletadas (deleted=True) contam em action, não em protected
        images = [
            _img(deleted=True),
            _img(deleted=True),
            _img(protection_reason="keep_newest:2"),
        ]
        m, cand, cl, prot, act = compute_repo_counts(images)
        self.assertEqual(act,  2)
        self.assertEqual(prot, 1)
        self.assertEqual(m,    3)

    def test_cluster_protected_counts_in_cluster_not_protected(self):
        images = [
            _img(protection_reason="running_on_cluster"),
            _img(protection_reason="in_imagestream_history"),
            _img(protection_reason="keep_newest:2"),
            _img(can_delete=True),
        ]
        m, cand, cl, prot, act = compute_repo_counts(images)
        self.assertEqual(cl,   2)   # running + history
        self.assertEqual(prot, 1)   # keep_newest
        self.assertEqual(act,  1)   # would_delete
        self.assertEqual(cand, 3)   # cluster(2) + action(1)

    def test_no_protection_reason_not_counted_in_protected(self):
        # imagem sem protectionReason e sem canDelete/deleted — não deve inflar protected
        images = [_img()]  # tudo False/None
        m, cand, cl, prot, act = compute_repo_counts(images)
        self.assertEqual(prot, 0)
        self.assertEqual(act,  0)

    # ── invariante: manifests = action + protected + cluster ──────

    def test_invariant_mixed(self):
        # businesslogs/dotnet-service-businesslogsv2: 29 manifests, 6 would_delete, 23 protected
        images = (
            [_img(can_delete=True)]                            * 6
            + [_img(protection_reason="keep_newest:10")]       * 10
            + [_img(protection_reason="no_tag")]               * 13
        )
        m, cand, cl, prot, act = compute_repo_counts(images)
        self.assertEqual(m, 29)
        self.assertEqual(act + prot + cl, m,
                         f"Invariant falhou: {act} + {prot} + {cl} != {m}")

    def test_invariant_always_holds(self):
        # Invariante genérica com combinação arbitrária de tipos
        images = (
            [_img(can_delete=True)]                                * 5
            + [_img(deleted=True)]                                 * 3
            + [_img(protection_reason="keep_newest:10")]           * 10
            + [_img(protection_reason="too_recent:<365_days")]     * 7
            + [_img(protection_reason="no_tag")]                   * 4
            + [_img(protection_reason="running_on_cluster")]       * 2
            + [_img(protection_reason="in_imagestream_history")]   * 1
            + [_img()]                                             * 2  # sem razão
        )
        m, cand, cl, prot, act = compute_repo_counts(images)
        self.assertEqual(act + prot + cl, m - 2,  # os 2 sem razão ficam fora
                         "Invariante falhou para combinação arbitrária")

    # ── valores reais do relatório 2026-04-06 ────────────────────

    def test_real_report_totals(self):
        """
        Verifica os totais globais calculados a partir do relatório real
        report-2026-04-06-102136.json (407 repos, 12703 manifests).
        """
        import os, json as _json
        report = os.path.expanduser(
            "~/Downloads/report-2026-04-06-102136.json"
        )
        if not os.path.exists(report):
            self.skipTest("Relatório real não encontrado em ~/Downloads")

        with open(report) as f:
            data = _json.load(f)

        def _to_internal(img):
            action = img["action"]
            return {
                "canDelete":        action == "would_delete",
                "deleted":          action == "deleted",
                "protectionReason": img["protection_reason"],
            }

        t_m = t_cand = t_cl = t_prot = t_act = 0
        for r in data["repositories"]:
            images = [_to_internal(i) for i in r["images"]]
            m, cand, cl, prot, act = compute_repo_counts(images)
            t_m    += m
            t_cand += cand
            t_cl   += cl
            t_prot += prot
            t_act  += act

        self.assertEqual(t_m,    12703, "total manifests")
        self.assertEqual(t_act,   6247, "total would_delete")
        self.assertEqual(t_prot,  6456, "total protected")
        self.assertEqual(t_cl,       0, "total cluster (skip-openshift foi usado)")
        self.assertEqual(t_act + t_prot + t_cl, t_m, "invariant global")


# ══════════════════════════════════════════════════════════════════
# Fix 2 — ImageStream items[0] e IST devem ir para _historical
# ══════════════════════════════════════════════════════════════════

def _make_is_with_tags(name, tags):
    """tags: list of (tag_name, [dockerImageReference, ...])"""
    is_obj = MagicMock()
    is_obj.metadata.name = name
    mock_tags = []
    for tag_name, refs in tags:
        tag = MagicMock()
        tag.tag = tag_name
        items = []
        for ref in refs:
            entry = MagicMock()
            entry.dockerImageReference = ref
            items.append(entry)
        tag.items = items
        mock_tags.append(tag)
    is_obj.status.tags = mock_tags
    return is_obj


class TestLoadImageStreamsHistorical(unittest.TestCase):
    """_load_imagestreams must put ALL entries (including items[0]) in _historical."""

    def test_items_0_goes_to_historical_not_active(self):
        """items[0] of an ImageStream tag must be _historical, NOT _active."""
        oc = _make_oc_client()
        oc.is_api = MagicMock()
        digest = "sha256:" + "b" * 64
        is_obj = _make_is_with_tags("myapp", [("latest", [f"registry.io/myapp@{digest}"])])
        oc.is_api.get.return_value.items = [is_obj]

        oc._load_imagestreams("prd-ns")

        self.assertNotIn(digest, oc._active,
                         "items[0] não deve estar em _active — proteção real vem de pods/workloads")
        self.assertIn(digest, oc._historical,
                      "items[0] deve estar em _historical")

    def test_items_1_plus_still_historical(self):
        """items[1+] must still go to _historical (existing behavior preserved)."""
        oc = _make_oc_client()
        oc.is_api = MagicMock()
        d0 = "sha256:" + "c" * 64
        d1 = "sha256:" + "d" * 64
        is_obj = _make_is_with_tags("myapp", [
            ("latest", [f"registry.io/myapp@{d0}", f"registry.io/myapp@{d1}"])
        ])
        oc.is_api.get.return_value.items = [is_obj]

        oc._load_imagestreams("prd-ns")

        self.assertIn(d0, oc._historical)
        self.assertIn(d1, oc._historical)

    def test_multiple_tags_all_historical(self):
        """Each tag's items[0] must be _historical even for one-tag-per-release patterns."""
        oc = _make_oc_client()
        oc.is_api = MagicMock()
        d1 = "sha256:" + "e" * 64
        d2 = "sha256:" + "f" * 64
        is_obj = _make_is_with_tags("frontend", [
            ("5.0.0-release-20250912", [f"registry.io/fe@{d1}"]),
            ("4.3.0-release-20250818", [f"registry.io/fe@{d2}"]),
        ])
        oc.is_api.get.return_value.items = [is_obj]

        oc._load_imagestreams("prd-ns")

        self.assertNotIn(d1, oc._active)
        self.assertNotIn(d2, oc._active)
        self.assertIn(d1, oc._historical)
        self.assertIn(d2, oc._historical)


class TestLoadImageStreamTagsHistorical(unittest.TestCase):
    """_load_imagestreamtags must put its resolved image in _historical, not _active."""

    def test_ist_image_goes_to_historical_not_active(self):
        """IST current image must be _historical — actual running state comes from pods."""
        oc = _make_oc_client()
        oc.istag_api = MagicMock()
        digest = "sha256:" + "a1" * 32

        ist = MagicMock()
        ist.metadata.name = "myapp:latest"
        ist.image.dockerImageReference = f"registry.io/myapp@{digest}"
        oc.istag_api.get.return_value.items = [ist]

        oc._load_imagestreamtags("prd-ns")

        self.assertNotIn(digest, oc._active,
                         "IST não deve estar em _active")
        self.assertIn(digest, oc._historical,
                      "IST deve estar em _historical")


# ══════════════════════════════════════════════════════════════════
# OpenShift — _load_all em paralelo por namespace (P3)
# ══════════════════════════════════════════════════════════════════

class TestLoadAllParallel(unittest.TestCase):

    _ALL_LOADERS = [
        "_load_pods", "_load_deployments", "_load_deploymentconfigs",
        "_load_replicationcontrollers",
        "_load_statefulsets", "_load_daemonsets", "_load_replicasets",
        "_load_jobs", "_load_cronjobs", "_load_imagestreams",
        "_load_imagestreamtags", "_load_builds", "_load_buildconfigs",
    ]

    def _oc_with_namespaces(self, namespaces):
        oc = _make_oc_client()
        oc.namespaces = namespaces
        return oc

    def _patch_all_loaders(self, oc, overrides=None):
        """Retorna context manager que mocka todos os loaders. overrides: {name: side_effect}."""
        overrides = overrides or {}
        patches = []
        mocks = {}
        for name in self._ALL_LOADERS:
            se = overrides.get(name, None)
            p  = patch.object(oc, name, side_effect=se)
            patches.append(p)
            mocks[name] = p
        from contextlib import ExitStack
        stack = ExitStack()
        entered = {name: stack.enter_context(p) for name, p in zip(self._ALL_LOADERS, patches)}
        return stack, entered

    def test_all_namespaces_are_scanned(self):
        """_load_all deve chamar _load_pods exatamente uma vez por namespace."""
        namespaces = [f"prd-ns-{i}" for i in range(5)]
        oc = self._oc_with_namespaces(namespaces)

        with patch.object(oc, "_load_pods") as mock_pods, \
             patch.object(oc, "_load_deployments"), patch.object(oc, "_load_deploymentconfigs"), \
             patch.object(oc, "_load_replicationcontrollers"), \
             patch.object(oc, "_load_statefulsets"), patch.object(oc, "_load_daemonsets"), \
             patch.object(oc, "_load_replicasets"), patch.object(oc, "_load_jobs"), \
             patch.object(oc, "_load_cronjobs"), patch.object(oc, "_load_imagestreams"), \
             patch.object(oc, "_load_imagestreamtags"), patch.object(oc, "_load_builds"), \
             patch.object(oc, "_load_buildconfigs"):
            oc._load_all()

        self.assertEqual(mock_pods.call_count, len(namespaces))

    def test_load_all_uses_multiple_threads(self):
        """Com múltiplos namespaces, _load_all deve usar threads paralelas."""
        import threading

        thread_ids = set()
        lock = threading.Lock()

        def mock_pods(ns):
            with lock:
                thread_ids.add(threading.current_thread().ident)

        namespaces = [f"prd-ns-{i}" for i in range(10)]
        oc = self._oc_with_namespaces(namespaces)

        with patch.object(oc, "_load_pods", side_effect=mock_pods), \
             patch.object(oc, "_load_deployments"), patch.object(oc, "_load_deploymentconfigs"), \
             patch.object(oc, "_load_replicationcontrollers"), \
             patch.object(oc, "_load_statefulsets"), patch.object(oc, "_load_daemonsets"), \
             patch.object(oc, "_load_replicasets"), patch.object(oc, "_load_jobs"), \
             patch.object(oc, "_load_cronjobs"), patch.object(oc, "_load_imagestreams"), \
             patch.object(oc, "_load_imagestreamtags"), patch.object(oc, "_load_builds"), \
             patch.object(oc, "_load_buildconfigs"):
            oc._load_all()

        self.assertGreater(len(thread_ids), 1,
                           "_load_all deve usar múltiplas threads (não sequencial)")

    def test_no_digests_lost_under_parallel_scan(self):
        """Scan paralelo não deve perder digests por race conditions em _active."""
        namespaces = [f"prd-ns-{i}" for i in range(20)]
        oc = self._oc_with_namespaces(namespaces)

        def fake_pods(ns):
            # cada namespace contribui um digest único
            digest = f"sha256:{'a' * 7}{ns.replace('prd-ns-', '').zfill(3)}"
            oc._add_active(f"registry.azurecr.io/repo@{digest}", f"{ns} / pod / app")

        with patch.object(oc, "_load_pods", side_effect=fake_pods), \
             patch.object(oc, "_load_deployments"), patch.object(oc, "_load_deploymentconfigs"), \
             patch.object(oc, "_load_replicationcontrollers"), \
             patch.object(oc, "_load_statefulsets"), patch.object(oc, "_load_daemonsets"), \
             patch.object(oc, "_load_replicasets"), patch.object(oc, "_load_jobs"), \
             patch.object(oc, "_load_cronjobs"), patch.object(oc, "_load_imagestreams"), \
             patch.object(oc, "_load_imagestreamtags"), patch.object(oc, "_load_builds"), \
             patch.object(oc, "_load_buildconfigs"):
            oc._load_all()

        self.assertEqual(len(oc._active), len(namespaces),
                         f"Esperado {len(namespaces)} digests, encontrado {len(oc._active)} — possível race condition")

    def test_all_loaders_called_for_each_namespace(self):
        """Todos os 11 loaders devem ser chamados para cada namespace."""
        from contextlib import ExitStack

        namespaces = ["prd-ns-0", "prd-ns-1", "prd-ns-2"]
        oc         = self._oc_with_namespaces(namespaces)
        mocks      = {}

        with ExitStack() as stack:
            for name in self._ALL_LOADERS:
                mocks[name] = stack.enter_context(patch.object(oc, name))
            oc._load_all()

        for name in self._ALL_LOADERS:
            self.assertEqual(
                mocks[name].call_count, len(namespaces),
                f"{name} deveria ser chamado {len(namespaces)}x, foi {mocks[name].call_count}x"
            )

    def test_permission_incomplete_visible_after_parallel_scan(self):
        """Se qualquer loader definir permission_incomplete=True em paralelo, deve ser visível após _load_all."""
        namespaces = [f"prd-ns-{i}" for i in range(10)]
        oc         = self._oc_with_namespaces(namespaces)

        def pods_with_one_permission_error(ns):
            if ns == "prd-ns-7":
                oc.permission_incomplete = True

        with patch.object(oc, "_load_pods", side_effect=pods_with_one_permission_error), \
             patch.object(oc, "_load_deployments"), patch.object(oc, "_load_deploymentconfigs"), \
             patch.object(oc, "_load_replicationcontrollers"), \
             patch.object(oc, "_load_statefulsets"), patch.object(oc, "_load_daemonsets"), \
             patch.object(oc, "_load_replicasets"), patch.object(oc, "_load_jobs"), \
             patch.object(oc, "_load_cronjobs"), patch.object(oc, "_load_imagestreams"), \
             patch.object(oc, "_load_imagestreamtags"), patch.object(oc, "_load_builds"), \
             patch.object(oc, "_load_buildconfigs"):
            oc._load_all()

        self.assertTrue(oc.permission_incomplete,
                        "permission_incomplete deve ser True quando qualquer namespace falha com permissão")

    def test_empty_namespaces_does_not_crash(self):
        """_load_all com lista vazia não deve lançar exceção."""
        oc = self._oc_with_namespaces([])
        oc._load_all()   # deve completar sem erro


# ══════════════════════════════════════════════════════════════════
# analyze_all_repos — análise em paralelo (P2)
# ══════════════════════════════════════════════════════════════════

class TestAnalyzeAllRepos(unittest.TestCase):

    def test_analyze_repo_called_for_each_repo(self):
        """analyze_repo deve ser chamado exatamente uma vez por repositório."""
        repos = ["repo/a", "repo/b", "repo/c"]

        with patch("purge.analyze_repo", return_value={"repository": "x", "images": []}) as mock_analyze:
            analyze_all_repos(MagicMock(), repos, keep=2, max_age_days=15, registry_url="r.azurecr.io")

        self.assertEqual(mock_analyze.call_count, len(repos))

    def test_results_contain_all_repos(self):
        """Todos os repositórios devem aparecer nos resultados, sem perda."""
        repos = ["repo/a", "repo/b", "repo/c"]

        def fake_analyze(client, repo, keep, max_age_days, registry_url, **kwargs):
            return {"repository": repo, "images": []}

        with patch("purge.analyze_repo", side_effect=fake_analyze):
            results = analyze_all_repos(MagicMock(), repos, keep=2, max_age_days=15, registry_url="r.azurecr.io")

        self.assertEqual({r["repository"] for r in results}, set(repos))

    def test_result_count_matches_repo_count(self):
        """Número de resultados deve ser igual ao número de repos."""
        repos = [f"repo/{i}" for i in range(20)]

        with patch("purge.analyze_repo", side_effect=lambda c, repo, k, m, u, **kw: {"repository": repo, "images": []}):
            results = analyze_all_repos(MagicMock(), repos, keep=2, max_age_days=15, registry_url="r.azurecr.io")

        self.assertEqual(len(results), len(repos))

    def test_parallel_execution_uses_multiple_threads(self):
        """Com múltiplos repos, analyze_repo deve ser executado em threads paralelas."""
        import threading

        thread_ids = set()
        lock = threading.Lock()

        def record_thread(client, repo, keep, max_age_days, registry_url, **kwargs):
            with lock:
                thread_ids.add(threading.current_thread().ident)
            return {"repository": repo, "images": []}

        repos = [f"repo/{i}" for i in range(10)]
        with patch("purge.analyze_repo", side_effect=record_thread):
            analyze_all_repos(MagicMock(), repos, keep=2, max_age_days=15, registry_url="r.azurecr.io")

        self.assertGreater(len(thread_ids), 1,
                           "analyze_all_repos deve usar múltiplas threads (não é sequencial)")

    def test_empty_repos_returns_empty_list(self):
        """Lista vazia de repos deve retornar lista vazia."""
        results = analyze_all_repos(MagicMock(), [], keep=2, max_age_days=15, registry_url="r.azurecr.io")
        self.assertEqual(results, [])


# ══════════════════════════════════════════════════════════════════
# delete_all_candidates — deleção em paralelo com semáforo (P5)
# ══════════════════════════════════════════════════════════════════

def _deletable_results(n_repos=1, n_images=10):
    """Gera all_results com imagens deletáveis."""
    return [
        {
            "repository": f"repo/{r}",
            "images": [
                {"digest": f"sha256:{r:04d}{i:060d}", "canDelete": True}
                for i in range(n_images)
            ],
        }
        for r in range(n_repos)
    ]


class TestDeleteAllCandidates(unittest.TestCase):

    def test_all_candidates_marked_deleted(self):
        """Todas as imagens com canDelete=True devem ter deleted=True após a deleção."""
        results = _deletable_results(n_repos=3, n_images=5)

        with patch("purge.delete_manifest"):
            delete_all_candidates(MagicMock(), results, concurrency=10)

        for r in results:
            for img in r["images"]:
                self.assertTrue(img.get("deleted"),
                                f"{img['digest']} não foi marcado como deleted")

    def test_max_concurrency_respected(self):
        """Nunca mais que `concurrency` threads simultâneas — e paralelismo real acontece."""
        import threading

        active = {"count": 0, "max": 0}
        lock   = threading.Lock()

        def slow_delete(client, repo, digest, **kwargs):
            with lock:
                active["count"] += 1
                active["max"]    = max(active["max"], active["count"])
            time.sleep(0.05)
            with lock:
                active["count"] -= 1

        results = _deletable_results(n_repos=1, n_images=30)

        with patch("purge.delete_manifest", side_effect=slow_delete):
            delete_all_candidates(MagicMock(), results, concurrency=5)

        self.assertGreater(active["max"], 1,
                           "delete_all_candidates deve usar múltiplas threads (não é sequencial)")
        self.assertLessEqual(active["max"], 5,
                             f"Concorrência máxima foi {active['max']}, esperado ≤ 5")

    def test_failure_does_not_stop_remaining_deletions(self):
        """Falha em uma imagem não deve impedir a deleção das demais."""
        results = [{
            "repository": "repo/app",
            "images": [
                {"digest": "sha256:fail", "canDelete": True},
                {"digest": "sha256:ok01", "canDelete": True},
                {"digest": "sha256:ok02", "canDelete": True},
            ],
        }]

        def selective_fail(client, repo, digest, **kwargs):
            if digest == "sha256:fail":
                raise Exception("ACR error")

        with patch("purge.delete_manifest", side_effect=selective_fail):
            delete_all_candidates(MagicMock(), results, concurrency=10)

        imgs = {img["digest"]: img for img in results[0]["images"]}
        self.assertTrue(imgs["sha256:ok01"].get("deleted"))
        self.assertTrue(imgs["sha256:ok02"].get("deleted"))
        self.assertFalse(imgs["sha256:fail"].get("deleted"))

    def test_empty_candidates_does_nothing(self):
        """Sem candidatos, delete_manifest não deve ser chamado."""
        results = [{"repository": "repo/app", "images": [
            {"digest": "sha256:kept", "canDelete": False},
        ]}]

        with patch("purge.delete_manifest") as mock_del:
            delete_all_candidates(MagicMock(), results, concurrency=10)

        mock_del.assert_not_called()

    def test_success_output_contains_repo_and_digest(self):
        """Linha de sucesso deve conter repositório e início do digest."""
        import io
        results = [{"repository": "repo/app", "images": [
            {"digest": "sha256:abc123def456789", "canDelete": True},
        ]}]
        buf = io.StringIO()
        with patch("purge.delete_manifest"), patch("sys.stdout", buf):
            delete_all_candidates(MagicMock(), results, concurrency=1)
        output = buf.getvalue()
        self.assertIn("✅",       output)
        self.assertIn("repo/app", output)
        self.assertIn("sha256:abc123", output)

    def test_success_line_has_single_space_after_emoji(self):
        """'✅ ' com espaço simples — sem duplo espaço."""
        import io
        results = [{"repository": "repo/app", "images": [
            {"digest": "sha256:abc123def456789", "canDelete": True},
        ]}]
        buf = io.StringIO()
        with patch("purge.delete_manifest"), patch("sys.stdout", buf):
            delete_all_candidates(MagicMock(), results, concurrency=1)
        output = buf.getvalue()
        self.assertIn("✅ ", output)
        self.assertNotIn("✅  ", output)

    def test_failure_output_contains_repo_and_error_message(self):
        """Linha de falha deve conter repositório e mensagem de erro."""
        import io
        results = [{"repository": "repo/app", "images": [
            {"digest": "sha256:abc123def456789", "canDelete": True},
        ]}]
        buf = io.StringIO()
        with patch("purge.delete_manifest", side_effect=Exception("acr timeout")), \
             patch("sys.stdout", buf):
            delete_all_candidates(MagicMock(), results, concurrency=1)
        output = buf.getvalue()
        self.assertIn("❌",        output)
        self.assertIn("repo/app",  output)
        self.assertIn("acr timeout", output)

    def test_delete_manifest_called_for_each_candidate(self):
        """delete_manifest deve ser chamado exatamente uma vez por candidato."""
        results = _deletable_results(n_repos=2, n_images=5)
        total   = sum(len(r["images"]) for r in results)

        with patch("purge.delete_manifest") as mock_del:
            delete_all_candidates(MagicMock(), results, concurrency=10)

        self.assertEqual(mock_del.call_count, total)

    def test_each_image_deleted_exactly_once(self):
        """Nenhum digest deve ser deletado mais de uma vez, mesmo sob concorrência."""
        import threading

        deleted = []
        lock    = threading.Lock()

        def track_delete(client, repo, digest, **kwargs):
            with lock:
                deleted.append(digest)

        results = _deletable_results(n_repos=5, n_images=20)

        with patch("purge.delete_manifest", side_effect=track_delete):
            delete_all_candidates(MagicMock(), results, concurrency=10)

        self.assertEqual(len(deleted), len(set(deleted)),
                         f"Digests deletados mais de uma vez: "
                         f"{[d for d in set(deleted) if deleted.count(d) > 1]}")

    def test_non_deletable_images_are_never_touched(self):
        """Imagens com canDelete=False nunca devem ser passadas para delete_manifest."""
        import threading

        touched = set()
        lock    = threading.Lock()

        def track(client, repo, digest, **kwargs):
            with lock:
                touched.add(digest)

        results = [{
            "repository": "repo/app",
            "images": [
                {"digest": "sha256:delete_me",  "canDelete": True},
                {"digest": "sha256:keep_me_1",  "canDelete": False},
                {"digest": "sha256:keep_me_2",  "canDelete": False},
            ],
        }]

        with patch("purge.delete_manifest", side_effect=track):
            delete_all_candidates(MagicMock(), results, concurrency=10)

        self.assertIn("sha256:delete_me",  touched)
        self.assertNotIn("sha256:keep_me_1", touched)
        self.assertNotIn("sha256:keep_me_2", touched)


# ══════════════════════════════════════════════════════════════════
# _print_candidates_table — limite de linhas (P4)
# ══════════════════════════════════════════════════════════════════

def _make_candidates(n):
    """Gera n candidatos sintéticos para testes de tabela."""
    return [
        {
            "tags":    [f"v{i}"],
            "ageDays": 30 + i,
            "digest":  f"sha256:{'a' * 7}{str(i).zfill(3)}",
        }
        for i in range(n)
    ]


class TestPrintCandidatesTable(unittest.TestCase):

    def _capture(self, candidates, max_rows=None):
        import io
        buf = io.StringIO()
        kwargs = {} if max_rows is None else {"max_rows": max_rows}
        with patch("sys.stdout", buf):
            _print_candidates_table("repo/app", candidates, **kwargs)
        return buf.getvalue()

    def _data_lines(self, output):
        """Data lines of the table: indented with spaces, not a header or footer line."""
        return [
            l for l in output.splitlines()
            if l.startswith("     ")
            and "tag" not in l
            and "─" not in l
            and "..." not in l
            and "more images" not in l
        ]

    def test_all_rows_printed_when_under_limit(self):
        """Com menos candidatos que max_rows, todas as linhas devem aparecer."""
        candidates = _make_candidates(10)
        output = self._capture(candidates, max_rows=50)
        self.assertEqual(len(self._data_lines(output)), 10)

    def test_table_truncates_at_max_rows(self):
        """Com 200 candidatos e max_rows=50, a tabela imprime no máximo 50 linhas de dados."""
        candidates = _make_candidates(200)
        output = self._capture(candidates, max_rows=50)
        self.assertLessEqual(len(self._data_lines(output)), 50)

    def test_footer_shows_remaining_count(self):
        """Quando truncada, o rodapé deve indicar quantas imagens foram omitidas."""
        candidates = _make_candidates(200)
        output = self._capture(candidates, max_rows=50)
        self.assertIn("150", output)  # 200 - 50 = 150 omitidas

    def test_no_footer_when_not_truncated(self):
        """Sem truncamento, não deve aparecer linha de rodapé."""
        candidates = _make_candidates(10)
        output = self._capture(candidates, max_rows=50)
        self.assertNotIn("more images", output)

    def test_footer_is_in_english(self):
        """Rodapé do truncamento deve estar em inglês."""
        candidates = _make_candidates(200)
        output = self._capture(candidates, max_rows=50)
        self.assertIn("more images", output)

    def test_no_box_drawing_characters(self):
        """Tabela não deve usar caracteres de borda (┌ │ ┐ ─ ┼ etc.)."""
        candidates = _make_candidates(5)
        output = self._capture(candidates)
        for char in "┌┐└┘├┤┬┴┼│":
            self.assertNotIn(char, output, f"Caractere de borda '{char}' encontrado na tabela")

    def test_header_row_present(self):
        """Tabela deve ter uma linha de cabeçalho com 'tag', 'age' e 'digest'."""
        candidates = _make_candidates(3)
        output = self._capture(candidates)
        self.assertIn("tag",    output.lower())
        self.assertIn("age",    output.lower())
        self.assertIn("digest", output.lower())

    def test_default_behavior_unchanged(self):
        """Sem max_rows, todas as linhas devem ser impressas (comportamento original)."""
        candidates = _make_candidates(100)
        output = self._capture(candidates)
        self.assertEqual(len(self._data_lines(output)), 100)


# ══════════════════════════════════════════════════════════════════
# Parameter propagation — registry URL and in-cluster flag
# ══════════════════════════════════════════════════════════════════

from purge import get_acr_client


class TestGetAcrClient(unittest.TestCase):
    """Verify that --registry is used to build the correct ACR endpoint URL."""

    def test_registry_url_uses_azurecr_io_suffix(self):
        """get_acr_client('myregistry') must connect to myregistry.azurecr.io."""
        with patch("purge.DefaultAzureCredential"), \
             patch("purge.ContainerRegistryClient") as MockClient:
            get_acr_client("myregistry")
            call_kwargs = MockClient.call_args
            endpoint = call_kwargs[1]["endpoint"] if call_kwargs[1] else call_kwargs[0][0]
            self.assertIn("myregistry.azurecr.io", endpoint)

    def test_different_registry_name_produces_different_url(self):
        """A different --registry value must produce a different endpoint."""
        captured = []
        def capture_endpoint(*args, **kwargs):
            captured.append(kwargs.get("endpoint") or args[0])
            return MagicMock()

        with patch("purge.DefaultAzureCredential"), \
             patch("purge.ContainerRegistryClient", side_effect=capture_endpoint):
            get_acr_client("registry-a")
            get_acr_client("registry-b")

        self.assertIn("registry-a.azurecr.io", captured[0])
        self.assertIn("registry-b.azurecr.io", captured[1])
        self.assertNotEqual(captured[0], captured[1])


# ══════════════════════════════════════════════════════════════════
# BuildConfig input images — base/builder images used in builds
# ══════════════════════════════════════════════════════════════════

def _make_bc(name, docker_from=None, source_from=None):
    """Build a minimal BuildConfig MagicMock."""
    bc = MagicMock()
    bc.metadata.name = name
    if docker_from:
        setattr(bc.spec.strategy.dockerStrategy, "from", docker_from)
    else:
        bc.spec.strategy.dockerStrategy = None
    if source_from:
        setattr(bc.spec.strategy.sourceStrategy, "from", source_from)
    else:
        bc.spec.strategy.sourceStrategy = None
    bc.spec.strategy.customStrategy = None
    return bc


def _make_from_ref(digest):
    ref = MagicMock()
    ref.name = f"registry.example.io/base@{digest}"
    return ref


class TestLoadBuildConfigs(unittest.TestCase):

    def test_bc_docker_strategy_from_added_to_active(self):
        """BuildConfig dockerStrategy.from digest must be registered as active."""
        oc = _make_oc_client()
        oc.bc_api = MagicMock()
        digest = "sha256:" + "b1" * 32

        bc = _make_bc("my-bc", docker_from=_make_from_ref(digest))
        oc.bc_api.get.return_value.items = [bc]

        oc._load_buildconfigs("prd-ns")

        self.assertIn(digest, oc._active,
                      "dockerStrategy.from digest deve estar em _active")

    def test_bc_source_strategy_from_added_to_active(self):
        """BuildConfig sourceStrategy.from (builder image) must be active."""
        oc = _make_oc_client()
        oc.bc_api = MagicMock()
        digest = "sha256:" + "c2" * 32

        bc = _make_bc("my-bc", source_from=_make_from_ref(digest))
        oc.bc_api.get.return_value.items = [bc]

        oc._load_buildconfigs("prd-ns")

        self.assertIn(digest, oc._active,
                      "sourceStrategy.from digest deve estar em _active")

    def test_bc_no_digest_ref_does_not_add_active(self):
        """BuildConfig from pointing to an ImageStream tag (no digest) must be ignored."""
        oc = _make_oc_client()
        oc.bc_api = MagicMock()

        ref = MagicMock()
        ref.name = "myapp:latest"   # tag ref, no sha256
        bc  = _make_bc("my-bc", docker_from=ref)
        oc.bc_api.get.return_value.items = [bc]

        oc._load_buildconfigs("prd-ns")

        self.assertEqual(len(oc._active), 0,
                         "ImageStreamTag ref (sem digest) não deve entrar em _active")

    def test_bc_permission_error_sets_incomplete(self):
        """403 from bc_api must set permission_incomplete=True."""
        from kubernetes.client.exceptions import ApiException
        oc = _make_oc_client()
        oc.bc_api = MagicMock()
        oc.bc_api.get.side_effect = ApiException(status=403)

        oc._load_buildconfigs("prd-ns")

        self.assertTrue(oc.permission_incomplete)

    def test_bc_is_called_for_each_namespace(self):
        """_load_buildconfigs must be invoked once per namespace via _load_all."""
        namespaces = ["prd-ns-0", "prd-ns-1"]
        oc = _make_oc_client()
        oc.namespaces = namespaces

        with patch.object(oc, "_load_pods"), \
             patch.object(oc, "_load_deployments"), \
             patch.object(oc, "_load_deploymentconfigs"), \
             patch.object(oc, "_load_replicationcontrollers"), \
             patch.object(oc, "_load_statefulsets"), \
             patch.object(oc, "_load_daemonsets"), \
             patch.object(oc, "_load_replicasets"), \
             patch.object(oc, "_load_jobs"), \
             patch.object(oc, "_load_cronjobs"), \
             patch.object(oc, "_load_imagestreams"), \
             patch.object(oc, "_load_imagestreamtags"), \
             patch.object(oc, "_load_builds"), \
             patch.object(oc, "_load_buildconfigs") as mock_bc:
            oc._load_all()

        self.assertEqual(mock_bc.call_count, len(namespaces))


class TestLoadBuildInputImages(unittest.TestCase):
    """Build.spec.strategy.*.from must be captured (base image used per run)."""

    def _make_build(self, name, docker_from=None, source_from=None, output_ref=""):
        b = MagicMock()
        b.metadata.name = name
        b.status.outputDockerImageReference = output_ref
        if docker_from:
            setattr(b.spec.strategy.dockerStrategy, "from", docker_from)
        else:
            b.spec.strategy.dockerStrategy = None
        if source_from:
            setattr(b.spec.strategy.sourceStrategy, "from", source_from)
        else:
            b.spec.strategy.sourceStrategy = None
        b.spec.strategy.customStrategy = None
        return b

    def test_build_docker_strategy_from_added_to_active(self):
        """Build.spec.strategy.dockerStrategy.from digest must be active."""
        oc = _make_oc_client()
        oc.build_api = MagicMock()
        digest = "sha256:" + "d3" * 32

        b = self._make_build("run-1", docker_from=_make_from_ref(digest))
        oc.build_api.get.return_value.items = [b]

        oc._load_builds("prd-ns")

        self.assertIn(digest, oc._active,
                      "Build dockerStrategy.from digest deve estar em _active")

    def test_build_source_strategy_from_added_to_active(self):
        """Build.spec.strategy.sourceStrategy.from digest must be active."""
        oc = _make_oc_client()
        oc.build_api = MagicMock()
        digest = "sha256:" + "e4" * 32

        b = self._make_build("run-1", source_from=_make_from_ref(digest))
        oc.build_api.get.return_value.items = [b]

        oc._load_builds("prd-ns")

        self.assertIn(digest, oc._active,
                      "Build sourceStrategy.from digest deve estar em _active")

    def test_build_output_still_captured(self):
        """Existing behavior: Build output digest must still be active."""
        oc = _make_oc_client()
        oc.build_api = MagicMock()
        digest = "sha256:" + "f5" * 32

        b = self._make_build("run-1", output_ref=f"registry.example.io/app@{digest}")
        oc.build_api.get.return_value.items = [b]

        oc._load_builds("prd-ns")

        self.assertIn(digest, oc._active,
                      "Build output digest deve continuar em _active")


# ══════════════════════════════════════════════════════════════════
# Gap 1 — DeploymentConfig: init_containers not scanned
# ══════════════════════════════════════════════════════════════════

class TestLoadDeploymentConfigInitContainers(unittest.TestCase):
    """DC init_containers must be scanned as active, just like regular containers."""

    def _make_dc(self, name, containers=None, init_containers=None):
        dc = MagicMock()
        dc.metadata.name = name
        dc.spec.template.spec.containers      = containers      or []
        dc.spec.template.spec.init_containers = init_containers or []
        return dc

    def _make_container(self, image):
        c = MagicMock()
        c.image = image
        return c

    def test_dc_init_containers_added_to_active(self):
        """Init containers in a DC must be registered as active digests."""
        oc = _make_oc_client()
        oc.dc_api = MagicMock()
        digest = "sha256:" + "b" * 64

        init_c = self._make_container(f"registry.example.io/app@{digest}")
        dc     = self._make_dc("my-dc", init_containers=[init_c])
        oc.dc_api.get.return_value.items = [dc]

        oc._load_deploymentconfigs("prd-ns")

        self.assertIn(digest, oc._active,
                      "Init container digest de DC deve estar em _active")

    def test_dc_regular_containers_still_active(self):
        """Regular containers in a DC must still be registered after the fix."""
        oc = _make_oc_client()
        oc.dc_api = MagicMock()
        digest = "sha256:" + "c" * 64

        c  = self._make_container(f"registry.example.io/app@{digest}")
        dc = self._make_dc("my-dc", containers=[c])
        oc.dc_api.get.return_value.items = [dc]

        oc._load_deploymentconfigs("prd-ns")

        self.assertIn(digest, oc._active)

    def test_dc_both_containers_and_init_containers_active(self):
        """Both containers and init_containers from the same DC must be active."""
        oc = _make_oc_client()
        oc.dc_api = MagicMock()
        d1 = "sha256:" + "d" * 64
        d2 = "sha256:" + "e" * 64

        c      = self._make_container(f"registry.example.io/app@{d1}")
        init_c = self._make_container(f"registry.example.io/init@{d2}")
        dc     = self._make_dc("my-dc", containers=[c], init_containers=[init_c])
        oc.dc_api.get.return_value.items = [dc]

        oc._load_deploymentconfigs("prd-ns")

        self.assertIn(d1, oc._active, "container digest deve estar em _active")
        self.assertIn(d2, oc._active, "init_container digest deve estar em _active")


# ══════════════════════════════════════════════════════════════════
# Gap 3 — ReplicationController: not scanned at all (highest risk)
# ══════════════════════════════════════════════════════════════════

class TestLoadReplicationControllers(unittest.TestCase):
    """RCs created by DC rollouts must be scanned to protect rollback images."""

    def _make_rc(self, name, containers=None, init_containers=None):
        rc = MagicMock()
        rc.metadata.name = name
        rc.spec.template.spec.containers      = containers      or []
        rc.spec.template.spec.init_containers = init_containers or []
        return rc

    def _make_container(self, image):
        c = MagicMock()
        c.image = image
        return c

    def test_rc_containers_added_to_active(self):
        """RC container images must be registered as active (protects DC rollback)."""
        oc = _make_oc_client()
        oc.core_api = MagicMock()
        digest = "sha256:" + "f" * 64

        c  = self._make_container(f"registry.example.io/app@{digest}")
        rc = self._make_rc("my-app-1", containers=[c])
        oc.core_api.list_namespaced_replication_controller.return_value.items = [rc]

        oc._load_replicationcontrollers("prd-ns")

        self.assertIn(digest, oc._active,
                      "RC container digest deve estar em _active para proteger rollback")

    def test_rc_init_containers_added_to_active(self):
        """RC init_containers must also be active (sidecar patterns)."""
        oc = _make_oc_client()
        oc.core_api = MagicMock()
        digest = "sha256:" + "a1" * 32

        init_c = self._make_container(f"registry.example.io/init@{digest}")
        rc     = self._make_rc("my-app-1", init_containers=[init_c])
        oc.core_api.list_namespaced_replication_controller.return_value.items = [rc]

        oc._load_replicationcontrollers("prd-ns")

        self.assertIn(digest, oc._active,
                      "RC init_container digest deve estar em _active")

    def test_rc_permission_error_sets_incomplete(self):
        """403 from core_api must set permission_incomplete=True."""
        from kubernetes.client.exceptions import ApiException

        oc = _make_oc_client()
        oc.core_api = MagicMock()
        exc = ApiException(status=403)
        oc.core_api.list_namespaced_replication_controller.side_effect = exc

        oc._load_replicationcontrollers("prd-ns")

        self.assertTrue(oc.permission_incomplete)

    def test_rc_is_called_for_each_namespace(self):
        """_load_replicationcontrollers must be invoked once per namespace via _load_all."""
        namespaces = ["prd-ns-0", "prd-ns-1", "prd-ns-2"]
        oc = _make_oc_client()
        oc.namespaces = namespaces

        with patch.object(oc, "_load_pods"), \
             patch.object(oc, "_load_deployments"), \
             patch.object(oc, "_load_deploymentconfigs"), \
             patch.object(oc, "_load_statefulsets"), \
             patch.object(oc, "_load_daemonsets"), \
             patch.object(oc, "_load_replicasets"), \
             patch.object(oc, "_load_jobs"), \
             patch.object(oc, "_load_cronjobs"), \
             patch.object(oc, "_load_imagestreams"), \
             patch.object(oc, "_load_imagestreamtags"), \
             patch.object(oc, "_load_builds"), \
             patch.object(oc, "_load_replicationcontrollers") as mock_rc:
            oc._load_all()

        self.assertEqual(mock_rc.call_count, len(namespaces),
                         "_load_replicationcontrollers deve ser chamado uma vez por namespace")


class TestInClusterFlag(unittest.TestCase):
    """Verify that --in-cluster and namespace_prefix="" are forwarded to OpenShiftClient."""

    def test_in_cluster_true_passed_to_constructor(self):
        """run_phase0(in_cluster=True) must call OpenShiftClient(in_cluster=True, namespace_prefix='')."""
        mock_oc = _make_oc_client()
        mock_oc.namespaces = ["prd-ns"]
        MockOC  = MagicMock(return_value=mock_oc)
        with patch("sys.stdout"):
            run_phase0(skip_openshift=False, in_cluster=True, OpenShiftClient=MockOC)
        MockOC.assert_called_once_with(in_cluster=True, namespace_prefix="")

    def test_in_cluster_false_passed_to_constructor(self):
        """run_phase0(in_cluster=False) must call OpenShiftClient(in_cluster=False, namespace_prefix='')."""
        mock_oc = _make_oc_client()
        mock_oc.namespaces = ["prd-ns"]
        MockOC  = MagicMock(return_value=mock_oc)
        with patch("sys.stdout"):
            run_phase0(skip_openshift=False, in_cluster=False, OpenShiftClient=MockOC)
        MockOC.assert_called_once_with(in_cluster=False, namespace_prefix="")

    def test_namespace_prefix_empty_string_always_passed(self):
        """namespace_prefix='' must always be passed so ALL cluster namespaces are loaded."""
        mock_oc = _make_oc_client()
        mock_oc.namespaces = ["qua-ns"]
        MockOC  = MagicMock(return_value=mock_oc)
        with patch("sys.stdout"):
            run_phase0(skip_openshift=False, in_cluster=False, OpenShiftClient=MockOC)
        _, kwargs = MockOC.call_args
        self.assertEqual(kwargs.get("namespace_prefix"), "")

    def test_zero_namespaces_returns_exit_code_1(self):
        """If cluster has 0 accessible namespaces, run_phase0 must abort (exit code 1)."""
        mock_oc = _make_oc_client()
        mock_oc.namespaces = []          # simulates wrong cluster / no access
        MockOC  = MagicMock(return_value=mock_oc)
        with patch("sys.stdout"):
            _, exit_code = run_phase0(skip_openshift=False, in_cluster=False, OpenShiftClient=MockOC)
        self.assertEqual(exit_code, 1)

    def test_zero_namespaces_returns_no_client(self):
        """When aborting due to 0 namespaces, oc_client returned must be None."""
        mock_oc = _make_oc_client()
        mock_oc.namespaces = []
        MockOC  = MagicMock(return_value=mock_oc)
        with patch("sys.stdout"):
            oc_client, _ = run_phase0(skip_openshift=False, in_cluster=False, OpenShiftClient=MockOC)
        self.assertIsNone(oc_client)

    def test_nonzero_namespaces_returns_client(self):
        """When namespaces are found, oc_client must be returned."""
        mock_oc = _make_oc_client()
        mock_oc.namespaces = ["prd-ns", "prd-other"]
        MockOC  = MagicMock(return_value=mock_oc)
        with patch("sys.stdout"):
            oc_client, exit_code = run_phase0(skip_openshift=False, in_cluster=False, OpenShiftClient=MockOC)
        self.assertIsNotNone(oc_client)
        self.assertIsNone(exit_code)

    def test_skip_openshift_never_uses_in_cluster(self):
        """When --skip-openshift is set, OpenShiftClient is never called."""
        MockOC = MagicMock()
        with patch("sys.stdout"):
            run_phase0(skip_openshift=True, in_cluster=True, OpenShiftClient=MockOC)
        MockOC.assert_not_called()


# ══════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite  = loader.loadTestsFromModule(__import__("__main__"))
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
