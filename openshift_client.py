#!/usr/bin/env python3
"""
OpenShift Client — scans the cluster for image digest references.

Separates references into two levels:
  - active    : digest is currently running or actively referenced
  - historical: digest appears only in ImageStream history (old entries)

This distinction prevents deleting images that are truly in use
while allowing better decisions on images that are only in history.
"""

import threading
import urllib3
from concurrent.futures import ThreadPoolExecutor, as_completed

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from kubernetes import client as k8s_client, config
from kubernetes.client.exceptions import ApiException
from openshift.dynamic import DynamicClient


class OpenShiftClient:

    NAMESPACE_PREFIX = "prd-"

    def __init__(self, in_cluster=False):
        if in_cluster:
            config.load_incluster_config()
        else:
            config.load_kube_config()

        k8s = k8s_client.ApiClient()
        dyn = DynamicClient(k8s)

        # Kubernetes APIs
        self.core_api  = k8s_client.CoreV1Api()
        self.apps_api  = k8s_client.AppsV1Api()
        self.batch_api = k8s_client.BatchV1Api()

        # OpenShift APIs
        self.is_api      = dyn.resources.get(api_version="image.openshift.io/v1", kind="ImageStream")
        self.istag_api   = dyn.resources.get(api_version="image.openshift.io/v1", kind="ImageStreamTag")
        self.dc_api      = dyn.resources.get(api_version="apps.openshift.io/v1", kind="DeploymentConfig")
        self.build_api   = dyn.resources.get(api_version="build.openshift.io/v1", kind="Build")
        self.bc_api      = dyn.resources.get(api_version="build.openshift.io/v1", kind="BuildConfig")
        self.project_api = dyn.resources.get(api_version="project.openshift.io/v1", kind="Project")

        self.permission_incomplete = False
        self._lock = threading.Lock()

        # { digest: "namespace / kind / name" }
        self._active     = {}
        self._historical = {}

        self.namespaces = self._list_valid_namespaces()
        self._load_all()

    # ================================================================
    # NAMESPACE LISTING
    # ================================================================
    def _list_valid_namespaces(self):
        # Usa a API de Projects do OpenShift — retorna apenas os projetos
        # que o usuário tem acesso, sem precisar de permissão cluster-level.
        try:
            items = self.project_api.get().items
            return [
                p.metadata.name for p in items
                if p.metadata.name.startswith(self.NAMESPACE_PREFIX)
            ]
        except Exception as e:
            if self._is_permission_error(e):
                print(f"⚠️  No permission to list projects")
                self.permission_incomplete = True
                return []
            # FIX: any other error (network, timeout, expired token, VPN down)
            # is fatal — raise so purge.py can abort safely with exit(1).
            raise RuntimeError(f"Failed to connect to OpenShift cluster: {e}") from e

    # ================================================================
    # DIGEST HELPERS
    # ================================================================
    @staticmethod
    def _parse_digest(image):
        """Extracts sha256:<hash> from an image reference string."""
        if not image or "sha256:" not in image:
            return None
        try:
            sha = image.split("sha256:")[1].split()[0].strip("\"'@")
            return f"sha256:{sha}"
        except Exception:
            return None

    def _add_active(self, image, location):
        """Register a digest as actively in use. Thread-safe."""
        digest = self._parse_digest(image)
        if digest:
            with self._lock:
                if digest not in self._active:
                    self._active[digest] = location

    def _add_historical(self, image, location):
        """Register a digest as historical only, unless already active. Thread-safe."""
        digest = self._parse_digest(image)
        if digest:
            with self._lock:
                if digest not in self._active and digest not in self._historical:
                    self._historical[digest] = location

    def _is_permission_error(self, exc):
        status = getattr(exc, "status", None)
        return status in (401, 403)

    # ================================================================
    # LOAD ALL RESOURCES
    # ================================================================
    def _load_namespace(self, ns):
        """Run all resource loaders for a single namespace."""
        self._load_pods(ns)
        self._load_deployments(ns)
        self._load_deploymentconfigs(ns)
        self._load_replicationcontrollers(ns)
        self._load_statefulsets(ns)
        self._load_daemonsets(ns)
        self._load_replicasets(ns)
        self._load_jobs(ns)
        self._load_cronjobs(ns)
        self._load_imagestreams(ns)
        self._load_imagestreamtags(ns)
        self._load_builds(ns)
        self._load_buildconfigs(ns)

    def _load_all(self):
        if not self.namespaces:
            return

        total = len(self.namespaces)
        done  = 0

        def load_and_report(ns):
            nonlocal done
            self._load_namespace(ns)
            with self._lock:
                done += 1
                print(f"\r  Scanning namespaces [{done}/{total}]{'':<30}", end="", flush=True)

        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(load_and_report, ns) for ns in self.namespaces]
            for f in as_completed(futures):
                f.result()  # propaga exceções

        print(f"\r{'':<80}\r", end="", flush=True)

    # ================================================================
    # PODS
    # Checks status.containerStatuses[].imageID — the actual running digest.
    # This is the most reliable source for currently running images.
    # ================================================================
    def _load_pods(self, ns):
        try:
            pods = self.core_api.list_namespaced_pod(ns).items
        except ApiException as e:
            if self._is_permission_error(e):
                print(f"\n🚫 No permission to list pods in {ns}")
                self.permission_incomplete = True
            return

        for pod in pods:
            location = f"{ns} / pod / {pod.metadata.name}"

            for cs in (pod.status.container_statuses or []):
                self._add_active(cs.image_id, location)

            for cs in (pod.status.init_container_statuses or []):
                self._add_active(cs.image_id, location)

    # ================================================================
    # WORKLOAD HELPERS
    # Checks spec.template.spec.containers[].image.
    # Only catches digests if the workload explicitly references by digest.
    # Most workloads reference by tag — pods cover the running digest.
    # ================================================================
    def _load_workload_spec(self, ns, kind, items):
        for obj in items:
            location   = f"{ns} / {kind} / {obj.metadata.name}"
            containers = (
                list(obj.spec.template.spec.containers       or []) +
                list(obj.spec.template.spec.init_containers  or [])
            )
            for c in containers:
                self._add_active(c.image, location)

    def _load_deployments(self, ns):
        try:
            items = self.apps_api.list_namespaced_deployment(ns).items
        except ApiException as e:
            if self._is_permission_error(e):
                self.permission_incomplete = True
            return
        self._load_workload_spec(ns, "deployment", items)

    def _load_statefulsets(self, ns):
        try:
            items = self.apps_api.list_namespaced_stateful_set(ns).items
        except ApiException as e:
            if self._is_permission_error(e):
                self.permission_incomplete = True
            return
        self._load_workload_spec(ns, "statefulset", items)

    def _load_daemonsets(self, ns):
        try:
            items = self.apps_api.list_namespaced_daemon_set(ns).items
        except ApiException as e:
            if self._is_permission_error(e):
                self.permission_incomplete = True
            return
        self._load_workload_spec(ns, "daemonset", items)

    def _load_replicasets(self, ns):
        try:
            items = self.apps_api.list_namespaced_replica_set(ns).items
        except ApiException as e:
            if self._is_permission_error(e):
                self.permission_incomplete = True
            return
        self._load_workload_spec(ns, "replicaset", items)

    def _load_jobs(self, ns):
        try:
            items = self.batch_api.list_namespaced_job(ns).items
        except ApiException as e:
            if self._is_permission_error(e):
                self.permission_incomplete = True
            return
        self._load_workload_spec(ns, "job", items)

    def _load_cronjobs(self, ns):
        try:
            items = self.batch_api.list_namespaced_cron_job(ns).items
        except ApiException as e:
            if self._is_permission_error(e):
                self.permission_incomplete = True
            return
        for cj in items:
            location   = f"{ns} / cronjob / {cj.metadata.name}"
            tpl        = cj.spec.job_template.spec.template
            containers = (
                list(tpl.spec.containers      or []) +
                list(tpl.spec.init_containers or [])
            )
            for c in containers:
                self._add_active(c.image, location)

    # ================================================================
    # REPLICATION CONTROLLERS — created per DC rollout revision
    # Critical for DC rollback: each rollout leaves an RC behind that
    # references the image used in that revision. Must be active so
    # `oc rollback` images are never pruned from ACR.
    # ================================================================
    def _load_replicationcontrollers(self, ns):
        try:
            items = self.core_api.list_namespaced_replication_controller(ns).items
        except ApiException as e:
            if self._is_permission_error(e):
                self.permission_incomplete = True
            return

        for rc in items:
            location   = f"{ns} / replicationcontroller / {rc.metadata.name}"
            containers = (
                list(rc.spec.template.spec.containers      or []) +
                list(rc.spec.template.spec.init_containers or [])
            )
            for c in containers:
                self._add_active(getattr(c, "image", None), location)

    def _load_deploymentconfigs(self, ns):
        try:
            items = self.dc_api.get(namespace=ns).items
        except Exception as e:
            if self._is_permission_error(e):
                self.permission_incomplete = True
            return
        for dc in items:
            location   = f"{ns} / deploymentconfig / {dc.metadata.name}"
            containers = (
                list(dc.spec.template.spec.containers      or []) +
                list(dc.spec.template.spec.init_containers or [])
            )
            for c in containers:
                self._add_active(getattr(c, "image", None), location)

    # ================================================================
    # IMAGESTREAMS
    # ALL entries go to _historical — including items[0].
    # Rationale: the ImageStream is a registry catalog, not a workload.
    # "Truly active" means a pod or workload is running the image right
    # now. That evidence comes from _load_pods, _load_deploymentconfigs,
    # etc. Marking items[0] as _active caused all historical release tags
    # (e.g. 1.0.0-release-20231214) to appear as "[running]" even when
    # no pod was using them for months.
    # ================================================================
    def _load_imagestreams(self, ns):
        try:
            items = self.is_api.get(namespace=ns).items
        except Exception as e:
            if self._is_permission_error(e):
                self.permission_incomplete = True
            return

        for is_obj in items:
            location = f"{ns} / imagestream / {is_obj.metadata.name}"

            for tag in (getattr(is_obj.status, "tags", None) or []):
                tag_items = getattr(tag, "items", None) or []

                for entry in tag_items:
                    ref = getattr(entry, "dockerImageReference", "")
                    self._add_historical(ref, location)

    # ================================================================
    # IMAGESTREAMTAGS — all entries go to _historical for the same
    # reason: IST existence proves registry registration, not execution.
    # ================================================================
    def _load_imagestreamtags(self, ns):
        try:
            items = self.istag_api.get(namespace=ns).items
        except Exception as e:
            if self._is_permission_error(e):
                self.permission_incomplete = True
            return

        for ist in items:
            location = f"{ns} / imagestreamtag / {ist.metadata.name}"
            ref      = getattr(ist.image, "dockerImageReference", "")
            self._add_historical(ref, location)

    # ================================================================
    # BUILD STRATEGY HELPER
    # Extracts 'from' image references from any build strategy type.
    # 'from' is a Python keyword so getattr is required.
    # ================================================================
    def _add_strategy_inputs(self, strategy, location):
        """Register 'from' digest refs from dockerStrategy / sourceStrategy / customStrategy."""
        for attr in ("dockerStrategy", "sourceStrategy", "customStrategy"):
            st = getattr(strategy, attr, None)
            if st is None:
                continue
            from_obj = getattr(st, "from", None)
            if from_obj:
                self._add_active(getattr(from_obj, "name", None), location)

    # ================================================================
    # BUILDS — output image + input base/builder image per run
    # ================================================================
    def _load_builds(self, ns):
        try:
            items = self.build_api.get(namespace=ns).items
        except Exception as e:
            if self._is_permission_error(e):
                self.permission_incomplete = True
            return

        for b in items:
            location = f"{ns} / build / {b.metadata.name}"
            # Output: the image pushed to the registry
            ref = getattr(b.status, "outputDockerImageReference", "")
            self._add_active(ref, location)
            # Input: the base/builder image used for this build run
            self._add_strategy_inputs(b.spec.strategy, location)

    # ================================================================
    # BUILDCONFIGS — base/builder images referenced as build inputs
    # Protects images used as FROM in Dockerfiles or as S2I builders.
    # ================================================================
    def _load_buildconfigs(self, ns):
        try:
            items = self.bc_api.get(namespace=ns).items
        except Exception as e:
            if self._is_permission_error(e):
                self.permission_incomplete = True
            return

        for bc in items:
            location = f"{ns} / buildconfig / {bc.metadata.name}"
            self._add_strategy_inputs(bc.spec.strategy, location)

    # ================================================================
    # PUBLIC API
    # ================================================================
    def check_digest(self, digest):
        """
        Returns (status, location):
          ("active",     location) — digest is actively in use
          ("historical", location) — digest found only in ImageStream history
          (None, None)             — not found in cluster
        """
        if digest in self._active:
            return "active", self._active[digest]
        if digest in self._historical:
            return "historical", self._historical[digest]
        return None, None
