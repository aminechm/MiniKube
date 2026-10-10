import asyncio
from unittest.mock import patch

import pytest

from src.mini_kube import Deployment, MiniKube, Node, Pod, PodPhase, Service


class TestNode:
    def setup_method(self):
        self.node = Node(name="worker-1", capacity=3)

    def teardown_method(self):
        self.node = None

    def test_available_slots_on_empty_node(self):
        assert self.node.available_slots == 3

    def test_available_slots_after_adding_pods(self):
        self.node.pod_ids.extend(["pod-1", "pod-2"])

        assert self.node.available_slots == 1

    def test_available_slots_when_node_is_full(self):
        self.node.pod_ids.extend(["pod-1", "pod-2", "pod-3"])

        assert self.node.available_slots == 0


class TestApplyDeployment:
    def setup_method(self):
        self.cluster = MiniKube(nodes=[Node(name="worker-1", capacity=3)])

    def teardown_method(self):
        self.cluster = None

    @pytest.mark.asyncio
    async def test_create_deployment(self):
        await self.cluster.apply_deployment(
            name="web", image="nginx:alpine", replicas=2
        )

        assert self.cluster.deployments["web"] == Deployment(
            name="web", image="nginx:alpine", replicas=2
        )

    @pytest.mark.asyncio
    async def test_update_existing_deployment(self):
        await self.cluster.apply_deployment("web", "nginx:1.0", 2)

        await self.cluster.apply_deployment("web", "nginx:2.0", 4)

        assert self.cluster.deployments["web"] == Deployment(
            name="web", image="nginx:2.0", replicas=4
        )

    @pytest.mark.asyncio
    async def test_zero_replicas_is_allowed(self):
        await self.cluster.apply_deployment("web", "nginx:alpine", 0)

        assert self.cluster.deployments["web"].replicas == 0

    @pytest.mark.asyncio
    async def test_negative_replicas_raise_value_error(self):
        with pytest.raises(ValueError, match="replicas must be >= 0"):
            await self.cluster.apply_deployment("web", "nginx:alpine", -1)

    @pytest.mark.asyncio
    async def test_invalid_deployment_does_not_create_deployment(self):
        with pytest.raises(ValueError):
            await self.cluster.apply_deployment("web", "nginx:alpine", -1)

        assert "web" not in self.cluster.deployments


class TestApplyService:
    def setup_method(self):
        self.cluster = MiniKube(nodes=[Node(name="worker-1", capacity=3)])

    def teardown_method(self):
        self.cluster = None

    @pytest.mark.asyncio
    async def test_create_service(self):
        await self.cluster.apply_service("web-svc", "web", 80)

        assert self.cluster.services["web-svc"] == Service(
            name="web-svc", app="web", port=80
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("port", [1, 65535])
    async def test_valid_port_boundaries_are_accepted(self, port):
        await self.cluster.apply_service("web-svc", "web", port)

        assert self.cluster.services["web-svc"].port == port

    @pytest.mark.asyncio
    @pytest.mark.parametrize("port", [0, -1, 65536])
    async def test_invalid_ports_raise_value_error(self, port):
        with pytest.raises(ValueError, match="port must be between 1 and 65535"):
            await self.cluster.apply_service("web-svc", "web", port)

    @pytest.mark.asyncio
    async def test_update_existing_service(self):
        await self.cluster.apply_service("web-svc", "web", 80)

        await self.cluster.apply_service("web-svc", "api", 8080)

        assert self.cluster.services["web-svc"] == Service(
            name="web-svc", app="api", port=8080
        )


class TestScheduler:
    def setup_method(self):
        self.cluster = MiniKube(
            nodes=[
                Node(name="worker-1", capacity=3),
                Node(name="worker-2", capacity=2),
            ]
        )

    def teardown_method(self):
        self.cluster = None

    def test_schedule_selects_node_with_most_free_slots(self):
        selected = self.cluster._schedule("pod-1")

        assert selected is self.cluster.nodes["worker-1"]

    def test_schedule_uses_remaining_capacity(self):
        self.cluster.nodes["worker-1"].pod_ids.extend(["pod-1", "pod-2", "pod-3"])

        selected = self.cluster._schedule("pod-4")

        assert selected is self.cluster.nodes["worker-2"]

    def test_schedule_returns_none_when_all_nodes_are_full(self):
        self.cluster.nodes["worker-1"].pod_ids.extend(["pod-1", "pod-2", "pod-3"])
        self.cluster.nodes["worker-2"].pod_ids.extend(["pod-4", "pod-5"])

        selected = self.cluster._schedule("pod-6")

        assert selected is None

    def test_schedule_returns_none_when_cluster_has_no_nodes(self):
        self.cluster = MiniKube(nodes=[])

        selected = self.cluster._schedule("pod-1")

        assert selected is None


class TestCreatePod:
    def setup_method(self):
        self.cluster = MiniKube(nodes=[Node(name="worker-1", capacity=2)])
        self.deployment = Deployment(name="web", image="nginx:alpine", replicas=1)

    def teardown_method(self):
        self.cluster = None
        self.deployment = None

    @pytest.mark.asyncio
    async def test_create_pod_returns_true_when_capacity_exists(self, monkeypatch):
        async def no_sleep(_):
            return None

        monkeypatch.setattr(asyncio, "sleep", no_sleep)

        created = await self.cluster._create_pod(self.deployment)

        assert created is True

    @pytest.mark.asyncio
    async def test_created_pod_is_running_after_startup(self, monkeypatch):
        async def no_sleep(_):
            return None

        monkeypatch.setattr(asyncio, "sleep", no_sleep)

        await self.cluster._create_pod(self.deployment)

        pod_id = self.deployment.pod_ids[0]
        assert self.cluster.pods[pod_id].phase == PodPhase.RUNNING

    @pytest.mark.asyncio
    async def test_created_pod_has_correct_app_label(self, monkeypatch):
        async def no_sleep(_):
            return None

        monkeypatch.setattr(asyncio, "sleep", no_sleep)

        await self.cluster._create_pod(self.deployment)

        pod_id = self.deployment.pod_ids[0]
        assert self.cluster.pods[pod_id].app == "web"

    @pytest.mark.asyncio
    async def test_created_pod_is_registered_on_node(self, monkeypatch):
        async def no_sleep(_):
            return None

        monkeypatch.setattr(asyncio, "sleep", no_sleep)

        await self.cluster._create_pod(self.deployment)

        pod_id = self.deployment.pod_ids[0]
        assert pod_id in self.cluster.nodes["worker-1"].pod_ids

    @pytest.mark.asyncio
    async def test_creation_fails_when_cluster_has_no_capacity(self):
        self.cluster.nodes["worker-1"].pod_ids.extend(["occupied-1", "occupied-2"])

        created = await self.cluster._create_pod(self.deployment)

        assert created is False


class TestReconcile:
    def setup_method(self):
        self.cluster = MiniKube(
            nodes=[
                Node(name="worker-1", capacity=5),
                Node(name="worker-2", capacity=5),
            ]
        )

    def teardown_method(self):
        self.cluster = None

    @pytest.mark.asyncio
    async def test_reconcile_creates_desired_replicas(self, monkeypatch):
        async def no_sleep(_):
            return None

        monkeypatch.setattr(asyncio, "sleep", no_sleep)

        await self.cluster.apply_deployment("web", "nginx", 3)
        await self.cluster.reconcile_once()

        assert len(self.cluster.deployments["web"].pod_ids) == 3

    @pytest.mark.asyncio
    async def test_reconcile_does_not_create_pods_for_zero_replicas(self):
        await self.cluster.apply_deployment("web", "nginx", 0)

        await self.cluster.reconcile_once()

        assert self.cluster.pods == {}

    @pytest.mark.asyncio
    async def test_reconcile_scales_down_excess_replicas(self, monkeypatch):
        async def no_sleep(_):
            return None

        monkeypatch.setattr(asyncio, "sleep", no_sleep)

        await self.cluster.apply_deployment("web", "nginx", 3)
        await self.cluster.reconcile_once()
        await self.cluster.apply_deployment("web", "nginx", 1)

        await self.cluster.reconcile_once()

        assert len(self.cluster.deployments["web"].pod_ids) == 1

    @pytest.mark.asyncio
    async def test_reconcile_respects_cluster_capacity(self, monkeypatch):
        self.cluster = MiniKube(nodes=[Node(name="worker-1", capacity=2)])

        async def no_sleep(_):
            return None

        monkeypatch.setattr(asyncio, "sleep", no_sleep)

        await self.cluster.apply_deployment("web", "nginx", 4)
        await self.cluster.reconcile_once()

        assert len(self.cluster.deployments["web"].pod_ids) == 2

    @pytest.mark.asyncio
    async def test_reconcile_recreates_deleted_replica(self, monkeypatch):
        async def no_sleep(_):
            return None

        monkeypatch.setattr(asyncio, "sleep", no_sleep)

        await self.cluster.apply_deployment("web", "nginx", 1)
        await self.cluster.reconcile_once()

        old_pod_id = self.cluster.deployments["web"].pod_ids[0]
        await self.cluster.delete_pod(old_pod_id)
        await self.cluster.reconcile_once()

        assert len(self.cluster.deployments["web"].pod_ids) == 1


class TestDeletePod:
    def setup_method(self):
        self.cluster = MiniKube(nodes=[Node(name="worker-1", capacity=2)])
        self.pod = Pod(
            id="pod-1",
            app="web",
            node_name="worker-1",
            phase=PodPhase.RUNNING,
        )
        self.cluster.pods[self.pod.id] = self.pod
        self.cluster.nodes["worker-1"].pod_ids.append(self.pod.id)
        self.cluster.deployments["web"] = Deployment(
            name="web",
            image="nginx",
            replicas=1,
            pod_ids=[self.pod.id],
        )

    def teardown_method(self):
        self.cluster = None
        self.pod = None

    @pytest.mark.asyncio
    async def test_delete_pod_removes_it_from_cluster(self):
        await self.cluster.delete_pod("pod-1")

        assert "pod-1" not in self.cluster.pods

    @pytest.mark.asyncio
    async def test_delete_pod_releases_node_capacity(self):
        await self.cluster.delete_pod("pod-1")

        assert self.cluster.nodes["worker-1"].available_slots == 2

    @pytest.mark.asyncio
    async def test_delete_pod_removes_deployment_reference(self):
        await self.cluster.delete_pod("pod-1")

        assert self.cluster.deployments["web"].pod_ids == []

    @pytest.mark.asyncio
    async def test_delete_missing_pod_does_not_raise(self):
        await self.cluster.delete_pod("missing-pod")

        assert "pod-1" in self.cluster.pods


class TestFailRandomPod:
    def setup_method(self):
        self.cluster = MiniKube(nodes=[Node(name="worker-1", capacity=3)])
        self.pods = [
            Pod("pod-1", "web", "worker-1", PodPhase.RUNNING),
            Pod("pod-2", "web", "worker-1", PodPhase.RUNNING),
            Pod("pod-3", "api", "worker-1", PodPhase.RUNNING),
        ]

        self.cluster.pods = {pod.id: pod for pod in self.pods}
        self.cluster.nodes["worker-1"].pod_ids = [pod.id for pod in self.pods]
        self.cluster.deployments["web"] = Deployment(
            "web", "nginx", 2, ["pod-1", "pod-2"]
        )
        self.cluster.deployments["api"] = Deployment("api", "python", 1, ["pod-3"])

    def teardown_method(self):
        self.cluster = None
        self.pods = []

    @pytest.mark.asyncio
    async def test_failure_removes_selected_app_pod(self, monkeypatch):
        monkeypatch.setattr(
            "src.mini_kube.random.choice",
            lambda candidates: candidates[0],
        )

        await self.cluster.fail_random_pod("web")

        assert "pod-1" not in self.cluster.pods

    @pytest.mark.asyncio
    async def test_failure_does_not_remove_other_app_pods(self, monkeypatch):
        monkeypatch.setattr(
            "src.mini_kube.random.choice",
            lambda candidates: candidates[0],
        )

        await self.cluster.fail_random_pod("web")

        assert "pod-3" in self.cluster.pods

    @pytest.mark.asyncio
    async def test_failure_does_nothing_when_app_has_no_running_pods(self):
        await self.cluster.fail_random_pod("missing-app")

        assert len(self.cluster.pods) == 3


class TestServiceRequest:
    def setup_method(self):
        self.cluster = MiniKube(nodes=[Node(name="worker-1", capacity=3)])
        self.cluster.services["web-svc"] = Service(name="web-svc", app="web", port=80)
        self.cluster.pods = {
            "pod-1": Pod("pod-1", "web", "worker-1", PodPhase.RUNNING),
            "pod-2": Pod("pod-2", "api", "worker-1", PodPhase.RUNNING),
        }

    def teardown_method(self):
        self.cluster = None

    def test_unknown_service_returns_not_found(self):
        response = self.cluster.service_request("missing")

        assert response == "Service 'missing' not found"

    def test_service_without_healthy_endpoints_returns_503(self):
        self.cluster.pods.clear()

        response = self.cluster.service_request("web-svc")

        assert response == (
            "503 Service Unavailable: no healthy endpoints " "for web-svc"
        )

    def test_service_routes_to_matching_running_pod(self, monkeypatch):
        monkeypatch.setattr(
            "src.mini_kube.random.choice",
            lambda endpoints: endpoints[0],
        )

        response = self.cluster.service_request("web-svc")

        assert response == ("200 OK via service/web-svc:80 " "-> pod/pod-1 on worker-1")

    def test_service_ignores_pods_with_wrong_app_label(self):
        self.cluster.pods = {"pod-2": self.cluster.pods["pod-2"]}

        response = self.cluster.service_request("web-svc")

        assert response.startswith("503 Service Unavailable")

    def test_service_ignores_non_running_matching_pods(self):
        self.cluster.pods["pod-1"].phase = PodPhase.FAILED

        response = self.cluster.service_request("web-svc")

        assert response.startswith("503 Service Unavailable")


class TestStatus:
    def setup_method(self):
        self.cluster = MiniKube(nodes=[Node(name="worker-1", capacity=2)])
        self.cluster.deployments["web"] = Deployment(
            name="web", image="nginx", replicas=1
        )
        self.cluster.pods["pod-1"] = Pod(
            id="pod-1",
            app="web",
            node_name="worker-1",
            phase=PodPhase.RUNNING,
        )
        self.cluster.nodes["worker-1"].pod_ids.append("pod-1")
        self.cluster.deployments["web"].pod_ids.append("pod-1")

    def teardown_method(self):
        self.cluster = None

    def test_status_displays_node_capacity(self, capsys):
        self.cluster.status()

        assert "Node worker-1: 1/2 slots used" in capsys.readouterr().out

    def test_status_displays_running_replica_count(self, capsys):
        self.cluster.status()

        assert "Deployment web: 1/1 running replicas" in capsys.readouterr().out

    def test_status_displays_pod_phase(self, capsys):
        self.cluster.status()

        assert "pod/pod-1: Running on worker-1" in capsys.readouterr().out
