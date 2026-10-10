"""
mini_kube.py — a tiny educational Kubernetes-style orchestrator.

Requires Python 3.10+.
Run: python mini_kube.py

This simulates pods; it does not create OS processes or containers.
"""

from __future__ import annotations

import asyncio
import random
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional


class PodPhase(str, Enum):
    PENDING = "Pending"
    RUNNING = "Running"
    FAILED = "Failed"
    TERMINATED = "Terminated"


@dataclass
class Node:
    name: str
    capacity: int
    pod_ids: List[str] = field(default_factory=list)

    @property
    def available_slots(self) -> int:
        return self.capacity - len(self.pod_ids)


@dataclass
class Pod:
    id: str
    app: str
    node_name: str
    phase: PodPhase = PodPhase.PENDING
    created_at: float = field(default_factory=time.time)


@dataclass
class Deployment:
    name: str
    image: str
    replicas: int
    pod_ids: List[str] = field(default_factory=list)


@dataclass
class Service:
    name: str
    app: str  # Selects pods by their app label.
    port: int


class MiniKube:
    def __init__(self, nodes: List[Node]) -> None:
        self.nodes: Dict[str, Node] = {node.name: node for node in nodes}
        self.pods: Dict[str, Pod] = {}
        self.deployments: Dict[str, Deployment] = {}
        self.services: Dict[str, Service] = {}
        self._lock = asyncio.Lock()

    async def apply_deployment(
        self, name: str, image: str, replicas: int
    ) -> None:
        if replicas < 0:
            raise ValueError("replicas must be >= 0")

        async with self._lock:
            deployment = self.deployments.get(name)
            if deployment is None:
                self.deployments[name] = Deployment(name, image, replicas)
            else:
                deployment.image = image
                deployment.replicas = replicas
        print(f"Applied deployment/{name}: image={image}, replicas={replicas}")

    async def apply_service(self, name: str, app: str, port: int) -> None:
        if not 1 <= port <= 65535:
            raise ValueError("port must be between 1 and 65535")
        self.services[name] = Service(name=name, app=app, port=port)
        print(f"Applied service/{name}: selects app={app}, port={port}")

    def _schedule(self, pod_id: str) -> Optional[Node]:
        # Simplified scheduler: choose the node with the most free capacity.
        candidates = [node for node in self.nodes.values() if node.available_slots > 0]
        if not candidates:
            return None
        return max(candidates, key=lambda node: node.available_slots)

    async def _create_pod(self, deployment: Deployment) -> bool:
        pod_id = str(uuid.uuid4())[:8]
        node = self._schedule(pod_id)
        if node is None:
            print(f"  Scheduler: no capacity for pod {pod_id}")
            return False

        pod = Pod(id=pod_id, app=deployment.name, node_name=node.name)
        self.pods[pod_id] = pod
        node.pod_ids.append(pod_id)
        deployment.pod_ids.append(pod_id)
        print(f"  Created pod/{pod_id} on {node.name} (Pending)")

        # Simulate a container starting.
        await asyncio.sleep(0.2)
        pod.phase = PodPhase.RUNNING
        print(f"  Started pod/{pod_id} (Running)")
        return True

    async def reconcile_once(self) -> None:
        """Move actual state toward each Deployment's desired replica count."""
        async with self._lock:
            deployments = list(self.deployments.values())

        for deployment in deployments:
            # Ignore deleted/failed pods when counting healthy replicas.
            active_pods_ids = [
                pod_id
                for pod_id in deployment.pod_ids
                if pod_id in self.pods
                and self.pods[pod_id].phase == PodPhase.RUNNING
            ]

            # Remove excess pods first.
            while len(active_pods_ids) > deployment.replicas:
                pod_id = active_pods_ids.pop()
                await self.delete_pod(pod_id)

            # Create missing replicas. If capacity is exhausted, try again next loop.
            while len(active_pods_ids) < deployment.replicas:
                created = await self._create_pod(deployment)
                if not created:
                    break
                active_pods_ids.append(deployment.pod_ids[-1])

    async def run_controller(self, interval: float = 1.0) -> None:
        """Kubernetes-like control loop: observe, compare, reconcile, repeat."""
        while True:
            await self.reconcile_once()
            await asyncio.sleep(interval)

    async def delete_pod(self, pod_id: str) -> None:
        async with self._lock:
            pod = self.pods.get(pod_id)
            if pod is None:
                print(f"Pod {pod_id} not found")
                return

            pod.phase = PodPhase.TERMINATED
            node = self.nodes[pod.node_name]
            if pod_id in node.pod_ids:
                node.pod_ids.remove(pod_id)

            for deployment in self.deployments.values():
                if pod_id in deployment.pod_ids:
                    deployment.pod_ids.remove(pod_id)

            del self.pods[pod_id]
        print(f"Deleted pod/{pod_id}; controller will replace it if needed")

    async def fail_random_pod(self, app: str) -> None:
        candidates = [
            pod for pod in self.pods.values()
            if pod.app == app and pod.phase == PodPhase.RUNNING
        ]
        if not candidates:
            print(f"No running pods for app={app}")
            return
        victim = random.choice(candidates)
        print(f"Simulating failure of pod/{victim.id}")
        await self.delete_pod(victim.id)

    def service_request(self, service_name: str) -> str:
        """Route a simulated request to one healthy pod selected by a Service."""
        service = self.services.get(service_name)
        if service is None:
            return f"Service {service_name!r} not found"

        endpoints = [
            pod for pod in self.pods.values()
            if pod.app == service.app and pod.phase == PodPhase.RUNNING
        ]
        if not endpoints:
            return f"503 Service Unavailable: no healthy endpoints for {service_name}"

        pod = random.choice(endpoints)
        return (
            f"200 OK via service/{service.name}:{service.port} "
            f"-> pod/{pod.id} on {pod.node_name}"
        )

    def status(self) -> None:
        print("\n=== Cluster status ===")
        for node in self.nodes.values():
            print(f"Node {node.name}: {len(node.pod_ids)}/{node.capacity} slots used")
        for deployment in self.deployments.values():
            pods = [
                self.pods[pod_id]
                for pod_id in deployment.pod_ids
                if pod_id in self.pods
            ]
            running = sum(pod.phase == PodPhase.RUNNING for pod in pods)
            print(
                f"Deployment {deployment.name}: "
                f"{running}/{deployment.replicas} running replicas"
            )
            for pod in pods:
                print(f"  pod/{pod.id}: {pod.phase.value} on {pod.node_name}")
        print("======================\n")


async def main() -> None:
    cluster = MiniKube(
        nodes=[
            Node(name="worker-1", capacity=3),
            Node(name="worker-2", capacity=3),
        ]
    )

    # Start the controller in the background.
    controller = asyncio.create_task(cluster.run_controller(interval=0.5))

    try:
        await cluster.apply_deployment(
            name="web", image="nginx:alpine", replicas=2
        )
        await cluster.apply_service(name="web-svc", app="web", port=80)
        await asyncio.sleep(0.8)
        cluster.status()

        print("Sending requests through the Service:")
        for _ in range(3):
            print(" ", cluster.service_request("web-svc"))

        print("\nScaling up to 4 replicas...")
        await cluster.apply_deployment("web", "nginx:alpine", replicas=4)
        await asyncio.sleep(1.0)
        cluster.status()

        print("Deleting a pod to demonstrate self-healing...")
        await cluster.fail_random_pod("web")
        await asyncio.sleep(1.0)
        cluster.status()

        print("Scaling down to 1 replica...")
        await cluster.apply_deployment("web", "nginx:alpine", replicas=1)
        await asyncio.sleep(1.0)
        cluster.status()

    finally:
        controller.cancel()
        try:
            await controller
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    asyncio.run(main())
