# Mini Kube

A tiny educational project built to help me learn Kubernetes concepts by implementing a simplified orchestrator in Python.

The project simulates core Kubernetes concepts, including:

- **Nodes** — manage available pod capacity.
- **Pods** — simulate application instances and their lifecycle.
- **Deployments** — manage desired replica counts and scaling.
- **Services** — route requests to healthy pods.
- **Reconciliation** — maintain the desired state and replace failed pods.

This project is purely for learning purposes. It simulates Kubernetes-style behavior without creating actual containers or processes.

## Run

```bash
python src/mini_kube.py
```

## Run tests

```bash
pip install pytest pytest-asyncio
pytest -v
```

The goal is to understand the fundamentals of Kubernetes through hands-on implementation and testing.