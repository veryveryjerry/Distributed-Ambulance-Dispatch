# Distributed Ambulance Dispatch System

A university project demonstrating distributed computing and DevOps principles through a simulated ambulance dispatch system.

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                      Client / Tester                    │
│              POST /patient  →  Coordinator node         │
└─────────────────────┬───────────────────────────────────┘
                      │ HTTP
          ┌───────────▼───────────┐
          │   Ambulance Node 3    │  ← Coordinator (highest ID wins Bully)
          │   ID=3 | HTTP :8083   │
          └──────┬────────┬───────┘
          gRPC   │        │ gRPC
    ┌────────────▼─┐  ┌───▼────────────┐
    │ Ambulance 1  │  │ Ambulance 2    │
    │ ID=1 :8081   │  │ ID=2 :8082     │
    └──────────────┘  └────────────────┘
                      ↓ HTTP POST /dispatch
              ┌───────────────────┐
              │  Hospital Service │
              │  Flask :5000      │
              └───────────────────┘
```

### Key Components

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Ambulance Node** | Python + gRPC + Flask | Bully election, dispatch handling |
| **Hospital Service** | Python + Flask | Receives dispatch confirmations |
| **Docker Compose** | Docker | 3 ambulance nodes + 1 hospital |
| **Kubernetes** | StatefulSet + Services | Production-grade deployment |
| **Ansible** | Playbook | Infrastructure provisioning |
| **GitHub Actions** | CI/CD workflow | Build & push Docker images |

---

## Distributed Computing — Bully Algorithm

The **Bully Algorithm** is used for leader election among the ambulance nodes.

1. On startup every node broadcasts an `ELECTION` RPC to all peers with a **higher** ID.
2. A higher-ID peer that receives the message replies `OK` and starts its own election.
3. If **no** higher peer replies, the node declares itself **Coordinator** and broadcasts `ANNOUNCE_COORDINATOR` to all peers.
4. The coordinator sends a periodic **Heartbeat** to all followers.
5. If a follower's heartbeat times out it assumes the coordinator has crashed and starts a new election (fault tolerance / self-healing).

In a fresh 3-node cluster, **Node 3** (highest ID) will always win the first election.

---

## Quick Start — Docker Compose

```bash
# Build images and start all services
docker compose up --build

# Wait ~10 seconds for the election to complete, then check who is coordinator
curl http://localhost:8081/status   # node 1
curl http://localhost:8082/status   # node 2
curl http://localhost:8083/status   # node 3  ← should be coordinator

# Send a patient rescue request to the coordinator
curl -X POST http://localhost:8083/patient \
     -H "Content-Type: application/json" \
     -d '{"patient_name": "Alice", "location": "123 Main St"}'

# Node 1 (lowest ID) is dispatched. Confirm the hospital received it:
curl http://localhost:5000/dispatches
```

### Simulating a coordinator crash

```bash
# Stop node 3 (the coordinator) — a new election will fire within ~10 seconds
docker stop ambulance3

# Watch node 2 become the new coordinator
curl http://localhost:8082/status
```

---

## Kubernetes Deployment

```bash
# Apply manifests
kubectl apply -f k8s/service.yaml
kubectl apply -f k8s/deployment.yaml

# Check pods
kubectl get pods -l app=ambulance

# Forward a node's HTTP port to localhost
kubectl port-forward ambulance-2 8080:8080

# Send a patient request
curl -X POST http://localhost:8080/patient \
     -H "Content-Type: application/json" \
     -d '{"patient_name": "Bob", "location": "456 Oak Ave"}'
```

> **StatefulSet vs Deployment**: A `StatefulSet` is used instead of a plain `Deployment` because the Bully Algorithm requires each pod to have a **stable, unique identity** (ordinal index). The pod name `ambulance-N` is parsed at runtime to derive `NODE_ID = N + 1`.

### Liveness Probe (DevOps CO6 — Self-Healing)

The `deployment.yaml` configures a Kubernetes **liveness probe** that polls `GET /health` every 15 seconds.  If a pod stops responding, Kubernetes automatically restarts it, demonstrating self-healing behaviour.

---

## Ansible Provisioning

```bash
# Install Ansible
pip install ansible

# Install the community.docker collection
ansible-galaxy collection install community.docker

# Edit inventory.ini with your actual server IPs, then run:
ansible-playbook -i ansible/inventory.ini ansible/setup.yml
```

The playbook:
* Installs Docker CE and Docker Compose plugin.
* Enables the Docker system service.
* Adds the SSH user to the `docker` group.
* Creates the `ambulance` Docker bridge network.

---

## CI/CD — GitHub Actions

The workflow in `.github/workflows/main.yml`:

1. Triggers on every push / PR to `main`.
2. Builds the `ambulance-node` Docker image from `./ambulance_node`.
3. Builds the `hospital-service` Docker image from `./hospital_service`.
4. Pushes both images to Docker Hub (on `main` branch pushes only).

### Required Secrets

Add the following in **Settings → Secrets and variables → Actions**:

| Secret | Value |
|--------|-------|
| `DOCKERHUB_USERNAME` | Your Docker Hub username |
| `DOCKERHUB_TOKEN` | A Docker Hub access token |

---

## Project Structure

```
.
├── ambulance_node/
│   ├── ambulance.proto         # gRPC service & message definitions
│   ├── ambulance_node.py       # Bully election + gRPC servicer + Flask API
│   ├── requirements.txt
│   └── Dockerfile
├── hospital_service/
│   ├── hospital_service.py     # Flask REST API (receives dispatch confirmations)
│   ├── requirements.txt
│   └── Dockerfile
├── k8s/
│   ├── deployment.yaml         # StatefulSet with liveness probe
│   └── service.yaml            # Headless + ClusterIP services
├── ansible/
│   ├── inventory.ini           # Host inventory (replace placeholder IPs)
│   └── setup.yml               # Provisioning playbook
├── .github/
│   └── workflows/
│       └── main.yml            # CI/CD pipeline
└── docker-compose.yml          # Local development stack
```