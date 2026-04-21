"""
Ambulance Node
==============
Each instance of this service represents one ambulance unit in the fleet.

Distributed mechanics
---------------------
* **Bully Algorithm** – on start-up (and when the coordinator is presumed
  dead) a node initiates an election.  It sends ELECTION RPCs to every peer
  with a higher ID.  If no higher peer replies OK, the node declares itself
  coordinator and broadcasts COORDINATOR to all peers.  If a higher peer
  replies OK, the node waits up to COORDINATOR_WAIT_TIMEOUT seconds for an
  ANNOUNCE_COORDINATOR RPC before restarting the election.

* **Heartbeat** – the coordinator sends a Heartbeat RPC to every follower
  every HEARTBEAT_INTERVAL seconds.  Followers that have not received a
  heartbeat for HEARTBEAT_TIMEOUT seconds assume the coordinator has crashed
  and start a new election.

HTTP API (Flask)
----------------
GET  /health   – liveness probe used by Docker / Kubernetes.
GET  /status   – returns JSON with node_id, coordinator_id, is_coordinator.
POST /patient  – (coordinator only) accepts a patient rescue request,
                 dispatches the lowest-ID available ambulance via gRPC, and
                 notifies the Hospital Service via HTTP.

Environment variables
---------------------
NODE_ID          int   Unique ID for this node (1, 2, 3, …).
GRPC_PORT        int   Port this node listens on for inbound gRPC calls (default 50051).
HTTP_PORT        int   Port for the Flask API (default 8080).
PEERS            str   Comma-separated "id=host:port" entries for all OTHER nodes.
                       Example: "2=ambulance2:50051,3=ambulance3:50051"
HOSPITAL_URL     str   Base URL of the Hospital Service REST API.
K8S_MODE         str   Set to "true" to derive NODE_ID and PEERS automatically
                       from the pod hostname (StatefulSet) and K8S_* variables.
K8S_REPLICAS     int   Total number of ambulance replicas (K8S_MODE only).
K8S_SERVICE_NAME str   Name of the headless k8s Service (K8S_MODE only).
K8S_NAMESPACE    str   k8s namespace (K8S_MODE only, default "default").
"""

import logging
import os
import socket
import threading
import time
from concurrent import futures

import grpc
import requests
from flask import Flask, jsonify
from flask import request as flask_request

import ambulance_pb2
import ambulance_pb2_grpc

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
logger = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────

K8S_MODE = os.environ.get("K8S_MODE", "false").lower() == "true"
GRPC_PORT = int(os.environ.get("GRPC_PORT", "50051"))
HTTP_PORT = int(os.environ.get("HTTP_PORT", "8080"))
HOSPITAL_URL = os.environ.get("HOSPITAL_URL", "http://hospital:5000")

# Bully / heartbeat timing (seconds)
HEARTBEAT_INTERVAL = 3
HEARTBEAT_TIMEOUT = 10
COORDINATOR_WAIT_TIMEOUT = 8
ELECTION_RPC_TIMEOUT = 3
DISPATCH_RPC_TIMEOUT = 5

# ── Node identity & peer map ──────────────────────────────────────────────────


def _build_identity_from_env():
    """Derive NODE_ID and peers dict from plain environment variables."""
    node_id = int(os.environ.get("NODE_ID", "1"))
    peers: dict[int, str] = {}
    peers_env = os.environ.get("PEERS", "")
    for entry in peers_env.split(","):
        entry = entry.strip()
        if entry and "=" in entry:
            pid_str, addr = entry.split("=", 1)
            peers[int(pid_str)] = addr
    return node_id, peers


def _build_identity_from_k8s():
    """
    Derive NODE_ID and peers from the StatefulSet pod hostname.
    Pod names follow the pattern  <statefulset-name>-<ordinal>.
    NODE_ID = ordinal + 1  (so pod-0 → ID 1, pod-1 → ID 2, …)
    """
    hostname = socket.gethostname()  # e.g. "ambulance-2"
    ordinal = int(hostname.rsplit("-", 1)[-1])
    node_id = ordinal + 1

    replicas = int(os.environ.get("K8S_REPLICAS", "3"))
    svc = os.environ.get("K8S_SERVICE_NAME", "ambulance-headless")
    ns = os.environ.get("K8S_NAMESPACE", "default")
    statefulset_name = hostname.rsplit("-", 1)[0]  # e.g. "ambulance"

    peers: dict[int, str] = {}
    for i in range(replicas):
        pid = i + 1
        if pid != node_id:
            # DNS name: <pod>.<headless-svc>.<ns>.svc.cluster.local
            dns = f"{statefulset_name}-{i}.{svc}.{ns}.svc.cluster.local:{GRPC_PORT}"
            peers[pid] = dns

    return node_id, peers


if K8S_MODE:
    NODE_ID, peers = _build_identity_from_k8s()
else:
    NODE_ID, peers = _build_identity_from_env()

logger.info("Node %d starting — peers: %s", NODE_ID, peers)

# ── Shared state ──────────────────────────────────────────────────────────────

coordinator_id: int | None = None
coordinator_lock = threading.Lock()

election_in_progress = False
election_lock = threading.Lock()

last_heartbeat = time.time()

# ── gRPC channel cache ────────────────────────────────────────────────────────

_channels: dict[int, grpc.Channel] = {}
_channel_lock = threading.Lock()


def get_stub(peer_id: int):
    """Return a cached gRPC stub for *peer_id*, or None if unknown."""
    addr = peers.get(peer_id)
    if not addr:
        return None
    with _channel_lock:
        if peer_id not in _channels:
            _channels[peer_id] = grpc.insecure_channel(addr)
        return ambulance_pb2_grpc.AmbulanceServiceStub(_channels[peer_id])


# ── Bully Algorithm ───────────────────────────────────────────────────────────


def start_election() -> None:
    """Initiate a Bully election from this node."""
    global election_in_progress

    with election_lock:
        if election_in_progress:
            logger.debug("Node %d: election already in progress, skipping", NODE_ID)
            return
        election_in_progress = True

    logger.info("Node %d: ── ELECTION STARTED ──", NODE_ID)
    higher_peers = {pid: addr for pid, addr in peers.items() if pid > NODE_ID}

    if not higher_peers:
        _declare_coordinator()
        return

    ok_received = False
    for pid in sorted(higher_peers.keys()):
        stub = get_stub(pid)
        if stub is None:
            continue
        try:
            resp = stub.SendElection(
                ambulance_pb2.ElectionMessage(sender_id=NODE_ID),
                timeout=ELECTION_RPC_TIMEOUT,
            )
            if resp.status == "OK":
                ok_received = True
                logger.info("Node %d: received OK from node %d — stepping back", NODE_ID, pid)
                break
        except grpc.RpcError as exc:
            logger.warning("Node %d: node %d unreachable (%s)", NODE_ID, pid, exc.code())

    with election_lock:
        election_in_progress = False

    if not ok_received:
        _declare_coordinator()
        return

    # Wait for a COORDINATOR announcement; restart if it doesn't arrive.
    deadline = time.time() + COORDINATOR_WAIT_TIMEOUT
    while time.time() < deadline:
        with coordinator_lock:
            if coordinator_id is not None:
                logger.info(
                    "Node %d: coordinator %d announced — election done",
                    NODE_ID,
                    coordinator_id,
                )
                return
        time.sleep(0.5)

    logger.warning(
        "Node %d: no COORDINATOR announced within %ds — restarting election",
        NODE_ID,
        COORDINATOR_WAIT_TIMEOUT,
    )
    threading.Thread(target=start_election, daemon=True).start()


def _declare_coordinator() -> None:
    """This node wins the election — announce itself and start heartbeating."""
    global coordinator_id, election_in_progress

    logger.info("Node %d: ══ COORDINATOR ══", NODE_ID)
    with coordinator_lock:
        coordinator_id = NODE_ID
    with election_lock:
        election_in_progress = False

    for pid in peers:
        stub = get_stub(pid)
        if stub is None:
            continue
        try:
            stub.AnnounceCoordinator(
                ambulance_pb2.CoordinatorMessage(coordinator_id=NODE_ID),
                timeout=ELECTION_RPC_TIMEOUT,
            )
        except grpc.RpcError as exc:
            logger.warning(
                "Node %d: could not announce coordinator to node %d (%s)",
                NODE_ID,
                pid,
                exc.code(),
            )

    threading.Thread(target=_send_heartbeats, daemon=True).start()


def _send_heartbeats() -> None:
    """Coordinator periodically heartbeats all follower nodes."""
    logger.info("Node %d: heartbeat thread started", NODE_ID)
    while True:
        with coordinator_lock:
            if coordinator_id != NODE_ID:
                logger.info("Node %d: no longer coordinator — stopping heartbeats", NODE_ID)
                return
        for pid in list(peers.keys()):
            stub = get_stub(pid)
            if stub is None:
                continue
            try:
                stub.Heartbeat(
                    ambulance_pb2.HeartbeatRequest(sender_id=NODE_ID),
                    timeout=2,
                )
            except grpc.RpcError:
                pass  # Follower offline — election will handle this
        time.sleep(HEARTBEAT_INTERVAL)


def _monitor_coordinator() -> None:
    """
    Follower thread: if the coordinator heartbeat is overdue, trigger a new
    election.  Starts after an initial grace period to allow the first
    election to complete.
    """
    global last_heartbeat
    time.sleep(HEARTBEAT_TIMEOUT)  # Grace period at startup
    while True:
        with coordinator_lock:
            is_coord = coordinator_id == NODE_ID
        if not is_coord:
            elapsed = time.time() - last_heartbeat
            if elapsed > HEARTBEAT_TIMEOUT:
                logger.warning(
                    "Node %d: heartbeat overdue (%.1fs) — coordinator presumed dead",
                    NODE_ID,
                    elapsed,
                )
                last_heartbeat = time.time()
                threading.Thread(target=start_election, daemon=True).start()
        time.sleep(HEARTBEAT_INTERVAL)


# ── gRPC Servicer ─────────────────────────────────────────────────────────────


class AmbulanceServicer(ambulance_pb2_grpc.AmbulanceServiceServicer):
    def SendElection(self, request, context):
        logger.info("Node %d: ELECTION from node %d", NODE_ID, request.sender_id)
        if NODE_ID > request.sender_id:
            # We have higher authority — reply OK and start our own election.
            threading.Thread(target=start_election, daemon=True).start()
            return ambulance_pb2.ElectionResponse(status="OK")
        return ambulance_pb2.ElectionResponse(status="NO")

    def AnnounceCoordinator(self, request, context):
        global coordinator_id, last_heartbeat
        logger.info(
            "Node %d: node %d is the new coordinator",
            NODE_ID,
            request.coordinator_id,
        )
        with coordinator_lock:
            coordinator_id = request.coordinator_id
        with election_lock:
            # Suppress any pending local election
            pass
        last_heartbeat = time.time()
        return ambulance_pb2.CoordinatorResponse(status="ACK")

    def Heartbeat(self, request, context):
        global last_heartbeat
        last_heartbeat = time.time()
        return ambulance_pb2.HeartbeatResponse(status="ALIVE")

    def Dispatch(self, request, context):
        """Coordinator orders this ambulance to respond to a patient."""
        logger.info(
            "Node %d: DISPATCHED to patient '%s' at '%s' (coordinator=%d)",
            NODE_ID,
            request.patient_name,
            request.location,
            request.coordinator_id,
        )
        return ambulance_pb2.DispatchResponse(status="DISPATCHED")


# ── Flask HTTP API ────────────────────────────────────────────────────────────

app = Flask(__name__)


@app.route("/health", methods=["GET"])
def health():
    """Liveness probe endpoint — always returns 200 if the process is alive."""
    return jsonify({"status": "healthy", "node_id": NODE_ID})


@app.route("/status", methods=["GET"])
def status():
    with coordinator_lock:
        coord = coordinator_id
    return jsonify(
        {
            "node_id": NODE_ID,
            "coordinator_id": coord,
            "is_coordinator": coord == NODE_ID,
        }
    )


@app.route("/patient", methods=["POST"])
def patient_request():
    """
    Accept a patient rescue request.  Only the current coordinator processes
    this endpoint; other nodes return 503 with the coordinator's ID so the
    caller can redirect.
    """
    with coordinator_lock:
        is_coord = coordinator_id == NODE_ID

    if not is_coord:
        return (
            jsonify(
                {
                    "error": "Not the coordinator",
                    "coordinator_id": coordinator_id,
                }
            ),
            503,
        )

    data = flask_request.get_json(silent=True) or {}
    patient_name = data.get("patient_name", "Unknown")
    location = data.get("location", "Unknown")

    logger.info(
        "Node %d (Coordinator): patient request — '%s' at '%s'",
        NODE_ID,
        patient_name,
        location,
    )

    # Dispatch the ambulance with the lowest available ID.
    all_ids = sorted([NODE_ID] + list(peers.keys()))
    dispatched_id: int | None = None

    for ambulance_id in all_ids:
        if ambulance_id == NODE_ID:
            # Coordinator is the lowest-ID unit — self-dispatch.
            logger.info("Node %d: self-dispatching as ambulance %d", NODE_ID, NODE_ID)
            dispatched_id = NODE_ID
            break
        else:
            stub = get_stub(ambulance_id)
            if stub is None:
                continue
            try:
                resp = stub.Dispatch(
                    ambulance_pb2.DispatchRequest(
                        patient_name=patient_name,
                        location=location,
                        coordinator_id=NODE_ID,
                    ),
                    timeout=DISPATCH_RPC_TIMEOUT,
                )
                if resp.status == "DISPATCHED":
                    dispatched_id = ambulance_id
                    logger.info(
                        "Node %d: ambulance %d acknowledged dispatch",
                        NODE_ID,
                        ambulance_id,
                    )
                    break
            except grpc.RpcError as exc:
                logger.warning(
                    "Node %d: ambulance %d unreachable (%s) — trying next",
                    NODE_ID,
                    ambulance_id,
                    exc.code(),
                )

    if dispatched_id is None:
        logger.error("Node %d: no ambulance available for dispatch", NODE_ID)
        return jsonify({"error": "No ambulance available"}), 503

    # Notify hospital (coordinator always sends the confirmation).
    payload = {
        "ambulance_id": dispatched_id,
        "patient_name": patient_name,
        "location": location,
        "coordinator_id": NODE_ID,
    }
    try:
        resp = requests.post(
            f"{HOSPITAL_URL}/dispatch",
            json=payload,
            timeout=5,
        )
        logger.info(
            "Node %d: hospital notified — HTTP %d", NODE_ID, resp.status_code
        )
    except requests.RequestException as exc:
        logger.error("Node %d: failed to notify hospital: %s", NODE_ID, exc)

    return jsonify(
        {
            "status": "dispatched",
            "ambulance_id": dispatched_id,
            "coordinator_id": NODE_ID,
            "patient_name": patient_name,
            "location": location,
        }
    )


# ── gRPC server ───────────────────────────────────────────────────────────────


def serve_grpc() -> None:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    ambulance_pb2_grpc.add_AmbulanceServiceServicer_to_server(AmbulanceServicer(), server)
    server.add_insecure_port(f"[::]:{GRPC_PORT}")
    server.start()
    logger.info("Node %d: gRPC server listening on port %d", NODE_ID, GRPC_PORT)
    server.wait_for_termination()


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # 1. Start gRPC server in background.
    threading.Thread(target=serve_grpc, daemon=True).start()

    # 2. Give the gRPC server (and peers) a moment to start up.
    time.sleep(3)

    # 3. Kick off the initial Bully election.
    threading.Thread(target=start_election, daemon=True).start()

    # 4. Monitor coordinator liveness in background.
    threading.Thread(target=_monitor_coordinator, daemon=True).start()

    # 5. Start Flask (blocking).
    logger.info("Node %d: HTTP server listening on port %d", NODE_ID, HTTP_PORT)
    app.run(host="0.0.0.0", port=HTTP_PORT)
