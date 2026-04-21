"""
Hospital Service
================
A lightweight Flask REST API that represents the receiving hospital.

Endpoints
---------
POST /dispatch
    Receives a dispatch-confirmed notification from the Ambulance Coordinator.
    Expected JSON body::

        {
            "ambulance_id":   1,
            "patient_name":   "Jane Doe",
            "location":       "123 Main St",
            "coordinator_id": 3
        }

    Returns 200 with a JSON acknowledgement.

GET /dispatches
    Returns all dispatch records logged so far (in-memory, not persisted).

GET /health
    Liveness probe — always returns 200.
"""

import logging
from datetime import datetime, timezone

from flask import Flask, jsonify, request

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# In-memory log of all received dispatch confirmations.
dispatches: list[dict] = []


@app.route("/dispatch", methods=["POST"])
def receive_dispatch():
    data = request.get_json(silent=True) or {}

    ambulance_id = data.get("ambulance_id", "unknown")
    patient_name = data.get("patient_name", "unknown")
    location = data.get("location", "unknown")
    coordinator_id = data.get("coordinator_id", "unknown")

    record = {
        "ambulance_id": ambulance_id,
        "patient_name": patient_name,
        "location": location,
        "coordinator_id": coordinator_id,
        "received_at": datetime.now(timezone.utc).isoformat(),
    }
    dispatches.append(record)

    logger.info(
        "Dispatch confirmed — ambulance %s → patient '%s' at '%s' "
        "(coordinator %s)",
        ambulance_id,
        patient_name,
        location,
        coordinator_id,
    )

    return jsonify({"status": "received", "record": record}), 200


@app.route("/dispatches", methods=["GET"])
def list_dispatches():
    return jsonify(dispatches), 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy", "service": "hospital"}), 200


if __name__ == "__main__":
    import os

    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
