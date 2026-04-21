# ── Ambulance Node Image ──────────────────────────────────────────────────────
# Generates gRPC stubs from ambulance.proto at build time, then runs the node.

FROM python:3.11-slim

WORKDIR /app

# Install Python dependencies (includes grpcio-tools for proto compilation).
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the proto file and compile it to Python stubs.
COPY ambulance.proto .
RUN python -m grpc_tools.protoc \
        -I. \
        --python_out=. \
        --grpc_python_out=. \
        ambulance.proto

# Copy the application source.
COPY ambulance_node.py .

# Expose gRPC and HTTP ports.
EXPOSE 50051 8080

CMD ["python", "ambulance_node.py"]
