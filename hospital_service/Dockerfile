# ── Hospital Service Image ────────────────────────────────────────────────────

FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY hospital_service.py .

EXPOSE 5000

CMD ["python", "hospital_service.py"]
