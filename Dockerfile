FROM python:3.13-slim

WORKDIR /srv/idempotent-transaction-service

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY sample_data ./sample_data
COPY scripts ./scripts

ENV TXN_DB_PATH=/data/txn.db
VOLUME /data

EXPOSE 8000

# Embedded worker runs inside the API process (TXN_WORKER_MODE=embedded).
# For a separate worker container, set TXN_WORKER_MODE=off and run:
#   python -m app.worker.processor
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
