# Demo script

A 5-minute live walkthrough. Assumes `docker compose up --build` is running (API on `:8080`).

## 0. Health

```bash
curl -s localhost:8080/healthz   # ok
curl -s localhost:8080/readyz    # ready (checks DB)
```

## 1. Submit and watch a job complete

```bash
ID=$(curl -s -X POST localhost:8080/api/v1/jobs \
  -H 'content-type: application/json' \
  -d '{"queue":"default","job_type":"sum","payload":{"numbers":[1,2,3,4]}}' \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["id"])')
echo "job=$ID"

# Live status stream — watch it go queued → processing → completed
curl -N localhost:8080/api/v1/jobs/$ID/events &
sleep 2
curl -s localhost:8080/api/v1/jobs/$ID | python3 -m json.tool   # result: {"sum":10.0}
```

## 2. Idempotency

```bash
# Submit twice with the same key → same id, second is not "created"
curl -s -X POST localhost:8080/api/v1/jobs -H 'content-type: application/json' \
  -d '{"queue":"default","job_type":"echo","payload":{},"idempotency_key":"demo-1"}' | python3 -m json.tool
curl -si -X POST localhost:8080/api/v1/jobs -H 'content-type: application/json' \
  -d '{"queue":"default","job_type":"echo","payload":{},"idempotency_key":"demo-1"}' | head -1  # HTTP/1.1 200 (not 201)
```

## 3. Retries, backoff, dead-letter

```bash
# A job that always fails, max 2 attempts → ends dead_lettered
curl -s -X POST localhost:8080/api/v1/jobs -H 'content-type: application/json' \
  -d '{"queue":"default","job_type":"fail","payload":{},"max_attempts":2,"backoff_base_secs":1}'
# Watch attempts accumulate, then check the DLQ
sleep 5
curl -s 'localhost:8080/api/v1/dlq?queue=default' | python3 -m json.tool
```

## 4. Flaky job that recovers

```bash
# Fails twice then succeeds — proves retries lead to eventual success
curl -s -X POST localhost:8080/api/v1/jobs -H 'content-type: application/json' \
  -d '{"queue":"default","job_type":"flaky","payload":{"fail_until":3},"backoff_base_secs":1}'
```

## 5. Pause / resume and stats

```bash
curl -s -X POST localhost:8080/api/v1/queues/default/pause
curl -s localhost:8080/api/v1/queues/default/stats | python3 -m json.tool
curl -s -X POST localhost:8080/api/v1/queues/default/resume
```

## 6. Observability

```bash
curl -s localhost:8080/metrics | grep -E 'djq_jobs_(completed|failed|retried|dead_lettered)_total|djq_queue_depth|djq_active_workers'
# Prometheus UI: http://localhost:9090
```

## 7. Throughput

```bash
COUNT=2000 CONCURRENCY=32 ./scripts/load_test.sh
```

## 8. Graceful shutdown

```bash
# Scale workers, submit a burst, then stop a worker mid-flight — jobs are
# recovered and finished by the remaining workers, none lost.
docker compose up --scale worker=3 -d
```
