# Reconciliation service

Run:

```sh
npm test
npm run build
API_TOKEN=a-long-random-secret npm start
```

Submit a JSON transaction file as the request body:

```http
POST /v1/reconciliations
Authorization: Bearer <token>
Idempotency-Key: <unique key>
X-File-Name: transactions.json
X-Reconciliation-Type: daily
X-Ruleset-Version: v1
```

Use the returned `Location` with:

- `GET /v1/reconciliations/{id}`
- `GET /v1/reconciliations/{id}/result`
- `POST /v1/reconciliations/{id}/retry`
- `GET /metrics`

The dependency-free implementation stores jobs and its queue in memory. For a
multi-instance production deployment, keep the service API but replace these
with PostgreSQL plus a transactional outbox, private object storage, and a
durable queue. Deploy the API and workers as separate managed container
services; replace the static token authenticator with OIDC.

SLO: 99.9% of authenticated status requests succeed within two seconds monthly.

Cost risk: repeated processing of pathological large files can multiply worker
cost; file-size, attempt, runtime, and concurrency limits bound it.
