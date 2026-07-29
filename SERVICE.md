# Reconciliation service

Run:

```sh
npm test
docker compose up --build
```

The Compose API is available at `http://localhost:3001` with bearer token
`local-development-token`.

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

The Express service stores jobs, source files, results, idempotency records, and
work leases in PostgreSQL. Migrations run during startup. Workers claim jobs
with `FOR UPDATE SKIP LOCKED`; lease tokens and heartbeats make stale worker
completion harmless.

Run the PostgreSQL integration test with:

```sh
TEST_DATABASE_URL=postgres://reconciliation:reconciliation@localhost:5433/reconciliation npm test
```

For production, deploy API and worker containers separately, move large source
files to private object storage, and replace the static token with OIDC.

SLO: 99.9% of authenticated status requests succeed within two seconds monthly.

Cost risk: repeated processing of pathological large files can multiply worker
cost; file-size, attempt, runtime, and concurrency limits bound it.
