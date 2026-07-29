CREATE TABLE IF NOT EXISTS reconciliation_jobs (
  id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  created_by TEXT NOT NULL,
  file_name TEXT NOT NULL,
  source_data BYTEA NOT NULL,
  content_sha256 CHAR(64) NOT NULL,
  reconciliation_type TEXT NOT NULL,
  ruleset_version TEXT NOT NULL,
  status TEXT NOT NULL CHECK (
    status IN ('QUEUED', 'RUNNING', 'RETRY_WAIT', 'SUCCEEDED', 'FAILED')
  ),
  progress SMALLINT NOT NULL DEFAULT 0 CHECK (progress BETWEEN 0 AND 100),
  attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  error_code TEXT,
  error_message TEXT,
  error_retryable BOOLEAN,
  result JSONB,
  retry_at TIMESTAMPTZ,
  lease_until TIMESTAMPTZ,
  lease_token UUID,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  started_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  UNIQUE (
    tenant_id,
    content_sha256,
    reconciliation_type,
    ruleset_version
  )
);

CREATE TABLE IF NOT EXISTS reconciliation_idempotency (
  tenant_id TEXT NOT NULL,
  idempotency_key VARCHAR(200) NOT NULL,
  request_fingerprint CHAR(64) NOT NULL,
  job_id UUID NOT NULL REFERENCES reconciliation_jobs(id) ON DELETE CASCADE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS reconciliation_jobs_work_idx
  ON reconciliation_jobs (status, retry_at, lease_until, created_at);

CREATE INDEX IF NOT EXISTS reconciliation_jobs_tenant_idx
  ON reconciliation_jobs (tenant_id, created_at DESC);
