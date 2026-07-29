import { createHash, randomUUID } from "node:crypto";

import {
  type Pool,
  type PoolClient,
  type QueryResultRow,
} from "pg";

import {
  PermanentJobError,
  ServiceError,
  processReconciliationFile,
  type AuditEvent,
  type JobError,
  type JobStatus,
  type Metrics,
  type Permission,
  type Principal,
  type ReconciliationApi,
  type ReconciliationJob,
  type ReconciliationProcessor,
  type ReconciliationResult,
  type Submission,
  type SubmitRequest,
} from "./reconciliation-service.js";

interface JobRow extends QueryResultRow {
  id: string;
  tenant_id: string;
  created_by: string;
  file_name: string;
  source_data: Buffer;
  content_sha256: string;
  reconciliation_type: string;
  ruleset_version: string;
  status: string;
  progress: number;
  attempts: number;
  error_code: string | null;
  error_message: string | null;
  error_retryable: boolean | null;
  result: unknown;
  lease_token: string | null;
  created_at: Date;
  started_at: Date | null;
  completed_at: Date | null;
}

interface IdempotencyRow extends QueryResultRow {
  request_fingerprint: string;
  job_id: string;
}

interface MetricsRow extends QueryResultRow {
  submissions: string;
  idempotency_records: string;
  succeeded: string;
  failed: string;
  retries: string;
  queue_depth: string;
}

type StoredJob = Readonly<{
  view: ReconciliationJob;
  file: Uint8Array;
  result: ReconciliationResult | null;
  leaseToken: string | null;
}>;

export type PostgresServiceOptions = Readonly<{
  processor?: ReconciliationProcessor;
  maxAttempts?: number;
  retryDelayMs?: number;
  maxFileBytes?: number;
  pollIntervalMs?: number;
  leaseMs?: number;
  logger?: (event: AuditEvent) => void;
  onWorkerError?: (error: unknown) => void;
  now?: () => Date;
  newId?: () => string;
}>;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function parseStatus(value: string): JobStatus {
  switch (value) {
    case "QUEUED":
    case "RUNNING":
    case "RETRY_WAIT":
    case "SUCCEEDED":
    case "FAILED":
      return value;
    default:
      throw new Error(`Unknown job status: ${value}`);
  }
}

function parseResult(value: unknown): ReconciliationResult | null {
  if (value === null) {
    return null;
  }
  if (
    !isRecord(value) ||
    typeof value.transactionCount !== "number" ||
    !Number.isInteger(value.transactionCount) ||
    !Array.isArray(value.totals)
  ) {
    throw new Error("Stored reconciliation result is invalid.");
  }

  const totalValues: readonly unknown[] = value.totals;
  const totals = totalValues.map((item) => {
    if (
      !isRecord(item) ||
      typeof item.account !== "string" ||
      typeof item.currency !== "string" ||
      typeof item.total !== "number" ||
      !Number.isFinite(item.total)
    ) {
      throw new Error("Stored reconciliation total is invalid.");
    }
    return {
      account: item.account,
      currency: item.currency,
      total: item.total,
    };
  });
  return { transactionCount: value.transactionCount, totals };
}

function parseError(row: JobRow): JobError | null {
  if (row.error_code === null) {
    return null;
  }
  return {
    code: row.error_code,
    message: row.error_message ?? "Processing failed.",
    retryable: row.error_retryable ?? false,
  };
}

function mapJob(row: JobRow): StoredJob {
  return {
    view: {
      id: row.id,
      tenantId: row.tenant_id,
      fileName: row.file_name,
      contentSha256: row.content_sha256.trim(),
      reconciliationType: row.reconciliation_type,
      rulesetVersion: row.ruleset_version,
      status: parseStatus(row.status),
      progress: row.progress,
      attempts: row.attempts,
      error: parseError(row),
      createdAt: row.created_at.toISOString(),
      startedAt: row.started_at?.toISOString() ?? null,
      completedAt: row.completed_at?.toISOString() ?? null,
    },
    file: new Uint8Array(row.source_data),
    result: parseResult(row.result),
    leaseToken: row.lease_token,
  };
}

function requireText(value: string, field: string): string {
  const normalized = value.trim();
  if (normalized.length === 0) {
    throw new ServiceError(400, "INVALID_REQUEST", `${field} is required.`);
  }
  return normalized;
}

async function findJob(
  client: Pool | PoolClient,
  id: string,
  tenantId: string,
): Promise<StoredJob | undefined> {
  const result = await client.query<JobRow>(
    `SELECT *
       FROM reconciliation_jobs
      WHERE id = $1 AND tenant_id = $2`,
    [id, tenantId],
  );
  const row = result.rows[0];
  return row === undefined ? undefined : mapJob(row);
}

export class PostgresReconciliationService implements ReconciliationApi {
  readonly #pool: Pool;
  readonly #processor: ReconciliationProcessor;
  readonly #maxAttempts: number;
  readonly #retryDelayMs: number;
  readonly #maxFileBytes: number;
  readonly #pollIntervalMs: number;
  readonly #leaseMs: number;
  readonly #logger: (event: AuditEvent) => void;
  readonly #onWorkerError: (error: unknown) => void;
  readonly #now: () => Date;
  readonly #newId: () => string;
  #started = false;
  #worker: Promise<void> | null = null;
  #pollTimer: NodeJS.Timeout | null = null;

  constructor(pool: Pool, options: PostgresServiceOptions = {}) {
    this.#pool = pool;
    this.#processor = options.processor ?? processReconciliationFile;
    this.#maxAttempts = options.maxAttempts ?? 3;
    this.#retryDelayMs = options.retryDelayMs ?? 1_000;
    this.#maxFileBytes = options.maxFileBytes ?? 5 * 1024 * 1024;
    this.#pollIntervalMs = options.pollIntervalMs ?? 500;
    this.#leaseMs = options.leaseMs ?? 60_000;
    this.#logger = options.logger ?? (() => undefined);
    this.#onWorkerError = options.onWorkerError ?? (() => undefined);
    this.#now = options.now ?? (() => new Date());
    this.#newId = options.newId ?? randomUUID;
  }

  start(): void {
    if (!this.#started) {
      this.#started = true;
      this.#kick();
    }
  }

  async stop(): Promise<void> {
    this.#started = false;
    if (this.#pollTimer !== null) {
      clearTimeout(this.#pollTimer);
      this.#pollTimer = null;
    }
    await this.#worker;
  }

  async submit(
    request: SubmitRequest,
    principal: Principal,
  ): Promise<Submission> {
    this.#authorize(principal, "reconciliation.submit");
    const fileName = requireText(request.fileName, "fileName");
    const reconciliationType = requireText(
      request.reconciliationType,
      "reconciliationType",
    );
    const rulesetVersion = requireText(
      request.rulesetVersion,
      "rulesetVersion",
    );
    const idempotencyKey = requireText(
      request.idempotencyKey,
      "Idempotency-Key",
    );
    if (idempotencyKey.length > 200) {
      throw new ServiceError(400, "INVALID_REQUEST", "Idempotency-Key is too long.");
    }
    if (request.file.byteLength === 0) {
      throw new ServiceError(400, "EMPTY_FILE", "File must not be empty.");
    }
    if (request.file.byteLength > this.#maxFileBytes) {
      throw new ServiceError(413, "FILE_TOO_LARGE", "File exceeds the size limit.");
    }

    const contentSha256 = createHash("sha256")
      .update(request.file)
      .digest("hex");
    const fingerprint = createHash("sha256")
      .update(
        JSON.stringify([
          fileName,
          contentSha256,
          reconciliationType,
          rulesetVersion,
        ]),
      )
      .digest("hex");
    const client = await this.#pool.connect();
    try {
      await client.query("BEGIN");
      await client.query(
        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
        [JSON.stringify([principal.tenantId, idempotencyKey])],
      );

      const prior = await client.query<IdempotencyRow>(
        `SELECT request_fingerprint, job_id
           FROM reconciliation_idempotency
          WHERE tenant_id = $1 AND idempotency_key = $2`,
        [principal.tenantId, idempotencyKey],
      );
      const priorRecord = prior.rows[0];
      if (priorRecord !== undefined) {
        if (priorRecord.request_fingerprint.trim() !== fingerprint) {
          throw new ServiceError(
            409,
            "IDEMPOTENCY_CONFLICT",
            "Idempotency-Key was already used for a different request.",
          );
        }
        const existing = await findJob(
          client,
          priorRecord.job_id,
          principal.tenantId,
        );
        if (existing === undefined) {
          throw new Error("Idempotency record references a missing job.");
        }
        await client.query("COMMIT");
        this.#kick();
        return { job: existing.view, duplicate: true };
      }

      const inserted = await client.query<JobRow>(
        `INSERT INTO reconciliation_jobs (
           id, tenant_id, created_by, file_name, source_data, content_sha256,
           reconciliation_type, ruleset_version, status
         )
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'QUEUED')
         ON CONFLICT (
           tenant_id, content_sha256, reconciliation_type, ruleset_version
         ) DO NOTHING
         RETURNING *`,
        [
          this.#newId(),
          principal.tenantId,
          principal.subject,
          fileName,
          Buffer.from(request.file),
          contentSha256,
          reconciliationType,
          rulesetVersion,
        ],
      );
      let row = inserted.rows[0];
      const duplicate = row === undefined;
      if (row === undefined) {
        const existing = await client.query<JobRow>(
          `SELECT *
             FROM reconciliation_jobs
            WHERE tenant_id = $1
              AND content_sha256 = $2
              AND reconciliation_type = $3
              AND ruleset_version = $4`,
          [
            principal.tenantId,
            contentSha256,
            reconciliationType,
            rulesetVersion,
          ],
        );
        row = existing.rows[0];
      }
      if (row === undefined) {
        throw new Error("Failed to create or find reconciliation job.");
      }

      await client.query(
        `INSERT INTO reconciliation_idempotency (
           tenant_id, idempotency_key, request_fingerprint, job_id
         ) VALUES ($1, $2, $3, $4)`,
        [principal.tenantId, idempotencyKey, fingerprint, row.id],
      );
      await client.query("COMMIT");
      const job = mapJob(row);
      this.#log(duplicate ? "job.duplicate" : "job.queued", job.view);
      this.#kick();
      return { job: job.view, duplicate };
    } catch (error: unknown) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async getJob(id: string, principal: Principal): Promise<ReconciliationJob> {
    this.#authorize(principal, "reconciliation.read");
    return (await this.#ownedJob(id, principal)).view;
  }

  async getResult(
    id: string,
    principal: Principal,
  ): Promise<ReconciliationResult> {
    this.#authorize(principal, "reconciliation.read");
    const job = await this.#ownedJob(id, principal);
    if (job.view.status !== "SUCCEEDED" || job.result === null) {
      throw new ServiceError(409, "RESULT_NOT_READY", "Result is not available.");
    }
    return job.result;
  }

  async retry(id: string, principal: Principal): Promise<ReconciliationJob> {
    this.#authorize(principal, "reconciliation.retry");
    const existing = await this.#ownedJob(id, principal);
    if (
      existing.view.status !== "FAILED" ||
      existing.view.error?.retryable !== true
    ) {
      throw new ServiceError(409, "NOT_RETRYABLE", "Job cannot be retried.");
    }
    const result = await this.#pool.query<JobRow>(
      `UPDATE reconciliation_jobs
          SET status = 'QUEUED',
              progress = 0,
              error_code = NULL,
              error_message = NULL,
              error_retryable = NULL,
              retry_at = NULL,
              completed_at = NULL
        WHERE id = $1
          AND tenant_id = $2
          AND status = 'FAILED'
          AND error_retryable = TRUE
      RETURNING *`,
      [id, principal.tenantId],
    );
    const row = result.rows[0];
    if (row === undefined) {
      throw new ServiceError(409, "NOT_RETRYABLE", "Job cannot be retried.");
    }
    const job = mapJob(row);
    this.#log("job.manual_retry", job.view);
    this.#kick();
    return job.view;
  }

  async getMetrics(
    principal: Principal,
  ): Promise<Readonly<Metrics & { queueDepth: number }>> {
    this.#authorize(principal, "reconciliation.admin");
    const result = await this.#pool.query<MetricsRow>(
      `SELECT
         COUNT(*)::text AS submissions,
         (SELECT COUNT(*)::text FROM reconciliation_idempotency)
           AS idempotency_records,
         COUNT(*) FILTER (WHERE status = 'SUCCEEDED')::text AS succeeded,
         COUNT(*) FILTER (WHERE status = 'FAILED')::text AS failed,
         COALESCE(SUM(GREATEST(attempts - 1, 0)), 0)::text AS retries,
         COUNT(*) FILTER (
           WHERE status IN ('QUEUED', 'RETRY_WAIT')
         )::text AS queue_depth
       FROM reconciliation_jobs`,
    );
    const row = result.rows[0];
    if (row === undefined) {
      throw new Error("Metrics query returned no row.");
    }
    const submissions = Number(row.submissions);
    return {
      submissions,
      duplicates: Math.max(0, Number(row.idempotency_records) - submissions),
      succeeded: Number(row.succeeded),
      failed: Number(row.failed),
      retries: Number(row.retries),
      queueDepth: Number(row.queue_depth),
    };
  }

  async #ownedJob(id: string, principal: Principal): Promise<StoredJob> {
    const job = await findJob(this.#pool, id, principal.tenantId);
    if (job === undefined) {
      throw new ServiceError(404, "NOT_FOUND", "Job was not found.");
    }
    return job;
  }

  #authorize(principal: Principal, permission: Permission): void {
    if (!principal.permissions.includes(permission)) {
      throw new ServiceError(403, "FORBIDDEN", "Permission denied.");
    }
  }

  #kick(): void {
    if (!this.#started || this.#worker !== null) {
      return;
    }
    if (this.#pollTimer !== null) {
      clearTimeout(this.#pollTimer);
      this.#pollTimer = null;
    }
    this.#worker = this.#work()
      .catch((error: unknown) => this.#onWorkerError(error))
      .finally(() => {
        this.#worker = null;
        if (this.#started) {
          this.#pollTimer = setTimeout(() => {
            this.#pollTimer = null;
            this.#kick();
          }, this.#pollIntervalMs);
        }
      });
  }

  async #work(): Promise<void> {
    let job = await this.#claimNext();
    while (this.#started && job !== undefined) {
      await this.#run(job);
      job = await this.#claimNext();
    }
  }

  async #claimNext(): Promise<StoredJob | undefined> {
    const now = this.#now();
    const leaseToken = this.#newId();
    const leaseUntil = new Date(now.getTime() + this.#leaseMs);
    const result = await this.#pool.query<JobRow>(
      `WITH candidate AS (
         SELECT id
           FROM reconciliation_jobs
          WHERE status = 'QUEUED'
             OR (status = 'RETRY_WAIT' AND retry_at <= $1)
             OR (status = 'RUNNING' AND lease_until <= $1)
          ORDER BY created_at
          FOR UPDATE SKIP LOCKED
          LIMIT 1
       )
       UPDATE reconciliation_jobs AS job
          SET status = 'RUNNING',
              progress = 10,
              attempts = attempts + 1,
              started_at = COALESCE(started_at, $1),
              lease_until = $2,
              lease_token = $3,
              retry_at = NULL
         FROM candidate
        WHERE job.id = candidate.id
      RETURNING job.*`,
      [now, leaseUntil, leaseToken],
    );
    const row = result.rows[0];
    return row === undefined ? undefined : mapJob(row);
  }

  async #run(job: StoredJob): Promise<void> {
    const leaseToken = job.leaseToken;
    if (leaseToken === null) {
      throw new Error("Claimed job has no lease token.");
    }
    this.#log("job.running", job.view);
    const heartbeat = setInterval(() => {
      void this.#extendLease(job.view.id, leaseToken).catch(this.#onWorkerError);
    }, Math.max(100, Math.floor(this.#leaseMs / 3)));

    try {
      const result = await this.#processor(new Uint8Array(job.file));
      const updated = await this.#pool.query(
        `UPDATE reconciliation_jobs
            SET status = 'SUCCEEDED',
                progress = 100,
                result = $3::jsonb,
                error_code = NULL,
                error_message = NULL,
                error_retryable = NULL,
                completed_at = $4,
                lease_until = NULL,
                lease_token = NULL
          WHERE id = $1 AND lease_token = $2`,
        [
          job.view.id,
          leaseToken,
          JSON.stringify(result),
          this.#now(),
        ],
      );
      if (updated.rowCount === 1) {
        this.#log("job.succeeded", {
          ...job.view,
          status: "SUCCEEDED",
          progress: 100,
        });
      }
    } catch (error: unknown) {
      if (error instanceof PermanentJobError) {
        await this.#fail(job, leaseToken, error.code, error.message, false);
      } else if (job.view.attempts >= this.#maxAttempts) {
        await this.#fail(
          job,
          leaseToken,
          "PROCESSING_FAILED",
          "Retry limit reached.",
          true,
        );
      } else {
        const delay = this.#retryDelayMs * 2 ** (job.view.attempts - 1);
        const retryAt = new Date(this.#now().getTime() + delay);
        const updated = await this.#pool.query(
          `UPDATE reconciliation_jobs
              SET status = 'RETRY_WAIT',
                  progress = 0,
                  error_code = 'TRANSIENT_FAILURE',
                  error_message = 'Temporary processing failure.',
                  error_retryable = TRUE,
                  retry_at = $3,
                  lease_until = NULL,
                  lease_token = NULL
            WHERE id = $1 AND lease_token = $2`,
          [job.view.id, leaseToken, retryAt],
        );
        if (updated.rowCount === 1) {
          this.#log("job.retry_wait", {
            ...job.view,
            status: "RETRY_WAIT",
            progress: 0,
          });
        }
      }
    } finally {
      clearInterval(heartbeat);
    }
  }

  async #extendLease(id: string, leaseToken: string): Promise<void> {
    const leaseUntil = new Date(this.#now().getTime() + this.#leaseMs);
    await this.#pool.query(
      `UPDATE reconciliation_jobs
          SET lease_until = $3
        WHERE id = $1 AND lease_token = $2 AND status = 'RUNNING'`,
      [id, leaseToken, leaseUntil],
    );
  }

  async #fail(
    job: StoredJob,
    leaseToken: string,
    code: string,
    message: string,
    retryable: boolean,
  ): Promise<void> {
    const updated = await this.#pool.query(
      `UPDATE reconciliation_jobs
          SET status = 'FAILED',
              progress = 100,
              error_code = $3,
              error_message = $4,
              error_retryable = $5,
              completed_at = $6,
              lease_until = NULL,
              lease_token = NULL
        WHERE id = $1 AND lease_token = $2`,
      [job.view.id, leaseToken, code, message, retryable, this.#now()],
    );
    if (updated.rowCount === 1) {
      this.#log("job.failed", {
        ...job.view,
        status: "FAILED",
        progress: 100,
      });
    }
  }

  #log(event: string, job: ReconciliationJob): void {
    this.#logger({
      event,
      jobId: job.id,
      tenantId: job.tenantId,
      status: job.status,
      attempt: job.attempts,
      timestamp: this.#now().toISOString(),
    });
  }
}
