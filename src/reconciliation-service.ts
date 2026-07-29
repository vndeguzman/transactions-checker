import { createHash, randomUUID } from "node:crypto";

import {
  groupTransactionTotals,
  type AccountCurrencyTotal,
} from "./transaction-validator.js";

export type JobStatus =
  | "QUEUED"
  | "RUNNING"
  | "RETRY_WAIT"
  | "SUCCEEDED"
  | "FAILED";

export type Permission =
  | "reconciliation.submit"
  | "reconciliation.read"
  | "reconciliation.retry"
  | "reconciliation.admin";

export type Principal = Readonly<{
  subject: string;
  tenantId: string;
  permissions: readonly Permission[];
}>;

export type ReconciliationResult = Readonly<{
  transactionCount: number;
  totals: readonly AccountCurrencyTotal[];
}>;

export type JobError = Readonly<{
  code: string;
  message: string;
  retryable: boolean;
}>;

export type ReconciliationJob = Readonly<{
  id: string;
  tenantId: string;
  fileName: string;
  contentSha256: string;
  reconciliationType: string;
  rulesetVersion: string;
  status: JobStatus;
  progress: number;
  attempts: number;
  error: JobError | null;
  createdAt: string;
  startedAt: string | null;
  completedAt: string | null;
}>;

export type Submission = Readonly<{
  job: ReconciliationJob;
  duplicate: boolean;
}>;

export type SubmitRequest = Readonly<{
  fileName: string;
  file: Uint8Array;
  reconciliationType: string;
  rulesetVersion: string;
  idempotencyKey: string;
}>;

export type ReconciliationProcessor = (
  file: Uint8Array,
) => Promise<ReconciliationResult>;

export type AuditEvent = Readonly<{
  event: string;
  jobId: string;
  tenantId: string;
  status: JobStatus;
  attempt: number;
  timestamp: string;
}>;

export class ServiceError extends Error {
  constructor(
    readonly statusCode: number,
    readonly code: string,
    message: string,
  ) {
    super(message);
  }
}

export class PermanentJobError extends Error {
  constructor(
    readonly code: string,
    message: string,
  ) {
    super(message);
  }
}

type JobRecord = {
  id: string;
  tenantId: string;
  fileName: string;
  file: Uint8Array;
  contentSha256: string;
  reconciliationType: string;
  rulesetVersion: string;
  status: JobStatus;
  progress: number;
  attempts: number;
  error: JobError | null;
  result: ReconciliationResult | null;
  createdAt: string;
  startedAt: string | null;
  completedAt: string | null;
};

type IdempotencyRecord = Readonly<{
  fingerprint: string;
  jobId: string;
}>;

type Metrics = {
  submissions: number;
  duplicates: number;
  succeeded: number;
  failed: number;
  retries: number;
};

type Options = Readonly<{
  processor?: ReconciliationProcessor;
  maxAttempts?: number;
  retryDelayMs?: number;
  maxFileBytes?: number;
  logger?: (event: AuditEvent) => void;
  now?: () => Date;
  newId?: () => string;
}>;

async function defaultProcessor(
  file: Uint8Array,
): Promise<ReconciliationResult> {
  let parsed: unknown;
  try {
    parsed = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(file));
  } catch {
    throw new PermanentJobError("INVALID_JSON", "File is not valid UTF-8 JSON.");
  }

  const result = groupTransactionTotals(parsed);
  if (!result.ok) {
    throw new PermanentJobError(
      "INVALID_TRANSACTIONS",
      `File failed validation with ${result.errors.length} error(s).`,
    );
  }
  return {
    transactionCount: result.transactionCount,
    totals: result.totals,
  };
}

function requireText(value: string, field: string): string {
  const normalized = value.trim();
  if (normalized.length === 0) {
    throw new ServiceError(400, "INVALID_REQUEST", `${field} is required.`);
  }
  return normalized;
}

export class ReconciliationService {
  readonly #jobs = new Map<string, JobRecord>();
  readonly #idempotency = new Map<string, IdempotencyRecord>();
  readonly #content = new Map<string, string>();
  readonly #queue: string[] = [];
  readonly #idleWaiters: Array<() => void> = [];
  readonly #metrics: Metrics = {
    submissions: 0,
    duplicates: 0,
    succeeded: 0,
    failed: 0,
    retries: 0,
  };
  readonly #processor: ReconciliationProcessor;
  readonly #maxAttempts: number;
  readonly #retryDelayMs: number;
  readonly #maxFileBytes: number;
  readonly #logger: (event: AuditEvent) => void;
  readonly #now: () => Date;
  readonly #newId: () => string;
  #processing = false;
  #pendingRetries = 0;

  constructor(options: Options = {}) {
    this.#processor = options.processor ?? defaultProcessor;
    this.#maxAttempts = options.maxAttempts ?? 3;
    this.#retryDelayMs = options.retryDelayMs ?? 100;
    this.#maxFileBytes = options.maxFileBytes ?? 5 * 1024 * 1024;
    this.#logger = options.logger ?? (() => undefined);
    this.#now = options.now ?? (() => new Date());
    this.#newId = options.newId ?? randomUUID;
  }

  submit(request: SubmitRequest, principal: Principal): Submission {
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
    const idempotencyIndex = `${principal.tenantId}\0${idempotencyKey}`;
    const priorRequest = this.#idempotency.get(idempotencyIndex);
    if (priorRequest !== undefined) {
      if (priorRequest.fingerprint !== fingerprint) {
        throw new ServiceError(
          409,
          "IDEMPOTENCY_CONFLICT",
          "Idempotency-Key was already used for a different request.",
        );
      }
      this.#metrics.duplicates += 1;
      return {
        job: this.#view(this.#mustFind(priorRequest.jobId)),
        duplicate: true,
      };
    }

    const contentIndex = [
      principal.tenantId,
      contentSha256,
      reconciliationType,
      rulesetVersion,
    ].join("\0");
    const priorJobId = this.#content.get(contentIndex);
    if (priorJobId !== undefined) {
      this.#idempotency.set(idempotencyIndex, {
        fingerprint,
        jobId: priorJobId,
      });
      this.#metrics.duplicates += 1;
      return { job: this.#view(this.#mustFind(priorJobId)), duplicate: true };
    }

    const now = this.#now().toISOString();
    const job: JobRecord = {
      id: this.#newId(),
      tenantId: principal.tenantId,
      fileName,
      file: new Uint8Array(request.file),
      contentSha256,
      reconciliationType,
      rulesetVersion,
      status: "QUEUED",
      progress: 0,
      attempts: 0,
      error: null,
      result: null,
      createdAt: now,
      startedAt: null,
      completedAt: null,
    };
    this.#jobs.set(job.id, job);
    this.#content.set(contentIndex, job.id);
    this.#idempotency.set(idempotencyIndex, {
      fingerprint,
      jobId: job.id,
    });
    this.#queue.push(job.id);
    this.#metrics.submissions += 1;
    this.#log("job.queued", job);
    this.#kick();
    return { job: this.#view(job), duplicate: false };
  }

  getJob(id: string, principal: Principal): ReconciliationJob {
    this.#authorize(principal, "reconciliation.read");
    return this.#view(this.#ownedJob(id, principal));
  }

  getResult(id: string, principal: Principal): ReconciliationResult {
    this.#authorize(principal, "reconciliation.read");
    const job = this.#ownedJob(id, principal);
    if (job.status !== "SUCCEEDED" || job.result === null) {
      throw new ServiceError(409, "RESULT_NOT_READY", "Result is not available.");
    }
    return job.result;
  }

  retry(id: string, principal: Principal): ReconciliationJob {
    this.#authorize(principal, "reconciliation.retry");
    const job = this.#ownedJob(id, principal);
    if (job.status !== "FAILED" || job.error?.retryable !== true) {
      throw new ServiceError(409, "NOT_RETRYABLE", "Job cannot be retried.");
    }
    job.status = "QUEUED";
    job.progress = 0;
    job.error = null;
    job.completedAt = null;
    this.#queue.push(job.id);
    this.#log("job.manual_retry", job);
    this.#kick();
    return this.#view(job);
  }

  getMetrics(principal: Principal): Readonly<Metrics & { queueDepth: number }> {
    this.#authorize(principal, "reconciliation.admin");
    return { ...this.#metrics, queueDepth: this.#queue.length };
  }

  async waitForIdle(): Promise<void> {
    if (!this.#processing && this.#queue.length === 0 && this.#pendingRetries === 0) {
      return;
    }
    await new Promise<void>((resolve) => this.#idleWaiters.push(resolve));
  }

  #authorize(principal: Principal, permission: Permission): void {
    if (!principal.permissions.includes(permission)) {
      throw new ServiceError(403, "FORBIDDEN", "Permission denied.");
    }
  }

  #ownedJob(id: string, principal: Principal): JobRecord {
    const job = this.#jobs.get(id);
    if (job === undefined || job.tenantId !== principal.tenantId) {
      throw new ServiceError(404, "NOT_FOUND", "Job was not found.");
    }
    return job;
  }

  #mustFind(id: string): JobRecord {
    const job = this.#jobs.get(id);
    if (job === undefined) {
      throw new Error("Internal job index is inconsistent.");
    }
    return job;
  }

  #view(job: JobRecord): ReconciliationJob {
    return {
      id: job.id,
      tenantId: job.tenantId,
      fileName: job.fileName,
      contentSha256: job.contentSha256,
      reconciliationType: job.reconciliationType,
      rulesetVersion: job.rulesetVersion,
      status: job.status,
      progress: job.progress,
      attempts: job.attempts,
      error: job.error,
      createdAt: job.createdAt,
      startedAt: job.startedAt,
      completedAt: job.completedAt,
    };
  }

  #kick(): void {
    if (this.#processing) {
      return;
    }
    this.#processing = true;
    queueMicrotask(() => void this.#drain());
  }

  async #drain(): Promise<void> {
    let jobId = this.#queue.shift();
    while (jobId !== undefined) {
      const job = this.#mustFind(jobId);
      await this.#run(job);
      jobId = this.#queue.shift();
    }
    this.#processing = false;
    this.#resolveIdle();
  }

  async #run(job: JobRecord): Promise<void> {
    job.status = "RUNNING";
    job.progress = 10;
    job.attempts += 1;
    job.startedAt = this.#now().toISOString();
    this.#log("job.running", job);

    try {
      job.result = await this.#processor(new Uint8Array(job.file));
      job.status = "SUCCEEDED";
      job.progress = 100;
      job.completedAt = this.#now().toISOString();
      this.#metrics.succeeded += 1;
      this.#log("job.succeeded", job);
    } catch (error: unknown) {
      if (error instanceof PermanentJobError) {
        this.#fail(job, error.code, error.message, false);
      } else if (job.attempts >= this.#maxAttempts) {
        this.#fail(job, "PROCESSING_FAILED", "Retry limit reached.", true);
      } else {
        job.status = "RETRY_WAIT";
        job.progress = 0;
        job.error = {
          code: "TRANSIENT_FAILURE",
          message: "Temporary processing failure.",
          retryable: true,
        };
        this.#metrics.retries += 1;
        this.#pendingRetries += 1;
        this.#log("job.retry_wait", job);
        const delay = this.#retryDelayMs * 2 ** (job.attempts - 1);
        setTimeout(() => {
          this.#pendingRetries -= 1;
          if (job.status === "RETRY_WAIT") {
            job.status = "QUEUED";
            this.#queue.push(job.id);
            this.#kick();
          }
          this.#resolveIdle();
        }, delay);
      }
    }
  }

  #fail(
    job: JobRecord,
    code: string,
    message: string,
    retryable: boolean,
  ): void {
    job.status = "FAILED";
    job.progress = 100;
    job.error = { code, message, retryable };
    job.completedAt = this.#now().toISOString();
    this.#metrics.failed += 1;
    this.#log("job.failed", job);
  }

  #log(event: string, job: JobRecord): void {
    this.#logger({
      event,
      jobId: job.id,
      tenantId: job.tenantId,
      status: job.status,
      attempt: job.attempts,
      timestamp: this.#now().toISOString(),
    });
  }

  #resolveIdle(): void {
    if (this.#processing || this.#queue.length > 0 || this.#pendingRetries > 0) {
      return;
    }
    let resolve = this.#idleWaiters.shift();
    while (resolve !== undefined) {
      resolve();
      resolve = this.#idleWaiters.shift();
    }
  }
}
