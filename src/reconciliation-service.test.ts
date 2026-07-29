import {
  deepStrictEqual,
  equal,
  fail,
  ok,
} from "node:assert/strict";
import { test } from "node:test";

import { createReconciliationHttpServer } from "./reconciliation-http.js";
import {
  ReconciliationService,
  ServiceError,
  type Principal,
  type ReconciliationProcessor,
} from "./reconciliation-service.js";

const principal: Principal = {
  subject: "user-1",
  tenantId: "tenant-1",
  permissions: [
    "reconciliation.submit",
    "reconciliation.read",
    "reconciliation.retry",
    "reconciliation.admin",
  ],
};

function file(value: unknown): Uint8Array {
  return new TextEncoder().encode(JSON.stringify(value));
}

function request(body: Uint8Array, idempotencyKey = "request-1") {
  return {
    fileName: "transactions.json",
    file: body,
    reconciliationType: "daily",
    rulesetVersion: "v1",
    idempotencyKey,
  };
}

function expectServiceError(action: () => unknown, code: string): void {
  try {
    action();
    fail(`Expected ${code}.`);
  } catch (error: unknown) {
    ok(error instanceof ServiceError);
    equal(error.code, code);
  }
}

test("moves a valid file through queued, running work, and success", async () => {
  const service = new ReconciliationService();
  const submission = service.submit(
    request(
      file([
        { amount: 10, currency: "USD", account: "cash", date: "2026-01-01" },
        { amount: 5, currency: "USD", account: "cash", date: "2026-01-02" },
      ]),
    ),
    principal,
  );

  equal(submission.duplicate, false);
  equal(submission.job.status, "QUEUED");
  await service.waitForIdle();
  equal(service.getJob(submission.job.id, principal).status, "SUCCEEDED");
  deepStrictEqual(service.getResult(submission.job.id, principal), {
    transactionCount: 2,
    totals: [{ account: "cash", currency: "USD", total: 15 }],
  });
});

test("returns the same job for idempotent and content duplicate submissions", () => {
  const service = new ReconciliationService();
  const body = file([
    { amount: 1, currency: "PHP", account: "a", date: "2026-01-01" },
  ]);
  const first = service.submit(request(body, "key-1"), principal);
  const sameRequest = service.submit(request(body, "key-1"), principal);
  const sameContent = service.submit(request(body, "key-2"), principal);

  equal(sameRequest.duplicate, true);
  equal(sameContent.duplicate, true);
  equal(first.job.id, sameRequest.job.id);
  equal(first.job.id, sameContent.job.id);
});

test("rejects reuse of an idempotency key for different content", () => {
  const service = new ReconciliationService();
  service.submit(request(file([1]), "same-key"), principal);

  expectServiceError(
    () => service.submit(request(file([2]), "same-key"), principal),
    "IDEMPOTENCY_CONFLICT",
  );
});

test("fails invalid files permanently without retrying", async () => {
  const service = new ReconciliationService({ retryDelayMs: 0 });
  const submission = service.submit(request(file([{ amount: "bad" }])), principal);

  await service.waitForIdle();
  const job = service.getJob(submission.job.id, principal);
  equal(job.status, "FAILED");
  equal(job.attempts, 1);
  equal(job.error?.code, "INVALID_TRANSACTIONS");
  equal(job.error?.retryable, false);
});

test("retries transient failures and then succeeds", async () => {
  let calls = 0;
  const processor: ReconciliationProcessor = async () => {
    calls += 1;
    if (calls === 1) {
      throw new Error("temporary");
    }
    return { transactionCount: 0, totals: [] };
  };
  const service = new ReconciliationService({
    processor,
    maxAttempts: 2,
    retryDelayMs: 0,
  });
  const submission = service.submit(request(file({})), principal);

  await service.waitForIdle();
  const job = service.getJob(submission.job.id, principal);
  equal(job.status, "SUCCEEDED");
  equal(job.attempts, 2);
  equal(service.getMetrics(principal).retries, 1);
});

test("enforces permissions and tenant isolation", () => {
  const service = new ReconciliationService();
  const reader: Principal = {
    subject: "reader",
    tenantId: "tenant-1",
    permissions: ["reconciliation.read"],
  };
  expectServiceError(
    () => service.submit(request(file([])), reader),
    "FORBIDDEN",
  );

  const submission = service.submit(request(file([])), principal);
  const otherTenant: Principal = {
    subject: "other",
    tenantId: "tenant-2",
    permissions: ["reconciliation.read"],
  };
  expectServiceError(
    () => service.getJob(submission.job.id, otherTenant),
    "NOT_FOUND",
  );
});

test("HTTP API authenticates, accepts a file, and exposes status and result", async () => {
  const service = new ReconciliationService();
  const server = createReconciliationHttpServer(service, async (authorization) =>
    authorization === "Bearer test-token" ? principal : undefined,
  );
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });

  try {
    const address = server.address();
    if (address === null || typeof address === "string") {
      fail("Expected a TCP server address.");
    }
    const base = `http://127.0.0.1:${address.port}`;
    const unauthorized = await fetch(`${base}/v1/reconciliations/missing`);
    equal(unauthorized.status, 401);

    const submitted = await fetch(`${base}/v1/reconciliations`, {
      method: "POST",
      headers: {
        authorization: "Bearer test-token",
        "content-type": "application/json",
        "idempotency-key": "http-1",
      },
      body: JSON.stringify([
        { amount: 8, currency: "EUR", account: "bank", date: "2026-01-01" },
      ]),
    });
    equal(submitted.status, 202);
    const location = submitted.headers.get("location");
    ok(location !== null);

    await service.waitForIdle();
    const status = await fetch(`${base}${location}`, {
      headers: { authorization: "Bearer test-token" },
    });
    equal(status.status, 200);
    const result = await fetch(`${base}${location}/result`, {
      headers: { authorization: "Bearer test-token" },
    });
    equal(result.status, 200);
    const resultBody: unknown = await result.json();
    deepStrictEqual(resultBody, {
      transactionCount: 1,
      totals: [{ account: "bank", currency: "EUR", total: 8 }],
    });
  } finally {
    await new Promise<void>((resolve, reject) => {
      server.close((error) => (error === undefined ? resolve() : reject(error)));
    });
  }
});
