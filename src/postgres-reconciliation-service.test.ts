import {
  deepStrictEqual,
  equal,
  fail,
} from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { test } from "node:test";

import { Pool } from "pg";

import { runPostgresMigrations } from "./postgres-migrations.js";
import { PostgresReconciliationService } from "./postgres-reconciliation-service.js";
import type { Principal } from "./reconciliation-service.js";

const databaseUrl = process.env.TEST_DATABASE_URL;

test(
  "persists and processes an idempotent job in PostgreSQL",
  { skip: databaseUrl === undefined },
  async () => {
    if (databaseUrl === undefined) {
      fail("TEST_DATABASE_URL is required for this test.");
    }
    const pool = new Pool({ connectionString: databaseUrl });
    const tenantId = `test-${randomUUID()}`;
    const principal: Principal = {
      subject: "integration-test",
      tenantId,
      permissions: [
        "reconciliation.submit",
        "reconciliation.read",
        "reconciliation.retry",
        "reconciliation.admin",
      ],
    };
    const service = new PostgresReconciliationService(pool, {
      pollIntervalMs: 10,
      retryDelayMs: 0,
    });

    try {
      await runPostgresMigrations(pool);
      service.start();
      const file = new TextEncoder().encode(
        JSON.stringify([
          {
            amount: 12,
            currency: "USD",
            account: "bank",
            date: "2026-07-29",
          },
        ]),
      );
      const request = {
        fileName: "transactions.json",
        file,
        reconciliationType: "daily",
        rulesetVersion: "v1",
        idempotencyKey: "integration-1",
      };
      const first = await service.submit(request, principal);
      const duplicate = await service.submit(request, principal);
      equal(first.job.id, duplicate.job.id);
      equal(duplicate.duplicate, true);

      const deadline = Date.now() + 5_000;
      let job = await service.getJob(first.job.id, principal);
      while (
        job.status !== "SUCCEEDED" &&
        job.status !== "FAILED" &&
        Date.now() < deadline
      ) {
        await new Promise<void>((resolve) => setTimeout(resolve, 20));
        job = await service.getJob(first.job.id, principal);
      }
      equal(job.status, "SUCCEEDED");
      deepStrictEqual(await service.getResult(first.job.id, principal), {
        transactionCount: 1,
        totals: [{ account: "bank", currency: "USD", total: 12 }],
      });

      await service.stop();
      const restarted = new PostgresReconciliationService(pool);
      equal(
        (await restarted.getJob(first.job.id, principal)).status,
        "SUCCEEDED",
      );
    } finally {
      await service.stop();
      await pool.query(
        "DELETE FROM reconciliation_idempotency WHERE tenant_id = $1",
        [tenantId],
      );
      await pool.query("DELETE FROM reconciliation_jobs WHERE tenant_id = $1", [
        tenantId,
      ]);
      await pool.end();
    }
  },
);
