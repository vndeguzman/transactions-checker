import { timingSafeEqual } from "node:crypto";

import { Pool } from "pg";

import { runPostgresMigrations } from "./postgres-migrations.js";
import { PostgresReconciliationService } from "./postgres-reconciliation-service.js";
import { createReconciliationHttpServer } from "./reconciliation-http.js";
import { type Permission } from "./reconciliation-service.js";

const apiToken = process.env.API_TOKEN;
if (apiToken === undefined || apiToken.length < 16) {
  throw new Error("API_TOKEN must contain at least 16 characters.");
}
const databaseUrl = process.env.DATABASE_URL;
if (databaseUrl === undefined) {
  throw new Error("DATABASE_URL is required.");
}

const expectedAuthorization = Buffer.from(`Bearer ${apiToken}`);
const tenantId = process.env.TENANT_ID ?? "local";
const permissions: readonly Permission[] = [
  "reconciliation.submit",
  "reconciliation.read",
  "reconciliation.retry",
  "reconciliation.admin",
];
const pool = new Pool({ connectionString: databaseUrl });
await runPostgresMigrations(pool);
const service = new PostgresReconciliationService(pool, {
  logger: (event) => console.log(JSON.stringify(event)),
  onWorkerError: (error) => console.error("worker error", error),
});
service.start();
const server = createReconciliationHttpServer(service, async (authorization) => {
  if (authorization === undefined) {
    return undefined;
  }
  const supplied = Buffer.from(authorization);
  if (
    supplied.byteLength !== expectedAuthorization.byteLength ||
    !timingSafeEqual(supplied, expectedAuthorization)
  ) {
    return undefined;
  }
  return { subject: "api-token", tenantId, permissions };
});

const portText = process.env.PORT ?? "3000";
const port = Number.parseInt(portText, 10);
if (!Number.isInteger(port) || port < 1 || port > 65_535) {
  throw new Error("PORT must be an integer from 1 to 65535.");
}
server.listen(port, () => console.log(`reconciliation service listening on ${port}`));

let stopping = false;
async function stop(): Promise<void> {
  if (stopping) {
    return;
  }
  stopping = true;
  await new Promise<void>((resolve, reject) => {
    server.close((error) => (error === undefined ? resolve() : reject(error)));
  });
  await service.stop();
  await pool.end();
}

process.on("SIGINT", () => void stop());
process.on("SIGTERM", () => void stop());
