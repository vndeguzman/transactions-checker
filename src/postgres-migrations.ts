import { readFile } from "node:fs/promises";

import type { Pool } from "pg";

export async function runPostgresMigrations(pool: Pool): Promise<void> {
  const sql = await readFile(
    new URL("../migrations/001_reconciliation.sql", import.meta.url),
    "utf8",
  );
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await client.query("SELECT pg_advisory_xact_lock(8675309)");
    await client.query(sql);
    await client.query("COMMIT");
  } catch (error: unknown) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}
