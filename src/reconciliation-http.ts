import { createServer, type IncomingMessage, type ServerResponse } from "node:http";

import {
  ReconciliationService,
  ServiceError,
  type Principal,
} from "./reconciliation-service.js";

export type Authenticator = (
  authorizationHeader: string | undefined,
) => Promise<Principal | undefined>;

function header(value: string | readonly string[] | undefined): string | undefined {
  return typeof value === "string" ? value : undefined;
}

function sendJson(response: ServerResponse, status: number, body: unknown): void {
  response.writeHead(status, { "content-type": "application/json" });
  response.end(JSON.stringify(body));
}

async function readBody(request: IncomingMessage): Promise<Uint8Array> {
  const chunks: Uint8Array[] = [];
  let size = 0;
  for await (const rawChunk of request) {
    const chunk: unknown = rawChunk;
    if (!(chunk instanceof Uint8Array)) {
      throw new ServiceError(400, "INVALID_BODY", "Request body must be bytes.");
    }
    size += chunk.byteLength;
    if (size > 6 * 1024 * 1024) {
      throw new ServiceError(413, "FILE_TOO_LARGE", "Request body is too large.");
    }
    chunks.push(chunk);
  }
  return new Uint8Array(Buffer.concat(chunks));
}

export function createReconciliationHttpServer(
  service: ReconciliationService,
  authenticate: Authenticator,
) {
  return createServer(async (request, response) => {
    try {
      const url = new URL(request.url ?? "/", "http://localhost");
      if (request.method === "GET" && url.pathname === "/healthz") {
        sendJson(response, 200, { ok: true });
        return;
      }

      const principal = await authenticate(header(request.headers.authorization));
      if (principal === undefined) {
        throw new ServiceError(401, "UNAUTHORIZED", "Authentication required.");
      }

      if (request.method === "POST" && url.pathname === "/v1/reconciliations") {
        const idempotencyKey = header(request.headers["idempotency-key"]);
        if (idempotencyKey === undefined) {
          throw new ServiceError(
            400,
            "INVALID_REQUEST",
            "Idempotency-Key is required.",
          );
        }
        const submission = service.submit(
          {
            fileName: header(request.headers["x-file-name"]) ?? "reconciliation.json",
            file: await readBody(request),
            reconciliationType:
              header(request.headers["x-reconciliation-type"]) ?? "default",
            rulesetVersion:
              header(request.headers["x-ruleset-version"]) ?? "v1",
            idempotencyKey,
          },
          principal,
        );
        response.setHeader(
          "location",
          `/v1/reconciliations/${submission.job.id}`,
        );
        sendJson(response, submission.duplicate ? 200 : 202, submission);
        return;
      }

      if (request.method === "GET" && url.pathname === "/metrics") {
        sendJson(response, 200, service.getMetrics(principal));
        return;
      }

      const match = /^\/v1\/reconciliations\/([^/]+)(?:\/(result|retry))?$/.exec(
        url.pathname,
      );
      const encodedId = match?.[1];
      const action = match?.[2];
      if (encodedId !== undefined) {
        const id = decodeURIComponent(encodedId);
        if (request.method === "GET" && action === undefined) {
          sendJson(response, 200, service.getJob(id, principal));
          return;
        }
        if (request.method === "GET" && action === "result") {
          sendJson(response, 200, service.getResult(id, principal));
          return;
        }
        if (request.method === "POST" && action === "retry") {
          sendJson(response, 202, service.retry(id, principal));
          return;
        }
      }

      throw new ServiceError(404, "NOT_FOUND", "Route was not found.");
    } catch (error: unknown) {
      if (error instanceof ServiceError) {
        sendJson(response, error.statusCode, {
          error: { code: error.code, message: error.message },
        });
      } else {
        sendJson(response, 500, {
          error: { code: "INTERNAL_ERROR", message: "Internal server error." },
        });
      }
    }
  });
}
