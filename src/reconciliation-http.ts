import { createServer } from "node:http";

import express, {
  type ErrorRequestHandler,
  type Express,
  type NextFunction,
  type Request,
  type RequestHandler,
  type Response,
} from "express";

import {
  ServiceError,
  type Principal,
  type ReconciliationApi,
} from "./reconciliation-service.js";

export type Authenticator = (
  authorizationHeader: string | undefined,
) => Promise<Principal | undefined>;

type AsyncRoute = (
  request: Request,
  response: Response,
) => Promise<void> | void;

function asyncRoute(handler: AsyncRoute): RequestHandler {
  return (request, response, next) => {
    void Promise.resolve(handler(request, response)).catch(next);
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function rawFile(body: unknown): Uint8Array {
  if (!(body instanceof Uint8Array)) {
    throw new ServiceError(
      400,
      "INVALID_BODY",
      "Send the file as an application/json request body.",
    );
  }
  return new Uint8Array(body);
}

function routeId(request: Request): string {
  const id = request.params.id;
  if (typeof id !== "string") {
    throw new ServiceError(400, "INVALID_REQUEST", "Job ID is required.");
  }
  return id;
}

export function createReconciliationApp(
  service: ReconciliationApi,
  authenticate: Authenticator,
): Express {
  const app = express();
  const principals = new WeakMap<Request, Principal>();
  app.disable("x-powered-by");

  app.get("/healthz", (_request, response) => {
    response.status(200).json({ ok: true });
  });

  const requireAuthentication: RequestHandler = (
    request,
    response,
    next,
  ) => {
    void authenticate(request.get("authorization")).then(
      (principal) => {
        if (principal === undefined) {
          response.status(401).json({
            error: {
              code: "UNAUTHORIZED",
              message: "Authentication required.",
            },
          });
          return;
        }
        principals.set(request, principal);
        next();
      },
      next,
    );
  };
  app.use(requireAuthentication);

  function principalFor(request: Request): Principal {
    const principal = principals.get(request);
    if (principal === undefined) {
      throw new ServiceError(401, "UNAUTHORIZED", "Authentication required.");
    }
    return principal;
  }

  app.post(
    "/v1/reconciliations",
    express.raw({ type: "application/json", limit: "6mb" }),
    asyncRoute(async (request, response) => {
      const idempotencyKey = request.get("idempotency-key");
      if (idempotencyKey === undefined) {
        throw new ServiceError(
          400,
          "INVALID_REQUEST",
          "Idempotency-Key is required.",
        );
      }
      const submission = await service.submit(
        {
          fileName: request.get("x-file-name") ?? "reconciliation.json",
          file: rawFile(request.body),
          reconciliationType:
            request.get("x-reconciliation-type") ?? "default",
          rulesetVersion: request.get("x-ruleset-version") ?? "v1",
          idempotencyKey,
        },
        principalFor(request),
      );
      response
        .location(`/v1/reconciliations/${submission.job.id}`)
        .status(submission.duplicate ? 200 : 202)
        .json(submission);
    }),
  );

  app.get(
    "/v1/reconciliations/:id",
    asyncRoute(async (request, response) => {
      response
        .status(200)
        .json(await service.getJob(routeId(request), principalFor(request)));
    }),
  );

  app.get(
    "/v1/reconciliations/:id/result",
    asyncRoute(async (request, response) => {
      response
        .status(200)
        .json(await service.getResult(routeId(request), principalFor(request)));
    }),
  );

  app.post(
    "/v1/reconciliations/:id/retry",
    asyncRoute(async (request, response) => {
      response
        .status(202)
        .json(await service.retry(routeId(request), principalFor(request)));
    }),
  );

  app.get(
    "/metrics",
    asyncRoute(async (request, response) => {
      response
        .status(200)
        .json(await service.getMetrics(principalFor(request)));
    }),
  );

  app.use((_request, _response, next: NextFunction) => {
    next(new ServiceError(404, "NOT_FOUND", "Route was not found."));
  });

  const errorHandler: ErrorRequestHandler = (
    error: unknown,
    _request,
    response,
    _next,
  ) => {
    if (error instanceof ServiceError) {
      response.status(error.statusCode).json({
        error: { code: error.code, message: error.message },
      });
      return;
    }
    if (isRecord(error) && error.status === 413) {
      response.status(413).json({
        error: { code: "FILE_TOO_LARGE", message: "Request body is too large." },
      });
      return;
    }
    response.status(500).json({
      error: { code: "INTERNAL_ERROR", message: "Internal server error." },
    });
  };
  app.use(errorHandler);

  return app;
}

export function createReconciliationHttpServer(
  service: ReconciliationApi,
  authenticate: Authenticator,
) {
  return createServer(createReconciliationApp(service, authenticate));
}
