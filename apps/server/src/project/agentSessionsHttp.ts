import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  EnvironmentHttpApi,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";

import { annotateEnvironmentRequest, requireEnvironmentScope } from "../auth/http.ts";
import { traceLocalHandlerWork } from "../cloud/traceRelayRequest.ts";
import * as AgentSessionImporter from "./AgentSessionImporter.ts";

/**
 * Moves native sessions between environments. A client reads the bundle from
 * the environment that has the session and hands it to the one that should
 * continue it; the environments never talk to each other directly.
 */
export const layer = HttpApiBuilder.group(
  EnvironmentHttpApi,
  "agentSessions",
  Effect.fnUntraced(function* (handlers) {
    const importer = yield* AgentSessionImporter.AgentSessionImporter;
    return handlers
      .handle(
        "bundle",
        Effect.fn("environment.agentSessions.bundle")(function* (args) {
          yield* annotateEnvironmentRequest(args.endpoint.name);
          yield* requireEnvironmentScope(AuthOrchestrationReadScope);
          return yield* importer.exportBundle(args.params).pipe(traceLocalHandlerWork);
        }),
      )
      .handle(
        "receive",
        Effect.fn("environment.agentSessions.receive")(function* (args) {
          yield* annotateEnvironmentRequest(args.endpoint.name);
          yield* requireEnvironmentScope(AuthOrchestrationOperateScope);
          return yield* importer.receiveSession(args.payload).pipe(traceLocalHandlerWork);
        }),
      );
  }),
);
