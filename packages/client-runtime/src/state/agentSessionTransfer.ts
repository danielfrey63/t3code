import type { AgentSessionBundleParams, AgentSessionReceiveInput } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as SubscriptionRef from "effect/SubscriptionRef";
import type { HttpClient } from "effect/http";
import type { Atom } from "effect/reactivity";

import * as RemoteEnvironmentAuthorization from "../authorization/service.ts";
import type { EnvironmentRegistry } from "../connection/registry.ts";
import * as EnvironmentSupervisor from "../connection/supervisor.ts";
import * as ManagedRelayDpopSigner from "../relay/managedRelay.ts";
import { EnvironmentRpcUnavailableError } from "../rpc/client.ts";
import { executeAuthenticatedEnvironmentHttpRequest } from "./environmentHttpAuth.ts";
import { createEnvironmentCommand } from "./runtime.ts";

/** Session transcripts can be tens of MiB; give slow tunnels time to move them. */
const AGENT_SESSION_TRANSFER_TIMEOUT_MS = 5 * 60_000;

const withEnvironmentHttp = <A, E, R>(
  run: (connection: {
    readonly prepared: Parameters<typeof executeAuthenticatedEnvironmentHttpRequest>[0]["prepared"];
    readonly signer: Option.Option<ManagedRelayDpopSigner.ManagedRelayDpopSigner["Service"]>;
    readonly remoteAuthorization: Option.Option<
      RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization["Service"]
    >;
  }) => Effect.Effect<A, E, R>,
) =>
  Effect.gen(function* () {
    const supervisor = yield* EnvironmentSupervisor.EnvironmentSupervisor;
    const prepared = yield* SubscriptionRef.get(supervisor.prepared);
    if (Option.isNone(prepared)) {
      return yield* new EnvironmentRpcUnavailableError({
        environmentId: supervisor.target.environmentId,
        message: "The environment is not connected.",
      });
    }
    return yield* run({
      prepared: prepared.value,
      signer: yield* Effect.serviceOption(ManagedRelayDpopSigner.ManagedRelayDpopSigner),
      remoteAuthorization: yield* Effect.serviceOption(
        RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization,
      ),
    });
  });

/**
 * Moving a native session between environments: read its bundle from the
 * environment that has it, then hand the bundle to the one that continues it.
 * Run each command against its own environment.
 */
export function createAgentSessionTransferAtoms<R, E>(
  runtime: Atom.AtomRuntime<EnvironmentRegistry | HttpClient.HttpClient | R, E>,
) {
  return {
    bundle: createEnvironmentCommand(runtime, {
      label: "environment-data:agent-sessions:bundle",
      execute: (params: AgentSessionBundleParams) =>
        withEnvironmentHttp((connection) =>
          executeAuthenticatedEnvironmentHttpRequest({
            ...connection,
            group: "agentSessions",
            method: "GET",
            url: (urls) => urls.bundle({ params }),
            timeoutMs: AGENT_SESSION_TRANSFER_TIMEOUT_MS,
            request: ({ client, headers }) => client.bundle({ params, headers }),
          }),
        ),
    }),
    receive: createEnvironmentCommand(runtime, {
      label: "environment-data:agent-sessions:receive",
      execute: (payload: AgentSessionReceiveInput) =>
        withEnvironmentHttp((connection) =>
          executeAuthenticatedEnvironmentHttpRequest({
            ...connection,
            group: "agentSessions",
            method: "POST",
            url: (urls) => urls.receive(),
            timeoutMs: AGENT_SESSION_TRANSFER_TIMEOUT_MS,
            request: ({ client, headers }) => client.receive({ payload, headers }),
          }),
        ),
    }),
  };
}
