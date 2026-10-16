import type {
  AgentSessionSummary,
  EnvironmentId,
  ProjectId,
  ProviderDriverKind,
  ThreadId,
} from "@t3tools/contracts";

/** Sessions one environment listed for its copy of the project. */
export interface AgentSessionPickerSource {
  readonly environmentId: EnvironmentId;
  readonly environmentLabel: string;
  readonly projectId: ProjectId;
  readonly sessions: ReadonlyArray<AgentSessionSummary>;
}

/**
 * What continuing a listed session takes on the environment the user is in:
 * open its thread, import the local session, or transfer it from elsewhere.
 */
export type AgentSessionPickerAction =
  | { readonly _tag: "OpenThread"; readonly threadId: ThreadId }
  | { readonly _tag: "OpenLocal" }
  | {
      readonly _tag: "Transfer";
      /** A Claude target resumes a 1:1 copy; any other provider continues from the conversation. */
      readonly mode: "copy" | "transform";
    };

export interface AgentSessionPickerRow {
  readonly key: string;
  readonly source: AgentSessionPickerSource;
  readonly session: AgentSessionSummary;
  readonly action: AgentSessionPickerAction;
}

/**
 * Newest first. A session present here and elsewhere, such as one already
 * copied, is offered once, from here. Only Claude sessions can travel.
 */
export function agentSessionPickerRows(input: {
  readonly currentEnvironmentId: EnvironmentId;
  readonly targetDriver: ProviderDriverKind;
  readonly sources: ReadonlyArray<AgentSessionPickerSource>;
}): ReadonlyArray<AgentSessionPickerRow> {
  const local = input.sources.filter(
    (source) => source.environmentId === input.currentEnvironmentId,
  );
  const rows: Array<AgentSessionPickerRow> = [];
  const seen = new Set<string>();
  for (const source of [...local, ...input.sources.filter((entry) => !local.includes(entry))]) {
    const isLocal = source.environmentId === input.currentEnvironmentId;
    for (const session of source.sessions) {
      if (seen.has(session.providerSessionId)) continue;
      if (!isLocal && session.provider !== "claudeAgent") continue;
      seen.add(session.providerSessionId);
      rows.push({
        key: `${source.environmentId}:${session.providerInstanceId}:${session.providerSessionId}`,
        source,
        session,
        action: isLocal
          ? session.threadId === null
            ? { _tag: "OpenLocal" }
            : { _tag: "OpenThread", threadId: session.threadId }
          : { _tag: "Transfer", mode: input.targetDriver === "claudeAgent" ? "copy" : "transform" },
      });
    }
  }
  return rows.sort((left, right) => right.session.updatedAt.localeCompare(left.session.updatedAt));
}
