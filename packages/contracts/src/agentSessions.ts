import * as Schema from "effect/Schema";
import {
  IsoDateTime,
  NonNegativeInt,
  ProjectId,
  ThreadId,
  TrimmedNonEmptyString,
} from "./baseSchemas.ts";
import { ProviderInstanceId } from "./providerInstance.ts";

/** Coding agent home directories the scanner knows how to read. */
export const AgentSessionSource = Schema.Literals(["claudeAgent", "codex"]);
export type AgentSessionSource = typeof AgentSessionSource.Type;

/** File identity saved with an imported session so bounded retries can skip unchanged history. */
export const AgentSessionImportSource = Schema.Struct({
  provider: AgentSessionSource,
  providerInstanceId: ProviderInstanceId,
  providerSessionId: TrimmedNonEmptyString,
  filePath: TrimmedNonEmptyString,
  size: NonNegativeInt,
  mtimeMs: Schema.NullOr(Schema.Number),
  device: Schema.Number,
  inode: Schema.NullOr(Schema.Number),
  birthtimeMs: Schema.NullOr(Schema.Number),
  /**
   * Claude compact lineage, so an unchanged transcript can still learn that a
   * newer file continues it after compaction: its last record ids, and the
   * records in other files that it continues from.
   */
  lineage: Schema.optional(
    Schema.Struct({
      tailUuids: Schema.Array(Schema.String),
      continuesFrom: Schema.Array(Schema.String),
    }),
  ),
});
export type AgentSessionImportSource = typeof AgentSessionImportSource.Type;

/**
 * Empty for now. Kept as a struct so future scan options (source filters,
 * explicit roots) can be added without a new method.
 */
export const AgentSessionScanInput = Schema.Struct({});
export type AgentSessionScanInput = typeof AgentSessionScanInput.Type;

/**
 * A directory that at least one agent CLI has run in, suitable for import as a
 * T3 Code project. `alreadyImported` marks candidates that already have an
 * active project rooted at the same path.
 */
/**
 * Git identity of a candidate directory, read from `.git/config` without
 * spawning git. `remoteKey` is the normalized origin URL, shared by every
 * clone of the same repository so the client can group them. `repository`
 * is the GitHub `owner/name` when the origin is on GitHub.
 */
export const AgentSessionProjectGit = Schema.Struct({
  remoteKey: Schema.NullOr(Schema.String),
  repository: Schema.NullOr(Schema.String),
});
export type AgentSessionProjectGit = typeof AgentSessionProjectGit.Type;

export const AgentSessionProjectCandidate = Schema.Struct({
  path: TrimmedNonEmptyString,
  title: TrimmedNonEmptyString,
  projectId: Schema.optional(ProjectId),
  sources: Schema.Array(AgentSessionSource),
  threadCount: NonNegativeInt,
  lastActiveAt: Schema.NullOr(IsoDateTime),
  alreadyImported: Schema.Boolean,
  /**
   * `null` when the directory is not the root of a git repository. Missing on
   * servers that predate the git scan, where the client cannot tell repositories
   * from plain folders and should treat every candidate as a standalone project.
   */
  git: Schema.optionalKey(Schema.NullOr(AgentSessionProjectGit)),
});
export type AgentSessionProjectCandidate = typeof AgentSessionProjectCandidate.Type;

export const AgentSessionScanResult = Schema.Struct({
  candidates: Schema.Array(AgentSessionProjectCandidate),
  scannedAt: IsoDateTime,
  truncated: Schema.optional(Schema.Boolean),
});
export type AgentSessionScanResult = typeof AgentSessionScanResult.Type;

export const AgentSessionImportInput = Schema.Struct({
  projectId: ProjectId,
  expectedWorkspaceRoot: Schema.optional(TrimmedNonEmptyString),
});
export type AgentSessionImportInput = typeof AgentSessionImportInput.Type;

export class AgentSessionImportProjectNotFoundError extends Schema.TaggedError<AgentSessionImportProjectNotFoundError>()(
  "AgentSessionImportProjectNotFoundError",
  { projectId: ProjectId },
) {
  override get message(): string {
    return `Project '${this.projectId}' does not exist.`;
  }
}

export class AgentSessionImportProjectChangedError extends Schema.TaggedError<AgentSessionImportProjectChangedError>()(
  "AgentSessionImportProjectChangedError",
  { projectId: ProjectId },
) {
  override get message(): string {
    return `Project '${this.projectId}' changed directories. Scan for projects again before importing history.`;
  }
}

export const AgentSessionImportResult = Schema.Struct({
  importedCount: NonNegativeInt,
  skippedCount: NonNegativeInt,
});
export type AgentSessionImportResult = typeof AgentSessionImportResult.Type;

export class AgentSessionScanError extends Schema.TaggedError<AgentSessionScanError>()(
  "AgentSessionScanError",
  {
    operation: Schema.Literals(["read-settings", "read-projects"]),
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Failed to scan agent sessions during ${this.operation}.`;
  }
}

/** Native sessions an environment can continue in one of its projects. */
export const AgentSessionListInput = Schema.Struct({ projectId: ProjectId });
export type AgentSessionListInput = typeof AgentSessionListInput.Type;

export const AgentSessionSummary = Schema.Struct({
  provider: AgentSessionSource,
  providerInstanceId: ProviderInstanceId,
  providerSessionId: TrimmedNonEmptyString,
  title: TrimmedNonEmptyString,
  updatedAt: IsoDateTime,
  /** The thread already bound to this session on the listing environment. */
  threadId: Schema.NullOr(ThreadId),
});
export type AgentSessionSummary = typeof AgentSessionSummary.Type;

export const AgentSessionListResult = Schema.Struct({
  sessions: Schema.Array(AgentSessionSummary),
});
export type AgentSessionListResult = typeof AgentSessionListResult.Type;

/** Continue one local native session as a thread, importing it when needed. */
export const AgentSessionOpenInput = Schema.Struct({
  projectId: ProjectId,
  providerInstanceId: ProviderInstanceId,
  providerSessionId: TrimmedNonEmptyString,
});
export type AgentSessionOpenInput = typeof AgentSessionOpenInput.Type;

export const AgentSessionOpenResult = Schema.Struct({ threadId: ThreadId });
export type AgentSessionOpenResult = typeof AgentSessionOpenResult.Type;

/**
 * A Claude session as Claude keeps it: the transcript and its sidecar
 * directory (`tool-results/`, `subagents/`), byte for byte. Paths are relative
 * to the session's project directory.
 */
export const AgentSessionBundle = Schema.Struct({
  provider: Schema.Literal("claudeAgent"),
  providerSessionId: TrimmedNonEmptyString,
  title: TrimmedNonEmptyString,
  files: Schema.Array(
    Schema.Struct({
      path: TrimmedNonEmptyString,
      contentBase64: Schema.String,
    }),
  ),
});
export type AgentSessionBundle = typeof AgentSessionBundle.Type;

export const AgentSessionBundleParams = Schema.Struct({
  providerInstanceId: ProviderInstanceId,
  providerSessionId: TrimmedNonEmptyString,
});
export type AgentSessionBundleParams = typeof AgentSessionBundleParams.Type;

/**
 * Continue a session from another environment. A Claude target receives a
 * 1:1 copy it resumes natively; any other target gets the conversation as
 * imported history, injected into its first native turn.
 */
export const AgentSessionReceiveInput = Schema.Struct({
  projectId: ProjectId,
  targetProviderInstanceId: ProviderInstanceId,
  bundle: AgentSessionBundle,
});
export type AgentSessionReceiveInput = typeof AgentSessionReceiveInput.Type;

export class AgentSessionTransferError extends Schema.TaggedError<AgentSessionTransferError>()(
  "AgentSessionTransferError",
  {
    reason: Schema.Literals([
      "session-not-found",
      "project-not-found",
      "unsupported-target",
      "invalid-bundle",
      "too-large",
      "diverged",
      "write-failed",
    ]),
    detail: Schema.optional(Schema.String),
  },
  { httpApiStatus: 422 },
) {
  override get message(): string {
    switch (this.reason) {
      case "session-not-found":
        return "The session is no longer available on its environment.";
      case "project-not-found":
        return "The target project does not exist.";
      case "unsupported-target":
        return "This provider cannot continue the session.";
      case "invalid-bundle":
        return "The session could not be read.";
      case "too-large":
        return "The session is too large to transfer.";
      case "diverged":
        return "The session was continued separately on both environments.";
      case "write-failed":
        return "The session could not be saved on this environment.";
    }
  }
}
