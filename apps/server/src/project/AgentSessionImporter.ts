import {
  DEFAULT_MODEL,
  DEFAULT_MODEL_BY_PROVIDER,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  DEFAULT_RUNTIME_MODE,
  AgentSessionImportProjectChangedError,
  AgentSessionImportProjectNotFoundError,
  AgentSessionImportSource,
  AgentSessionScanError,
  AgentSessionSource,
  CommandId,
  EventId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ThreadId,
  TurnItemId,
  AgentSessionTransferError,
  type AgentSessionBundle,
  type AgentSessionBundleParams,
  type AgentSessionImportInput,
  type AgentSessionImportResult,
  type AgentSessionListInput,
  type AgentSessionListResult,
  type AgentSessionOpenInput,
  type AgentSessionOpenResult,
  type AgentSessionReceiveInput,
  type AgentSessionSummary,
  type ProviderInstanceId,
  type OrchestrationV2AppThread,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import { normalizeProjectPathForComparison } from "@t3tools/shared/path";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as ProviderSessionRuntime from "../persistence/ProviderSessionRuntime.ts";
import { claudeProjectDirectoryName } from "../provider/Drivers/ClaudeHome.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as AgentSessionScanner from "./AgentSessionScanner.ts";
import * as ProjectService from "./ProjectService.ts";

const IMPORT_EVENT_PREFIX = "agent-session-import:v2";
const CLAUDE_SESSION_ID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const decodeImportedTranscriptPayload = Schema.decodeUnknownOption(
  Schema.Struct({
    cwd: Schema.optional(Schema.String),
    importedTranscripts: Schema.optional(Schema.Array(AgentSessionImportSource)),
  }),
);

class AgentSessionUnresumableSessionError extends Schema.TaggedError<AgentSessionUnresumableSessionError>()(
  "AgentSessionUnresumableSessionError",
  {
    source: AgentSessionSource,
    providerSessionId: Schema.String,
  },
) {
  override get message(): string {
    return `Session '${this.providerSessionId}' from '${this.source}' cannot be resumed.`;
  }
}

class AgentSessionThreadProjectConflictError extends Schema.TaggedError<AgentSessionThreadProjectConflictError>()(
  "AgentSessionThreadProjectConflictError",
  {
    threadId: ThreadId,
    expectedProjectId: ProjectId,
    actualProjectId: ProjectId,
  },
) {
  override get message(): string {
    return `Imported thread '${this.threadId}' belongs to project '${this.actualProjectId}', not '${this.expectedProjectId}'.`;
  }
}

class AgentSessionThreadModifiedError extends Schema.TaggedError<AgentSessionThreadModifiedError>()(
  "AgentSessionThreadModifiedError",
  { threadId: ThreadId },
) {
  override get message(): string {
    return `Imported thread '${this.threadId}' already contains non-imported activity.`;
  }
}

/** Transferred history feeds a model window, not a sidebar preview, so keep more of it. */
const MAX_TRANSFERRED_MESSAGES = 2_000;

/** Thread id of an imported native session, shared by every import path. */
function importedThreadId(source: AgentSessionImportSource): ThreadId {
  return ThreadId.make(`import:${source.providerInstanceId}:${source.providerSessionId}`);
}

/** Bundle paths stay inside the session: its transcript or its sidecar directory. */
function isBundleSessionPath(filePath: string, sessionId: string): boolean {
  const segments = filePath.split("/");
  return (
    segments.every((segment) => segment !== "" && segment !== "." && segment !== "..") &&
    !filePath.includes("\\") &&
    (filePath === `${sessionId}.jsonl` || (segments[0] === sessionId && segments.length > 1))
  );
}

function startsWithBytes(bytes: Uint8Array, prefix: Uint8Array): boolean {
  if (prefix.byteLength > bytes.byteLength) return false;
  for (let index = 0; index < prefix.byteLength; index++) {
    if (bytes[index] !== prefix[index]) return false;
  }
  return true;
}

function dateTime(value: string): DateTime.Utc {
  return DateTime.makeUnsafe(value);
}

function messageEvents(input: {
  readonly threadId: ThreadId;
  readonly index: number;
  readonly message: AgentSessionScanner.AgentSessionThreadMessage;
}): ReadonlyArray<OrchestrationV2DomainEvent> {
  const ordinal = input.index + 1;
  const suffix = String(input.index).padStart(6, "0");
  const messageId = MessageId.make(`${input.threadId}:${suffix}`);
  const turnItemId = TurnItemId.make(
    `${IMPORT_EVENT_PREFIX}:turn-item:${input.threadId}:${suffix}`,
  );
  const at = dateTime(input.message.createdAt);
  const message: OrchestrationV2ConversationMessage = {
    createdBy: input.message.role === "user" ? "user" : "agent",
    creationSource: "server",
    id: messageId,
    threadId: input.threadId,
    runId: null,
    nodeId: null,
    role: input.message.role,
    text: input.message.text,
    attachments: [],
    streaming: false,
    createdAt: at,
    updatedAt: at,
  };
  const common = {
    id: turnItemId,
    threadId: input.threadId,
    runId: null,
    nodeId: null,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal,
    status: "completed" as const,
    title: null,
    startedAt: at,
    completedAt: at,
    updatedAt: at,
  };
  const turnItem: OrchestrationV2TurnItem =
    input.message.role === "user"
      ? {
          ...common,
          createdBy: "user",
          creationSource: "server",
          type: "user_message",
          messageId,
          inputIntent: "turn_start",
          text: input.message.text,
          attachments: [],
        }
      : {
          ...common,
          type: "assistant_message",
          messageId,
          text: input.message.text,
          streaming: false,
        };
  return [
    {
      id: EventId.make(`${IMPORT_EVENT_PREFIX}:message:${input.threadId}:${suffix}`),
      type: "message.updated",
      threadId: input.threadId,
      occurredAt: at,
      payload: message,
    },
    {
      id: EventId.make(`${IMPORT_EVENT_PREFIX}:turn-item:${input.threadId}:${suffix}`),
      type: "turn-item.updated",
      threadId: input.threadId,
      occurredAt: at,
      payload: turnItem,
    },
  ];
}

const make = Effect.gen(function* () {
  const scanner = yield* AgentSessionScanner.AgentSessionScanner;
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const projects = yield* ProjectService.ProjectService;
  const eventSink = yield* EventSink.EventSinkV2;
  const idAllocator = yield* IdAllocator.IdAllocatorV2;
  const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
  const serverSettings = yield* ServerSettings.ServerSettingsService;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const syncImportedTitle = (input: {
    readonly threadId: ThreadId;
    readonly currentTitle: string;
    readonly title: string;
    readonly source: AgentSessionImportSource;
  }) =>
    input.title.trim() === "" || input.title === input.currentTitle
      ? Effect.void
      : orchestrator
          .dispatch({
            type: "thread.metadata.update",
            commandId: CommandId.make(
              `${IMPORT_EVENT_PREFIX}:title:${input.threadId}:${input.source.size}:${input.source.mtimeMs ?? 0}`,
            ),
            threadId: input.threadId,
            title: input.title,
          })
          .pipe(
            Effect.asVoid,
            Effect.catch((cause) =>
              Effect.logWarning("Could not update an imported thread title", {
                threadId: input.threadId,
                cause,
              }),
            ),
          );
  /**
   * Create (or refresh) the thread for one parsed session. With `target`, the
   * thread runs on that provider instance instead: no native session is bound,
   * so its first turn starts one and receives the history as imported context.
   */
  const importThread = Effect.fn("AgentSessionImporter.importThread")(function* (input: {
    readonly projectId: ProjectId;
    readonly workspaceRoot: string;
    readonly threadId: ThreadId;
    readonly thread: AgentSessionScanner.AgentSessionThread;
    readonly source: AgentSessionImportSource | null;
    readonly target?: {
      readonly providerInstanceId: ProviderInstanceId;
      readonly driver: ProviderDriverKind;
    };
  }) {
    const { thread, threadId, source, target } = input;
    if (
      target === undefined &&
      thread.source === "claudeAgent" &&
      !CLAUDE_SESSION_ID_PATTERN.test(thread.providerSessionId)
    ) {
      return yield* new AgentSessionUnresumableSessionError({
        source: thread.source,
        providerSessionId: thread.providerSessionId,
      });
    }
    const existing = yield* Effect.option(orchestrator.getThreadRecords(threadId, []));
    if (Option.isSome(existing)) {
      if (existing.value.thread.projectId !== input.projectId) {
        return yield* new AgentSessionThreadProjectConflictError({
          threadId,
          expectedProjectId: input.projectId,
          actualProjectId: existing.value.thread.projectId,
        });
      }
      if (existing.value.thread.historyOrigin !== "v1_import") {
        return yield* new AgentSessionThreadModifiedError({ threadId });
      }
      if (source !== null) {
        // A `/rename` in the CLI, or a compaction that continued in a newer
        // file, changes the name this transcript should be listed under.
        yield* syncImportedTitle({
          threadId,
          currentTitle: existing.value.thread.title,
          title: thread.title,
          source,
        });
        yield* runtimes.recordImportedTranscript({ threadId, source });
      }
      return threadId;
    }

    const driver = target?.driver ?? ProviderDriverKind.make(thread.source);
    const providerInstanceId = target?.providerInstanceId ?? thread.providerInstanceId;
    const model =
      (target === undefined ? thread.model : null) ??
      DEFAULT_MODEL_BY_PROVIDER[driver] ??
      DEFAULT_MODEL;
    const providerThreadId =
      target === undefined
        ? idAllocator.derive.providerThread({
            driver,
            nativeThreadId: thread.providerSessionId,
          })
        : null;
    const createdAt = dateTime(thread.createdAt);
    const updatedAt = dateTime(thread.updatedAt);
    const appThread: OrchestrationV2AppThread = {
      createdBy: "system",
      creationSource: "server",
      id: threadId,
      projectId: input.projectId,
      title: thread.title.trim() === "" ? "Untitled thread" : thread.title,
      providerInstanceId,
      modelSelection: { instanceId: providerInstanceId, model },
      runtimeMode: DEFAULT_RUNTIME_MODE,
      interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
      branch: null,
      worktreePath: null,
      linkedPullRequest: null,
      branchPullRequest: null,
      activeProviderThreadId: providerThreadId,
      historyOrigin: "v1_import",
      lineage: {
        parentThreadId: null,
        relationshipToParent: null,
        rootThreadId: threadId,
      },
      forkedFrom: null,
      createdAt,
      updatedAt,
      archivedAt: null,
      settledOverride: "settled",
      settledAt: updatedAt,
      unsettledAt: null,
      snoozedUntil: null,
      snoozedAt: null,
      pinnedAt: null,
      pinOrderKey: null,
      activeOrderKey: null,
      lastVisitedAt: null,
      deletedAt: null,
    };
    const messages = thread.messages.flatMap((message, index) =>
      messageEvents({ threadId, index, message }),
    );
    if (providerThreadId === null) {
      yield* eventSink.write({
        events: [
          {
            id: EventId.make(`${IMPORT_EVENT_PREFIX}:thread:${threadId}:created`),
            type: "thread.created",
            threadId,
            providerInstanceId,
            occurredAt: createdAt,
            payload: appThread,
          },
          ...messages,
        ],
      });
      return threadId;
    }
    const providerThread: OrchestrationV2ProviderThread = {
      id: providerThreadId,
      driver,
      providerInstanceId,
      providerSessionId: null,
      appThreadId: threadId,
      ownerNodeId: null,
      nativeThreadRef: {
        driver,
        nativeId: thread.providerSessionId,
        strength: "strong",
      },
      nativeConversationHeadRef: null,
      status: "idle",
      firstRunOrdinal: null,
      lastRunOrdinal: null,
      handoffIds: [],
      forkedFrom: null,
      pendingBackgroundTasks: [],
      createdAt,
      updatedAt,
    };

    yield* runtimes.upsert(
      {
        threadId,
        providerName: driver,
        providerInstanceId,
        adapterKey: driver,
        runtimeMode: DEFAULT_RUNTIME_MODE,
        status: "stopped",
        lastSeenAt: thread.updatedAt,
        resumeCursor:
          thread.source === "codex"
            ? { threadId: thread.providerSessionId }
            : { threadId, resume: thread.providerSessionId },
        runtimePayload: { cwd: input.workspaceRoot },
      },
      { onConflict: "ignore" },
    );
    yield* eventSink.write({
      events: [
        {
          id: EventId.make(`${IMPORT_EVENT_PREFIX}:thread:${threadId}:created`),
          type: "thread.created",
          threadId,
          providerInstanceId,
          occurredAt: createdAt,
          payload: appThread,
        },
        ...messages,
        {
          id: EventId.make(`${IMPORT_EVENT_PREFIX}:provider-thread:${providerThreadId}`),
          type: "provider-thread.updated",
          threadId,
          driver,
          providerInstanceId,
          occurredAt: updatedAt,
          payload: providerThread,
        },
      ],
    });
    if (source !== null) yield* runtimes.recordImportedTranscript({ threadId, source });
    return threadId;
  });

  const importRecentAgentThreads = Effect.fn("importRecentAgentThreadsV2")(function* (
    input: AgentSessionImportInput,
  ) {
    const project = yield* projects.getById(input.projectId).pipe(
      Effect.mapError((cause) => new AgentSessionScanError({ operation: "read-projects", cause })),
      Effect.flatMap(
        Option.match({
          onNone: () =>
            Effect.fail(new AgentSessionImportProjectNotFoundError({ projectId: input.projectId })),
          onSome: Effect.succeed,
        }),
      ),
    );
    if (
      input.expectedWorkspaceRoot !== undefined &&
      normalizeProjectPathForComparison(project.workspaceRoot) !==
        normalizeProjectPathForComparison(input.expectedWorkspaceRoot)
    ) {
      return yield* new AgentSessionImportProjectChangedError({ projectId: input.projectId });
    }
    const runtimeRows = yield* runtimes
      .list()
      .pipe(
        Effect.mapError(
          (cause) => new AgentSessionScanError({ operation: "read-projects", cause }),
        ),
      );
    const completedSources = runtimeRows.flatMap((runtime) => {
      const payload = decodeImportedTranscriptPayload(runtime.runtimePayload);
      if (
        Option.isNone(payload) ||
        payload.value.cwd === undefined ||
        normalizeProjectPathForComparison(payload.value.cwd) !==
          normalizeProjectPathForComparison(project.workspaceRoot)
      ) {
        return [];
      }
      return payload.value.importedTranscripts ?? [];
    });
    const outcomes = scanner.recentThreads(project.workspaceRoot, completedSources);
    const importedThreadIds = new Set<ThreadId>();
    let importedCount = 0;
    let skippedCount = 0;

    yield* Stream.runForEach(outcomes, (outcome) =>
      Effect.gen(function* () {
        if (outcome._tag === "Skipped") {
          skippedCount += 1;
          return;
        }
        const source = outcome.source;
        const threadId = importedThreadId(source);
        if (outcome._tag === "AlreadyImported") {
          importedThreadIds.add(threadId);
          importedCount += 1;
          if (outcome.continuedAfterCompact) {
            yield* Effect.option(orchestrator.getThreadRecords(threadId, [])).pipe(
              Effect.flatMap(
                Option.match({
                  onNone: () => Effect.void,
                  onSome: ({ thread }) =>
                    syncImportedTitle({
                      threadId,
                      currentTitle: thread.title,
                      title: AgentSessionScanner.withBeforeCompactSuffix(thread.title),
                      source,
                    }),
                }),
              ),
            );
          }
          return;
        }
        if (outcome._tag === "Duplicate") {
          if (importedThreadIds.has(threadId)) {
            yield* runtimes.recordImportedTranscript({ threadId, source }).pipe(Effect.ignore);
          }
          return;
        }

        const imported = yield* importThread({
          projectId: input.projectId,
          workspaceRoot: project.workspaceRoot,
          threadId,
          thread: outcome.thread,
          source,
        }).pipe(
          Effect.as(true),
          Effect.catch((cause) =>
            Effect.logWarning("Could not import an agent session", {
              provider: outcome.thread.source,
              sessionId: outcome.thread.providerSessionId,
              cause,
            }).pipe(Effect.as(false)),
          ),
        );
        if (imported) {
          importedThreadIds.add(threadId);
          importedCount += 1;
        } else {
          skippedCount += 1;
        }
      }),
    );

    return { importedCount, skippedCount } satisfies AgentSessionImportResult;
  });

  const requireProject = (projectId: ProjectId) =>
    projects.getById(projectId).pipe(
      Effect.mapError((cause) => new AgentSessionScanError({ operation: "read-projects", cause })),
      Effect.flatMap(
        Option.match({
          onNone: () => Effect.fail(new AgentSessionImportProjectNotFoundError({ projectId })),
          onSome: Effect.succeed,
        }),
      ),
    );

  /** Freshly discovered sessions for the picker, so a just-ended CLI session shows up. */
  const importableSessions = (workspaceRoot: string) =>
    scanner
      .recentThreads(workspaceRoot, [], { fresh: true })
      .pipe(
        Stream.filter(
          (
            outcome,
          ): outcome is Extract<
            AgentSessionScanner.AgentSessionRecentThread,
            { readonly _tag: "Importable" }
          > => outcome._tag === "Importable",
        ),
      );

  const listSessions = Effect.fn("AgentSessionImporter.listSessions")(function* (
    input: AgentSessionListInput,
  ) {
    const project = yield* requireProject(input.projectId);
    const sessions: Array<AgentSessionSummary> = [];
    yield* Stream.runForEach(importableSessions(project.workspaceRoot), ({ thread, source }) =>
      Effect.gen(function* () {
        const threadId = importedThreadId(source);
        const existing = yield* Effect.option(orchestrator.getThreadRecords(threadId, []));
        sessions.push({
          provider: thread.source,
          providerInstanceId: thread.providerInstanceId,
          providerSessionId: thread.providerSessionId,
          title: thread.title.trim() === "" ? "Untitled thread" : thread.title,
          updatedAt: thread.updatedAt,
          threadId:
            Option.isSome(existing) && existing.value.thread.projectId === input.projectId
              ? threadId
              : null,
        });
      }),
    );
    return { sessions } satisfies AgentSessionListResult;
  });

  const openSession = Effect.fn("AgentSessionImporter.openSession")(function* (
    input: AgentSessionOpenInput,
  ) {
    const project = yield* requireProject(input.projectId);
    // A shared home lists its sessions under the built-in instance.
    const match = yield* Stream.runHead(
      importableSessions(project.workspaceRoot).pipe(
        Stream.filter(
          ({ thread }) =>
            thread.providerSessionId === input.providerSessionId &&
            (thread.providerInstanceId === input.providerInstanceId ||
              thread.source === "claudeAgent"),
        ),
      ),
    );
    if (Option.isNone(match)) {
      return yield* new AgentSessionTransferError({ reason: "session-not-found" });
    }
    const { thread, source } = match.value;
    const threadId = yield* importThread({
      projectId: input.projectId,
      workspaceRoot: project.workspaceRoot,
      threadId: importedThreadId(source),
      thread,
      source,
    }).pipe(
      Effect.mapError(
        (cause) => new AgentSessionTransferError({ reason: "write-failed", detail: cause.message }),
      ),
    );
    return { threadId } satisfies AgentSessionOpenResult;
  });

  const receiveSession = Effect.fn("AgentSessionImporter.receiveSession")(function* (
    input: AgentSessionReceiveInput,
  ) {
    const project = yield* requireProject(input.projectId).pipe(
      Effect.mapError(() => new AgentSessionTransferError({ reason: "project-not-found" })),
    );
    const { bundle } = input;
    const sessionId = bundle.providerSessionId;
    const transcriptName = `${sessionId}.jsonl`;
    const files = bundle.files.map((file) => ({
      path: file.path,
      bytes: new Uint8Array(Buffer.from(file.contentBase64, "base64")),
    }));
    const transcript = files.find((file) => file.path === transcriptName);
    if (
      !CLAUDE_SESSION_ID_PATTERN.test(sessionId) ||
      transcript === undefined ||
      files.some((file) => !isBundleSessionPath(file.path, sessionId))
    ) {
      return yield* new AgentSessionTransferError({ reason: "invalid-bundle" });
    }

    const settings = yield* serverSettings.getSettings.pipe(
      Effect.mapError(
        (cause) => new AgentSessionTransferError({ reason: "write-failed", detail: String(cause) }),
      ),
    );
    const targetDriver =
      settings.providerInstances[input.targetProviderInstanceId]?.driver ??
      ProviderDriverKind.make(input.targetProviderInstanceId);

    if (targetDriver !== "claudeAgent") {
      // Any other provider continues from the conversation itself. Its first
      // turn receives the history as imported context, natively where the
      // adapter can inject it (Codex `thread/inject_items`).
      const thread = AgentSessionScanner.parseAgentSessionTranscript({
        contents: new TextDecoder().decode(transcript.bytes),
        source: "claudeAgent",
        providerInstanceId: input.targetProviderInstanceId,
        fallbackSessionId: sessionId,
        lastActiveAtMs: DateTime.toEpochMillis(yield* DateTime.now),
        maxMessages: MAX_TRANSFERRED_MESSAGES,
      });
      if (thread === null) {
        return yield* new AgentSessionTransferError({ reason: "invalid-bundle" });
      }
      const threadId = yield* importThread({
        projectId: input.projectId,
        workspaceRoot: project.workspaceRoot,
        threadId: ThreadId.make(`transfer:${input.targetProviderInstanceId}:${sessionId}`),
        thread: { ...thread, title: bundle.title },
        source: null,
        target: { providerInstanceId: input.targetProviderInstanceId, driver: targetDriver },
      }).pipe(
        Effect.mapError(
          (cause) =>
            new AgentSessionTransferError({ reason: "write-failed", detail: cause.message }),
        ),
      );
      return { threadId } satisfies AgentSessionOpenResult;
    }

    // Claude resumes the copy natively, so it must land byte for byte in the
    // directory the CLI derives from this project's root.
    const homes = yield* scanner
      .resolveHomes("claudeAgent")
      .pipe(
        Effect.mapError(
          (cause) =>
            new AgentSessionTransferError({ reason: "write-failed", detail: cause.message }),
        ),
      );
    const home =
      homes.find((candidate) => candidate.providerInstanceId === input.targetProviderInstanceId) ??
      homes[0];
    if (home === undefined) {
      return yield* new AgentSessionTransferError({ reason: "unsupported-target" });
    }
    const directory = path.join(
      home.homePath,
      "projects",
      claudeProjectDirectoryName(project.workspaceRoot),
    );
    const existingTranscript = yield* fileSystem
      .readFile(path.join(directory, transcriptName))
      .pipe(Effect.option);
    if (
      Option.isSome(existingTranscript) &&
      !startsWithBytes(transcript.bytes, existingTranscript.value)
    ) {
      // Both machines continued the session. Overwriting would drop this one's turns.
      if (!startsWithBytes(existingTranscript.value, transcript.bytes)) {
        return yield* new AgentSessionTransferError({ reason: "diverged" });
      }
    } else {
      yield* Effect.forEach(
        files,
        (file) =>
          Effect.gen(function* () {
            const target = path.join(directory, ...file.path.split("/"));
            yield* fileSystem.makeDirectory(path.dirname(target), { recursive: true });
            yield* fileSystem.writeFile(target, file.bytes);
          }),
        { discard: true },
      ).pipe(
        Effect.mapError(
          (cause) =>
            new AgentSessionTransferError({ reason: "write-failed", detail: String(cause) }),
        ),
      );
    }
    return yield* openSession({
      projectId: input.projectId,
      providerInstanceId: input.targetProviderInstanceId,
      providerSessionId: sessionId,
    }).pipe(
      Effect.mapError((cause) =>
        cause._tag === "AgentSessionTransferError"
          ? cause
          : new AgentSessionTransferError({ reason: "write-failed", detail: cause.message }),
      ),
    );
  });

  const exportBundle = Effect.fn("AgentSessionImporter.exportBundle")(function* (
    input: AgentSessionBundleParams,
  ) {
    const read = yield* scanner
      .readClaudeBundle(input.providerInstanceId, input.providerSessionId)
      .pipe(
        Effect.mapError(
          (cause) =>
            new AgentSessionTransferError({ reason: "write-failed", detail: cause.message }),
        ),
      );
    if (Option.isNone(read)) {
      return yield* new AgentSessionTransferError({ reason: "session-not-found" });
    }
    if (read.value.truncated) {
      return yield* new AgentSessionTransferError({ reason: "too-large" });
    }
    const transcript = read.value.files.find(
      (file) => file.path === `${input.providerSessionId}.jsonl`,
    );
    const parsed =
      transcript === undefined
        ? null
        : AgentSessionScanner.parseAgentSessionTranscript({
            contents: new TextDecoder().decode(transcript.bytes),
            source: "claudeAgent",
            providerInstanceId: input.providerInstanceId,
            fallbackSessionId: input.providerSessionId,
            lastActiveAtMs: DateTime.toEpochMillis(yield* DateTime.now),
          });
    if (parsed === null) {
      return yield* new AgentSessionTransferError({ reason: "invalid-bundle" });
    }
    return {
      provider: "claudeAgent",
      providerSessionId: input.providerSessionId,
      title: parsed.title.trim() === "" ? "Untitled thread" : parsed.title,
      files: read.value.files.map((file) => ({
        path: file.path,
        contentBase64: Buffer.from(file.bytes).toString("base64"),
      })),
    } satisfies AgentSessionBundle;
  });

  return {
    importRecentAgentThreads,
    listSessions,
    openSession,
    receiveSession,
    exportBundle,
  };
});

type AgentSessionImporterShape = Effect.Success<typeof make>;

export class AgentSessionImporter extends Context.Service<
  AgentSessionImporter,
  AgentSessionImporterShape
>()("t3/project/AgentSessionImporter") {}

export const layer = Layer.effect(AgentSessionImporter, make);
