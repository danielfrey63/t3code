import { expect, it } from "@effect/vitest";
import {
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";

import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as ProviderSessionRuntime from "../persistence/ProviderSessionRuntime.ts";
import * as AgentSessionImporter from "./AgentSessionImporter.ts";
import * as AgentSessionScanner from "./AgentSessionScanner.ts";
import * as ProjectService from "./ProjectService.ts";

const projectId = ProjectId.make("agent-session-import-project");
const providerInstanceId = ProviderInstanceId.make("codex");
const providerSessionId = "native-codex-thread";
const threadId = ThreadId.make(`import:${providerInstanceId}:${providerSessionId}`);
const unusedScannerMethods = {
  scan: Effect.die("unused"),
  resolveHomes: () => Effect.die("unused"),
  findClaudeTranscript: () => Effect.die("unused"),
};

it.effect("imports messages once and preserves the provider native resume binding", () => {
  const writes: Array<ReadonlyArray<OrchestrationV2DomainEvent>> = [];
  const upserts: Array<unknown> = [];
  const recorded: Array<unknown> = [];
  let imported = false;
  const scanner = AgentSessionScanner.AgentSessionScanner.of({
    ...unusedScannerMethods,
    recentThreads: () =>
      Stream.succeed({
        _tag: "Importable",
        source: {
          provider: "codex",
          providerInstanceId,
          providerSessionId,
          filePath: "/tmp/native-codex-thread.jsonl",
          size: 100,
          mtimeMs: 2,
          device: 3,
          inode: 4,
          birthtimeMs: 1,
        },
        thread: {
          source: "codex",
          providerInstanceId,
          providerSessionId,
          title: "Imported thread",
          model: "gpt-5.4",
          createdAt: "2026-09-01T10:00:00.000Z",
          updatedAt: "2026-09-01T10:01:00.000Z",
          messages: [
            { role: "user", text: "Fix it", createdAt: "2026-09-01T10:00:00.000Z" },
            { role: "assistant", text: "Fixed", createdAt: "2026-09-01T10:01:00.000Z" },
          ],
        },
      }),
  });
  const layerTest = AgentSessionImporter.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.succeed(AgentSessionScanner.AgentSessionScanner, scanner),
        Layer.mock(ProjectService.ProjectService)({
          getById: () =>
            Effect.succeed(
              Option.some({ id: projectId, workspaceRoot: "/workspace/project" } as never),
            ),
        }),
        Layer.mock(Orchestrator.OrchestratorV2)({
          getThreadRecords: () =>
            imported
              ? Effect.succeed({
                  thread: {
                    id: threadId,
                    projectId,
                    historyOrigin: "v1_import",
                    title: "Imported thread",
                  },
                } as never)
              : Effect.fail(new Orchestrator.OrchestratorProjectionError({ threadId })),
        }),
        Layer.mock(EventSink.EventSinkV2)({
          write: (input) =>
            Effect.sync(() => {
              writes.push(input.events);
              imported = true;
              return [];
            }),
        }),
        Layer.mock(ProviderSessionRuntime.ProviderSessionRuntimeRepository)({
          list: () => Effect.succeed([]),
          upsert: (input) => Effect.sync(() => void upserts.push(input)),
          recordImportedTranscript: (input) => Effect.sync(() => void recorded.push(input)),
        }),
        IdAllocator.layer,
      ),
    ),
  );

  return Effect.gen(function* () {
    const importer = yield* AgentSessionImporter.AgentSessionImporter;
    expect(yield* importer.importRecentAgentThreads({ projectId })).toEqual({
      importedCount: 1,
      skippedCount: 0,
    });
    expect(yield* importer.importRecentAgentThreads({ projectId })).toEqual({
      importedCount: 1,
      skippedCount: 0,
    });

    expect(writes).toHaveLength(1);
    expect(writes[0]?.map((event) => event.type)).toEqual([
      "thread.created",
      "message.updated",
      "turn-item.updated",
      "message.updated",
      "turn-item.updated",
      "provider-thread.updated",
    ]);
    const created = writes[0]?.find((event) => event.type === "thread.created");
    const providerThread = writes[0]?.find((event) => event.type === "provider-thread.updated");
    expect(created?.payload).toMatchObject({
      id: threadId,
      activeProviderThreadId: providerThread?.payload.id,
      historyOrigin: "v1_import",
    });
    expect(providerThread?.payload).toMatchObject({
      appThreadId: threadId,
      nativeThreadRef: {
        driver: "codex",
        nativeId: providerSessionId,
        strength: "strong",
      },
    });
    expect(
      writes[0]
        ?.filter((event) => event.type === "message.updated")
        .map((event) => event.payload.text),
    ).toEqual(["Fix it", "Fixed"]);
    expect(upserts).toEqual([
      expect.objectContaining({
        threadId,
        providerInstanceId,
        resumeCursor: { threadId: providerSessionId },
      }),
    ]);
    expect(recorded).toHaveLength(2);
  }).pipe(Effect.provide(layerTest));
});

it.effect("follows transcript renames and compactions for imported threads", () => {
  const claudeInstanceId = ProviderInstanceId.make("claudeAgent");
  const sessionId = "0b6a3f1e-8f61-4e83-9d0f-3c7d1c2f9a10";
  const renamedThreadId = ThreadId.make(`import:${claudeInstanceId}:${sessionId}`);
  const olderSessionId = "5d4c1b2a-1e2f-4a3b-8c9d-0e1f2a3b4c5d";
  const olderThreadId = ThreadId.make(`import:${claudeInstanceId}:${olderSessionId}`);
  const source = (providerSessionId: string) => ({
    provider: "claudeAgent" as const,
    providerInstanceId: claudeInstanceId,
    providerSessionId,
    filePath: `/claude/projects/-workspace-project/${providerSessionId}.jsonl`,
    size: 100,
    mtimeMs: 2,
    device: 3,
    inode: 4,
    birthtimeMs: 1,
  });
  const titles = new Map<string, string>([
    [renamedThreadId, "Old name"],
    [olderThreadId, "Ship release"],
  ]);
  const dispatched: Array<{ readonly threadId: string; readonly title: string | undefined }> = [];
  const scanner = AgentSessionScanner.AgentSessionScanner.of({
    ...unusedScannerMethods,
    recentThreads: () =>
      Stream.make(
        {
          _tag: "Importable" as const,
          source: source(sessionId),
          thread: {
            source: "claudeAgent" as const,
            providerInstanceId: claudeInstanceId,
            providerSessionId: sessionId,
            title: "PMO@HOME",
            model: null,
            createdAt: "2026-09-01T10:00:00.000Z",
            updatedAt: "2026-09-01T10:01:00.000Z",
            messages: [
              { role: "user" as const, text: "Hi", createdAt: "2026-09-01T10:00:00.000Z" },
            ],
          },
        },
        {
          _tag: "AlreadyImported" as const,
          source: source(olderSessionId),
          continuedAfterCompact: true,
        },
      ),
  });
  const layerTest = AgentSessionImporter.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.succeed(AgentSessionScanner.AgentSessionScanner, scanner),
        Layer.mock(ProjectService.ProjectService)({
          getById: () =>
            Effect.succeed(
              Option.some({ id: projectId, workspaceRoot: "/workspace/project" } as never),
            ),
        }),
        Layer.mock(Orchestrator.OrchestratorV2)({
          getThreadRecords: (id) =>
            Effect.succeed({
              thread: { id, projectId, historyOrigin: "v1_import", title: titles.get(id) },
            } as never),
          dispatch: (command) =>
            Effect.sync(() => {
              if (command.type === "thread.metadata.update") {
                dispatched.push({ threadId: command.threadId, title: command.title });
              }
              return {} as never;
            }),
        }),
        Layer.mock(EventSink.EventSinkV2)({}),
        Layer.mock(ProviderSessionRuntime.ProviderSessionRuntimeRepository)({
          list: () => Effect.succeed([]),
          recordImportedTranscript: () => Effect.void,
        }),
        IdAllocator.layer,
      ),
    ),
  );

  return Effect.gen(function* () {
    const importer = yield* AgentSessionImporter.AgentSessionImporter;
    yield* importer.importRecentAgentThreads({ projectId });
    expect(dispatched).toEqual([
      { threadId: renamedThreadId, title: "PMO@HOME" },
      { threadId: olderThreadId, title: "Ship release (before compact)" },
    ]);
  }).pipe(Effect.provide(layerTest));
});
