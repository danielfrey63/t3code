import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import {
  ProjectId,
  ProviderInstanceId,
  type AgentSessionBundle,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Stream from "effect/Stream";

import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as ProviderSessionRuntime from "../persistence/ProviderSessionRuntime.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as AgentSessionImporter from "./AgentSessionImporter.ts";
import * as AgentSessionScanner from "./AgentSessionScanner.ts";
import * as ProjectService from "./ProjectService.ts";

const projectId = ProjectId.make("transfer-project");
const workspaceRoot = "/workspace/project";
const claudeInstanceId = ProviderInstanceId.make("claudeAgent");
const codexInstanceId = ProviderInstanceId.make("codex");
const sessionId = "0b6a3f1e-8f61-4e83-9d0f-3c7d1c2f9a10";

const transcript = [
  JSON.stringify({ type: "custom-title", customTitle: "PMO@WORK", sessionId }),
  JSON.stringify({
    type: "user",
    sessionId,
    cwd: "D:\\Work\\project",
    message: { role: "user", content: "Plan the release" },
  }),
  JSON.stringify({
    type: "assistant",
    sessionId,
    cwd: "D:\\Work\\project",
    message: { role: "assistant", content: [{ type: "text", text: "Here is the plan" }] },
  }),
].join("\n");

const base64 = (text: string) => Buffer.from(text).toString("base64");
const bundle = (files: AgentSessionBundle["files"]): AgentSessionBundle => ({
  provider: "claudeAgent",
  providerSessionId: sessionId,
  title: "PMO@WORK",
  files,
});
const fullBundle = bundle([
  { path: `${sessionId}.jsonl`, contentBase64: base64(transcript) },
  { path: `${sessionId}/tool-results/result-1.txt`, contentBase64: base64("tool output") },
]);

const makeLayer = (claudeHome: string, writes: Array<ReadonlyArray<OrchestrationV2DomainEvent>>) =>
  AgentSessionImporter.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(AgentSessionScanner.AgentSessionScanner)({
          resolveHomes: () =>
            Effect.succeed([{ homePath: claudeHome, providerInstanceId: claudeInstanceId }]),
          recentThreads: () =>
            Stream.make({
              _tag: "Importable" as const,
              source: {
                provider: "claudeAgent" as const,
                providerInstanceId: claudeInstanceId,
                providerSessionId: sessionId,
                filePath: `${claudeHome}/projects/-workspace-project/${sessionId}.jsonl`,
                size: transcript.length,
                mtimeMs: 1,
                device: 1,
                inode: 1,
                birthtimeMs: 1,
              },
              thread: {
                source: "claudeAgent" as const,
                providerInstanceId: claudeInstanceId,
                providerSessionId: sessionId,
                title: "PMO@WORK",
                model: null,
                createdAt: "2026-09-01T10:00:00.000Z",
                updatedAt: "2026-09-01T10:01:00.000Z",
                messages: [],
              },
            }),
        }),
        Layer.mock(ProjectService.ProjectService)({
          getById: () => Effect.succeed(Option.some({ id: projectId, workspaceRoot } as never)),
        }),
        Layer.mock(Orchestrator.OrchestratorV2)({
          getThreadRecords: (threadId) =>
            Effect.fail(new Orchestrator.OrchestratorProjectionError({ threadId })),
        }),
        Layer.mock(EventSink.EventSinkV2)({
          write: (input) => Effect.sync(() => (writes.push(input.events), [])),
        }),
        Layer.mock(ProviderSessionRuntime.ProviderSessionRuntimeRepository)({
          upsert: () => Effect.void,
          recordImportedTranscript: () => Effect.void,
        }),
        Layer.mock(ServerSettings.ServerSettingsService)({
          getSettings: Effect.succeed({
            providerInstances: { codex: { driver: "codex" } },
          } as never),
        }),
        IdAllocator.layer,
        NodeServices.layer,
      ),
    ),
  );

it.layer(NodeServices.layer)("AgentSessionImporter transfer", (it) => {
  it.effect("copies a Claude session 1:1 into the project's Claude directory", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const claudeHome = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3code-transfer-" });
      const writes: Array<ReadonlyArray<OrchestrationV2DomainEvent>> = [];
      const receive = Effect.gen(function* () {
        const importer = yield* AgentSessionImporter.AgentSessionImporter;
        return yield* importer.receiveSession({
          projectId,
          targetProviderInstanceId: claudeInstanceId,
          bundle: fullBundle,
        });
      }).pipe(Effect.provide(makeLayer(claudeHome, writes)));

      expect(yield* receive).toEqual({ threadId: `import:claudeAgent:${sessionId}` });
      // Receiving the same session again is a no-op for the files.
      yield* receive;

      const directory = path.join(claudeHome, "projects", "-workspace-project");
      expect(yield* fileSystem.readFileString(path.join(directory, `${sessionId}.jsonl`))).toBe(
        transcript,
      );
      expect(
        yield* fileSystem.readFileString(
          path.join(directory, sessionId, "tool-results", "result-1.txt"),
        ),
      ).toBe("tool output");
    }).pipe(Effect.scoped),
  );

  it.effect("refuses to overwrite a session continued on both environments", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const claudeHome = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3code-transfer-" });
      const directory = path.join(claudeHome, "projects", "-workspace-project");
      yield* fileSystem.makeDirectory(directory, { recursive: true });
      yield* fileSystem.writeFileString(
        path.join(directory, `${sessionId}.jsonl`),
        `${transcript.slice(0, 40)}local continuation`,
      );
      const error = yield* Effect.gen(function* () {
        const importer = yield* AgentSessionImporter.AgentSessionImporter;
        return yield* importer.receiveSession({
          projectId,
          targetProviderInstanceId: claudeInstanceId,
          bundle: fullBundle,
        });
      }).pipe(Effect.provide(makeLayer(claudeHome, [])), Effect.flip);
      expect(error).toMatchObject({ _tag: "AgentSessionTransferError", reason: "diverged" });
    }).pipe(Effect.scoped),
  );

  it.effect("continues a Claude session on Codex from its conversation", () =>
    Effect.gen(function* () {
      const writes: Array<ReadonlyArray<OrchestrationV2DomainEvent>> = [];
      const result = yield* Effect.gen(function* () {
        const importer = yield* AgentSessionImporter.AgentSessionImporter;
        return yield* importer.receiveSession({
          projectId,
          targetProviderInstanceId: codexInstanceId,
          bundle: fullBundle,
        });
      }).pipe(Effect.provide(makeLayer("/unused", writes)));

      expect(result).toEqual({ threadId: `transfer:codex:${sessionId}` });
      const events = writes.flat();
      expect(events.map((event) => event.type)).toEqual([
        "thread.created",
        "message.updated",
        "turn-item.updated",
        "message.updated",
        "turn-item.updated",
      ]);
      expect(events[0]?.payload).toMatchObject({
        title: "PMO@WORK",
        providerInstanceId: "codex",
        activeProviderThreadId: null,
        historyOrigin: "v1_import",
      });
    }),
  );

  it.effect("rejects bundle paths outside the session", () =>
    Effect.gen(function* () {
      const error = yield* Effect.gen(function* () {
        const importer = yield* AgentSessionImporter.AgentSessionImporter;
        return yield* importer.receiveSession({
          projectId,
          targetProviderInstanceId: claudeInstanceId,
          bundle: bundle([
            { path: `${sessionId}.jsonl`, contentBase64: base64(transcript) },
            { path: `${sessionId}/../../evil.sh`, contentBase64: base64("x") },
          ]),
        });
      }).pipe(Effect.provide(makeLayer("/unused", [])), Effect.flip);
      expect(error).toMatchObject({ reason: "invalid-bundle" });
    }),
  );
});
