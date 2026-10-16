import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import { ProviderDriverKind, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";

import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as AgentSessionScanner from "./AgentSessionScanner.ts";
import * as ClaudeSessionTitleSync from "./ClaudeSessionTitleSync.ts";

const threadId = ThreadId.make("thread-1");
const sessionId = "0b6a3f1e-8f61-4e83-9d0f-3c7d1c2f9a10";

it.layer(NodeServices.layer)("ClaudeSessionTitleSync", (it) => {
  it.effect("names the Claude session after its thread once", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3code-title-" });
      const filePath = path.join(directory, `${sessionId}.jsonl`);
      yield* fileSystem.writeFileString(
        filePath,
        `${JSON.stringify({ type: "custom-title", customTitle: "Old", sessionId })}\n`,
      );
      const layerTest = ClaudeSessionTitleSync.layer.pipe(
        Layer.provide(
          Layer.mergeAll(
            Layer.mock(Orchestrator.OrchestratorV2)({
              getThreadRecords: () =>
                Effect.succeed({
                  thread: { id: threadId, activeProviderThreadId: "provider-thread" },
                  providerThreads: [
                    {
                      id: "provider-thread",
                      driver: ProviderDriverKind.make("claudeAgent"),
                      providerInstanceId: ProviderInstanceId.make("claudeAgent"),
                      nativeThreadRef: { nativeId: sessionId },
                    },
                  ],
                } as never),
            }),
            Layer.mock(AgentSessionScanner.AgentSessionScanner)({
              findClaudeTranscript: () => Effect.succeed(Option.some(filePath)),
            }),
          ),
        ),
      );
      const sync = (title: string) =>
        Effect.gen(function* () {
          const service = yield* ClaudeSessionTitleSync.ClaudeSessionTitleSync;
          yield* service.syncThread(threadId, title);
          yield* service.syncThread(threadId, title);
        }).pipe(Effect.provide(layerTest));

      yield* sync("Release prep");
      // A fresh instance reads the transcript instead of its memo.
      yield* sync("Release prep");

      const lines = (yield* fileSystem.readFileString(filePath)).trim().split("\n");
      expect(lines).toHaveLength(2);
      expect(ClaudeSessionTitleSync.latestClaudeCustomTitle(lines.join("\n"))).toBe("Release prep");
    }).pipe(Effect.scoped),
  );
});
