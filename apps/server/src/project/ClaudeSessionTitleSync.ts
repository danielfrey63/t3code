/**
 * ClaudeSessionTitleSync - keeps a Claude thread's native session named like
 * the thread.
 *
 * Claude Code lists sessions in `claude --resume` under the latest
 * `custom-title` record of their transcript, the record `/rename` appends.
 * When a thread backed by a Claude session is renamed here, append the same
 * record so the CLI and a later import show that name. The importer reads it
 * back, which closes the loop without a second source of truth.
 *
 * @module project/ClaudeSessionTitleSync
 */
import { ProviderDriverKind, type ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import { forkParked } from "../serverActivation.ts";
import * as AgentSessionScanner from "./AgentSessionScanner.ts";

/** `/rename` records are re-appended often, so the latest sits near the end. */
const TITLE_TAIL_READ_BYTES = 64 * 1024;
const CLAUDE_DRIVER = ProviderDriverKind.make("claudeAgent");

const CustomTitleRecord = Schema.Struct({
  type: Schema.Literal("custom-title"),
  customTitle: Schema.String,
  sessionId: Schema.optional(Schema.String),
});
const decodeCustomTitleRecord = Schema.decodeUnknownOption(
  Schema.fromJsonString(CustomTitleRecord),
);
const encodeCustomTitleRecord = Schema.encodeSync(Schema.fromJsonString(CustomTitleRecord));

/** The latest `/rename` title in a transcript tail, if any. */
export function latestClaudeCustomTitle(tail: string): string | null {
  let title: string | null = null;
  for (const line of tail.split("\n")) {
    // The first line of a tail read is usually partial and fails to decode.
    if (!line.includes('"custom-title"')) continue;
    const record = decodeCustomTitleRecord(line);
    if (Option.isSome(record)) {
      title = AgentSessionScanner.cleanClaudeSessionTitle(record.value.customTitle);
    }
  }
  return title;
}

export class ClaudeSessionTitleSync extends Context.Service<
  ClaudeSessionTitleSync,
  {
    /** Name the thread's Claude session after the thread. Idempotent. */
    readonly syncThread: (threadId: ThreadId, title: string) => Effect.Effect<void>;
    readonly start: () => Effect.Effect<void, never, Scope.Scope>;
  }
>()("t3/project/ClaudeSessionTitleSync") {}

const make = Effect.gen(function* () {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const scanner = yield* AgentSessionScanner.AgentSessionScanner;
  const fileSystem = yield* FileSystem.FileSystem;
  // Metadata updates also carry branch and worktree changes; skip file reads
  // for a title this process already wrote.
  const syncedTitles = new Map<ThreadId, string>();

  const readTail = (filePath: string) =>
    Effect.scoped(
      Effect.gen(function* () {
        const file = yield* fileSystem.open(filePath, { flag: "r" });
        const size = (yield* file.stat).size;
        const readSize =
          size > BigInt(TITLE_TAIL_READ_BYTES) ? BigInt(TITLE_TAIL_READ_BYTES) : size;
        yield* file.seek(size - readSize, "start");
        const chunk = yield* file.readAlloc(Number(readSize));
        return Option.match(chunk, {
          onNone: () => "",
          onSome: (bytes) => new TextDecoder().decode(bytes),
        });
      }),
    );

  const syncThread = (threadId: ThreadId, title: string) =>
    Effect.gen(function* () {
      const name = AgentSessionScanner.cleanClaudeSessionTitle(title);
      if (name === null || syncedTitles.get(threadId) === name) return;
      const { thread, providerThreads } = yield* orchestrator.getThreadRecords(threadId, [
        "providerThreads",
      ]);
      const active = providerThreads.find(
        (providerThread) => providerThread.id === thread.activeProviderThreadId,
      );
      const sessionId = active?.nativeThreadRef?.nativeId;
      if (active?.driver !== CLAUDE_DRIVER || !sessionId) return;
      const filePath = yield* scanner.findClaudeTranscript(active.providerInstanceId, sessionId);
      if (Option.isNone(filePath)) return;
      if (latestClaudeCustomTitle(yield* readTail(filePath.value)) !== name) {
        const record = encodeCustomTitleRecord({
          type: "custom-title",
          customTitle: name,
          sessionId,
        });
        yield* fileSystem.writeFileString(filePath.value, `${record}\n`, { flag: "a" });
      }
      syncedTitles.set(threadId, name);
    }).pipe(
      Effect.catchCause((cause) =>
        Effect.logWarning("Could not name the Claude session after its thread", {
          threadId,
          cause,
        }),
      ),
    );

  const start = () =>
    forkParked(
      Stream.runForEach(orchestrator.streamDomainEvents, (event) =>
        event.type === "thread.metadata-updated"
          ? syncThread(event.threadId, event.payload.title)
          : Effect.void,
      ).pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("Claude session title sync stopped", { cause }),
        ),
      ),
    );

  return ClaudeSessionTitleSync.of({ syncThread, start });
});

export const layer = Layer.effect(ClaudeSessionTitleSync, make);
