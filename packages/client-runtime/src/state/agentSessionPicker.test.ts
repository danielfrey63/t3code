import {
  EnvironmentId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  type AgentSessionSummary,
} from "@t3tools/contracts";
import { describe, expect, it } from "vitest";

import { agentSessionPickerRows } from "./agentSessionPicker.ts";

const here = EnvironmentId.make("env-home");
const there = EnvironmentId.make("env-work");
const session = (
  providerSessionId: string,
  updatedAt: string,
  overrides: Partial<AgentSessionSummary> = {},
): AgentSessionSummary => ({
  provider: "claudeAgent",
  providerInstanceId: ProviderInstanceId.make("claudeAgent"),
  providerSessionId,
  title: providerSessionId,
  updatedAt,
  threadId: null,
  ...overrides,
});

describe("agentSessionPickerRows", () => {
  const sources = [
    {
      environmentId: there,
      environmentLabel: "WORK",
      projectId: ProjectId.make("project-work"),
      sessions: [
        session("copied", "2026-09-03T00:00:00.000Z"),
        session("work-only", "2026-09-04T00:00:00.000Z"),
        session("codex-work", "2026-09-05T00:00:00.000Z", { provider: "codex" }),
      ],
    },
    {
      environmentId: here,
      environmentLabel: "HOME",
      projectId: ProjectId.make("project-home"),
      sessions: [
        session("copied", "2026-09-01T00:00:00.000Z", { threadId: ThreadId.make("thread-1") }),
        session("home-only", "2026-09-02T00:00:00.000Z"),
      ],
    },
  ];

  it("offers each session once, preferring the local copy, newest first", () => {
    const rows = agentSessionPickerRows({
      currentEnvironmentId: here,
      targetDriver: ProviderDriverKind.make("claudeAgent"),
      sources,
    });
    expect(
      rows.map((row) => [row.session.providerSessionId, row.source.environmentLabel, row.action]),
    ).toEqual([
      ["work-only", "WORK", { _tag: "Transfer", mode: "copy" }],
      ["home-only", "HOME", { _tag: "OpenLocal" }],
      ["copied", "HOME", { _tag: "OpenThread", threadId: "thread-1" }],
    ]);
  });

  it("transforms a transferred session for a non-Claude target", () => {
    const rows = agentSessionPickerRows({
      currentEnvironmentId: here,
      targetDriver: ProviderDriverKind.make("codex"),
      sources,
    });
    expect(rows.find((row) => row.session.providerSessionId === "work-only")?.action).toEqual({
      _tag: "Transfer",
      mode: "transform",
    });
  });
});
