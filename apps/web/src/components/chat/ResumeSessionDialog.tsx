import { RegistryContext, useAtomValue } from "@effect/atom-react";
import {
  agentSessionPickerRows,
  type AgentSessionPickerRow,
  type AgentSessionPickerSource,
} from "@t3tools/client-runtime/state/agentSessionPicker";
import { squashAtomCommandFailure } from "@t3tools/client-runtime/state/runtime";
import type {
  EnvironmentId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ScopedProjectRef,
  ThreadId,
} from "@t3tools/contracts";
import { useNavigate } from "@tanstack/react-router";
import * as Option from "effect/Option";
import { AsyncResult, Atom } from "effect/reactivity";
import { useContext, useMemo, useState } from "react";

import { formatEnvironmentQueryError } from "~/state/query";
import { agentSessionList, agentSessionOpen, agentSessionTransfer } from "~/state/agentSessions";
import { useAtomCommand } from "~/state/use-atom-command";
import { Button, InlineButton } from "../ui/button";
import {
  Dialog,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogPanel,
  DialogPopup,
  DialogTitle,
} from "../ui/dialog";
import { Spinner } from "../ui/spinner";

/** One environment's copy of the project the draft belongs to. */
export interface ResumeSessionTarget {
  readonly environmentId: EnvironmentId;
  readonly projectId: ProjectId;
  readonly label: string;
}

interface ResumeSessionDialogProps {
  readonly open: boolean;
  readonly onOpenChange: (open: boolean) => void;
  /** The environment and project the draft would start in. */
  readonly current: ResumeSessionTarget;
  /** Every environment's copy of the same project, the current one included. */
  readonly targets: ReadonlyArray<ResumeSessionTarget>;
  readonly targetProviderInstanceId: ProviderInstanceId | null;
  readonly targetDriver: ProviderDriverKind | null;
}

const ACTION_LABEL: Record<AgentSessionPickerRow["action"]["_tag"], string> = {
  OpenThread: "Open thread",
  OpenLocal: "Continue",
  Transfer: "Continue here",
};

function transferHint(row: AgentSessionPickerRow): string | null {
  if (row.action._tag !== "Transfer") return null;
  return row.action.mode === "copy"
    ? `Copies the session from ${row.source.environmentLabel}`
    : `Continues the conversation from ${row.source.environmentLabel} on the selected provider`;
}

/** "or resume a session" under the draft headline, with the dialog it opens. */
export function ResumeSessionAction(props: {
  readonly current: ScopedProjectRef;
  /** The project's copies on every environment, labelled by environment. */
  readonly members: ReadonlyArray<{
    readonly environmentId: EnvironmentId;
    readonly id: ProjectId;
    readonly environmentLabel: string | null;
  }>;
  readonly targetProviderInstanceId: ProviderInstanceId | null;
  readonly targetDriver: ProviderDriverKind | null;
}) {
  const [open, setOpen] = useState(false);
  const allowed = useAtomValue(agentSessionOpen.permissionAtom(props.current.environmentId));
  const targets = useMemo(
    () =>
      props.members.map((member) => ({
        environmentId: member.environmentId,
        projectId: member.id,
        label: member.environmentLabel ?? "This environment",
      })),
    [props.members],
  );
  const current = targets.find(
    (target) =>
      target.environmentId === props.current.environmentId &&
      target.projectId === props.current.projectId,
  );
  if (!allowed || current === undefined) return null;
  return (
    <>
      <InlineButton tone="muted" className="pointer-events-auto" onClick={() => setOpen(true)}>
        or resume a session
      </InlineButton>
      <ResumeSessionDialog
        open={open}
        onOpenChange={setOpen}
        current={current}
        targets={targets}
        targetProviderInstanceId={props.targetProviderInstanceId}
        targetDriver={props.targetDriver}
      />
    </>
  );
}

/**
 * Lists the native sessions of this project on every connected environment
 * and continues the chosen one here, copying or converting it when it lives
 * on another machine.
 */
export function ResumeSessionDialog(props: ResumeSessionDialogProps) {
  const registry = useContext(RegistryContext);
  const navigate = useNavigate();
  const openSession = useAtomCommand(agentSessionOpen, { reportFailure: false });
  const readBundle = useAtomCommand(agentSessionTransfer.bundle, { reportFailure: false });
  const receiveSession = useAtomCommand(agentSessionTransfer.receive, { reportFailure: false });
  const [pendingKey, setPendingKey] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  // Scans run only while the dialog is open, so each opening lists fresh sessions.
  const listsAtom = useMemo(
    () =>
      Atom.make((get) =>
        props.open
          ? props.targets.map((target) => {
              const atom = agentSessionList({
                environmentId: target.environmentId,
                input: { projectId: target.projectId },
              });
              const result = get(atom);
              return {
                target,
                sessions: Option.getOrNull(AsyncResult.value(result))?.sessions ?? null,
                error: result._tag === "Failure" ? formatEnvironmentQueryError(result.cause) : null,
                isPending: result.waiting || result._tag === "Initial",
                refresh: () => registry.refresh(atom),
              };
            })
          : [],
      ),
    [props.open, props.targets, registry],
  );
  const lists = useAtomValue(listsAtom);
  const rows = useMemo(() => {
    const sources: Array<AgentSessionPickerSource> = lists.flatMap((list) =>
      list.sessions === null
        ? []
        : [
            {
              environmentId: list.target.environmentId,
              environmentLabel: list.target.label,
              projectId: list.target.projectId,
              sessions: list.sessions,
            },
          ],
    );
    return props.targetDriver === null
      ? []
      : agentSessionPickerRows({
          currentEnvironmentId: props.current.environmentId,
          targetDriver: props.targetDriver,
          sources,
        });
  }, [lists, props.current.environmentId, props.targetDriver]);
  const isPending = lists.some((list) => list.isPending);
  const listErrors = lists.flatMap((list) =>
    list.error === null ? [] : [`${list.target.label}: ${list.error}`],
  );

  const continueRow = async (row: AgentSessionPickerRow) => {
    setPendingKey(row.key);
    setError(null);
    const threadId = await resolveThread(row);
    setPendingKey(null);
    if (threadId === null) return;
    props.onOpenChange(false);
    await navigate({
      to: "/$environmentId/$threadId",
      params: { environmentId: props.current.environmentId, threadId },
    });
  };

  const resolveThread = async (row: AgentSessionPickerRow): Promise<ThreadId | null> => {
    const fail = (cause: unknown) => {
      setError(cause instanceof Error ? cause.message : "The session could not be continued.");
      return null;
    };
    switch (row.action._tag) {
      case "OpenThread":
        return row.action.threadId;
      case "OpenLocal": {
        const opened = await openSession({
          environmentId: props.current.environmentId,
          input: {
            projectId: props.current.projectId,
            providerInstanceId: row.session.providerInstanceId,
            providerSessionId: row.session.providerSessionId,
          },
        });
        return opened._tag === "Success"
          ? opened.value.threadId
          : fail(squashAtomCommandFailure(opened));
      }
      case "Transfer": {
        if (props.targetProviderInstanceId === null) {
          return fail(new Error("Choose a provider for this thread first."));
        }
        const bundle = await readBundle({
          environmentId: row.source.environmentId,
          input: {
            providerInstanceId: row.session.providerInstanceId,
            providerSessionId: row.session.providerSessionId,
          },
        });
        if (bundle._tag !== "Success") return fail(squashAtomCommandFailure(bundle));
        const received = await receiveSession({
          environmentId: props.current.environmentId,
          input: {
            projectId: props.current.projectId,
            targetProviderInstanceId: props.targetProviderInstanceId,
            bundle: bundle.value,
          },
        });
        return received._tag === "Success"
          ? received.value.threadId
          : fail(squashAtomCommandFailure(received));
      }
    }
  };

  return (
    <Dialog
      open={props.open}
      onOpenChange={(nextOpen) => {
        if (pendingKey === null) props.onOpenChange(nextOpen);
      }}
    >
      <DialogPopup className="max-w-xl">
        <DialogHeader>
          <DialogTitle>Resume a session</DialogTitle>
          <DialogDescription>
            Sessions from the last 30 days in this project, on every connected environment.
          </DialogDescription>
        </DialogHeader>
        <DialogPanel>
          {rows.length === 0 ? (
            <p className="text-muted-foreground text-sm">
              {isPending ? "Looking for sessions…" : "No sessions found."}
            </p>
          ) : (
            <ul className="flex max-h-96 flex-col gap-1 overflow-y-auto">
              {rows.map((row) => (
                <li key={row.key}>
                  <button
                    type="button"
                    disabled={pendingKey !== null}
                    onClick={() => void continueRow(row)}
                    className="flex w-full items-center justify-between gap-3 rounded-lg px-3 py-2 text-start hover:bg-accent disabled:opacity-60"
                  >
                    <span className="min-w-0">
                      <span className="block truncate font-medium text-sm">
                        {row.session.title}
                      </span>
                      <span className="block truncate text-muted-foreground text-xs">
                        {row.source.environmentLabel} ·{" "}
                        {row.session.provider === "claudeAgent" ? "Claude" : "Codex"} ·{" "}
                        {new Date(row.session.updatedAt).toLocaleString()}
                        {transferHint(row) === null ? null : ` · ${transferHint(row)}`}
                      </span>
                    </span>
                    <span className="flex shrink-0 items-center gap-2 text-muted-foreground text-xs">
                      {pendingKey === row.key ? <Spinner size="sm" /> : null}
                      {ACTION_LABEL[row.action._tag]}
                    </span>
                  </button>
                </li>
              ))}
            </ul>
          )}
          {isPending && rows.length > 0 ? (
            <p className="flex items-center gap-2 text-muted-foreground text-xs">
              <Spinner size="sm" />
              Still scanning other environments…
            </p>
          ) : null}
          {[...listErrors, ...(error === null ? [] : [error])].map((message) => (
            <p key={message} className="text-destructive text-xs">
              {message}
            </p>
          ))}
        </DialogPanel>
        <DialogFooter>
          <Button
            type="button"
            variant="outline"
            size="sm"
            disabled={pendingKey !== null}
            onClick={() => props.onOpenChange(false)}
          >
            Cancel
          </Button>
        </DialogFooter>
      </DialogPopup>
    </Dialog>
  );
}
