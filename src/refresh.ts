import { Effect, Schema } from "effect";
import { captureGlobalBus, type CaptureDebug } from "./global-bus-capture.js";
import { readGlobalBusModule, type GlobalBusLike } from "./global-bus.js";
import type { RelayEventMessage } from "./protocol.js";

export type RefreshMode =
  | "global-bus-import"
  | "global-bus-capture"
  | "worker-rpc"
  | "none";

class RefreshProbeFailed extends Schema.TaggedErrorClass<RefreshProbeFailed>()(
  "RefreshProbeFailed",
  { cause: Schema.Defect },
) {}

const WorkerRpcEnvelopeJson = Schema.fromJsonString(Schema.Unknown);

export type Refresh = {
  mode: RefreshMode;
  publish(input: RelayEventMessage): Effect.Effect<void>;
};

export const makeRefresh = Effect.fn("makeRefresh")(function* (input: {
  client: unknown;
  debug?: CaptureDebug;
  importGlobalBus?: () => Promise<unknown>;
  captureGlobalBus?: (input: {
    client: unknown;
    debug?: CaptureDebug;
  }) => Promise<GlobalBusLike | undefined>;
}) {
  const globalBusModule = "opencode/bus/global";
  const imported = yield* Effect.tryPromise({
    try: () =>
      input.importGlobalBus ? input.importGlobalBus() : import(globalBusModule),
    catch: (cause) => new RefreshProbeFailed({ cause }),
  }).pipe(Effect.option);

  if (imported._tag === "None") {
    input.debug?.("private GlobalBus import failed");
  }

  if (imported._tag === "Some") {
    const bus = readGlobalBusModule(imported.value);

    if (bus) {
      return globalBusRefresh("global-bus-import", bus);
    }
  }

  const capture = input.captureGlobalBus;
  const captured = yield* (
    capture
      ? Effect.tryPromise({
          try: () =>
            capture({
              client: input.client,
              debug: input.debug,
            }),
          catch: (cause) => new RefreshProbeFailed({ cause }),
        })
      : captureGlobalBus({ client: input.client, debug: input.debug })
  ).pipe(Effect.option);

  if (captured._tag === "Some" && captured.value) {
    return globalBusRefresh("global-bus-capture", captured.value);
  }

  input.debug?.("no GlobalBus capture path validated");

  const workerRpc = readWorkerRpc();

  if (workerRpc) {
    return workerRpcRefresh(workerRpc);
  }

  input.debug?.(`postMessage type: ${typeof globalThis.postMessage}`);
  input.debug?.(`process.execPath: ${process.execPath}`);
  input.debug?.(`Bun.main: ${String(Bun.main)}`);
  input.debug?.(`process.argv: ${process.argv.slice(0, 3).join(" | ")}`);

  return noneRefresh;
});

function globalBusRefresh(mode: RefreshMode, bus: GlobalBusLike): Refresh {
  return {
    mode,
    publish(input) {
      return Effect.sync(() => {
        bus.emit("event", {
          directory: input.directory,
          project: input.projectID,
          workspace: input.workspaceID,
          payload: {
            id: input.event.id,
            type: input.event.type,
            properties: input.event.properties,
          },
        });
      });
    },
  };
}

function workerRpcRefresh(postMessage: (message: string) => void): Refresh {
  return {
    mode: "worker-rpc",
    publish(input) {
      return Effect.sync(() => {
        const envelope = {
          type: "rpc.event",
          event: "global.event",
          data: {
            directory: input.directory,
            project: input.projectID,
            workspace: input.workspaceID,
            payload: {
              id: input.event.id,
              type: input.event.type,
              properties: input.event.properties,
            },
          },
        };
        postMessage(Schema.encodeUnknownSync(WorkerRpcEnvelopeJson)(envelope));
      });
    },
  };
}

const noneRefresh: Refresh = {
  mode: "none",
  publish() {
    return Effect.void;
  },
};

function readWorkerRpc() {
  const postMessage = globalThis.postMessage;

  if (typeof postMessage !== "function") {
    return undefined;
  }

  return (message: string) => {
    postMessage.call(globalThis, message);
  };
}
