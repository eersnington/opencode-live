import type { Plugin } from "@opencode-ai/plugin";
import { spawn } from "node:child_process";
import fs from "node:fs/promises";
import { createHash, randomUUID } from "node:crypto";
import path from "node:path";
import { Effect, Exit, Option, Schema } from "effect";
import { opencodeDataDir, resolveOpencodeDbPath } from "./opencode-db.js";
import { connectIpc, socketPath } from "./ipc.js";
import { daemonRegistryFile, readDaemonRegistry } from "./daemon-registry.js";
import {
  AllowedEventTypeSchema,
  DbHashSchema,
  ProcessIDSchema,
  type DbHash,
  type ServerMessage,
} from "./protocol.js";
import { makeRefresh } from "./refresh.js";

type Options = {
  dbPath?: string;
  dataDir?: string;
  bunPath?: string;
  debug?: boolean;
  idleTimeoutMillis?: number;
};

type ConnectionInput = {
  dbPath: string;
  hash: DbHash;
  dataDir: string;
  options?: Options;
};

type PluginEventInput = {
  event: {
    id?: string;
    type: string;
    properties: unknown;
  };
};

type PluginContext = Parameters<Plugin>[0];

class DaemonNotReady extends Schema.TaggedErrorClass<DaemonNotReady>()(
  "DaemonNotReady",
  { socketPath: Schema.String },
) {}

class DaemonSpawnFailed extends Schema.TaggedErrorClass<DaemonSpawnFailed>()(
  "DaemonSpawnFailed",
  { cause: Schema.Defect },
) {}

const server: Plugin = (ctx, options?: Options) => {
  return Effect.runPromise(makeServerHooks(ctx, options));
};

const makeServerHooks = Effect.fn("makeServerHooks")(function* (
  ctx: PluginContext,
  options?: Options,
) {
  const startupStarted = performance.now();
  const dataDir = options?.dataDir ?? opencodeDataDir();

  const dbStarted = performance.now();
  const resolvedDbPath = yield* resolveOpencodeDbPath({
    dbPath: options?.dbPath,
    dataDir,
  });

  const hash = Schema.decodeUnknownSync(DbHashSchema)(
    createHash("md5").update(path.resolve(resolvedDbPath)).digest("hex"),
  );

  const processID = Schema.decodeUnknownSync(ProcessIDSchema)(
    `${process.pid}-${randomUUID()}`,
  );

  const debug = options?.debug
    ? (message: string) => console.error(`[opencode-live] ${message}`)
    : undefined;
  const dbMillis = performance.now() - dbStarted;

  const refreshStarted = performance.now();
  const refresh = yield* makeRefresh({
    client: ctx.client,
    serverUrl: ctx.serverUrl,
    debug,
  });
  const refreshMillis = performance.now() - refreshStarted;

  if (options?.debug) {
    console.error(`[opencode-live] refresh mode: ${refresh.mode}`);
  }

  const handleDaemonMessage = (message: ServerMessage) => {
    if (message.type === "error") {
      console.error(`[opencode-live] daemon error: ${message.message}`);
      return;
    }

    if (message.originProcessID === processID) {
      return;
    }

    const publish = Effect.runSyncExit(refresh.publish(message));

    if (Exit.isFailure(publish)) {
      console.error(`[opencode-live] refresh error: ${publish.cause}`);
    }
  };

  const daemonStarted = performance.now();
  const peer = yield* connectOrStartDaemon(
    { dbPath: resolvedDbPath, hash, dataDir, options },
    handleDaemonMessage,
  );
  const daemonMillis = performance.now() - daemonStarted;
  debug?.(
    `startup timings: db=${Math.round(dbMillis)}ms refresh=${Math.round(refreshMillis)}ms daemon=${Math.round(daemonMillis)}ms total=${Math.round(performance.now() - startupStarted)}ms mode=${refresh.mode}`,
  );

  peer.send({
    type: "hello",
    processID,
    dbPath: resolvedDbPath,
    dbHash: hash,
    directory: ctx.directory,
    projectID: ctx.project.id,
  });

  return {
    event(input: PluginEventInput) {
      const eventType = input.event.type;

      if (!Schema.is(AllowedEventTypeSchema)(eventType)) {
        return Promise.resolve();
      }

      peer.send({
        type: "event",
        originProcessID: processID,
        directory: ctx.directory,
        projectID: ctx.project.id,
        event: {
          id: input.event.id,
          type: eventType,
          properties: input.event.properties,
        },
      });

      return Promise.resolve();
    },
    dispose() {
      peer.close();
    },
  };
});

const connectOrStartDaemon = Effect.fn("connectOrStartDaemon")(function* (
  input: ConnectionInput,
  onMessage: (message: ServerMessage) => void,
) {
  const registryStarted = performance.now();
  const registry = yield* readDaemonRegistry(input.hash, input.dataDir);
  if (input.options?.debug) {
    console.error(
      `[opencode-live] daemon registry read: ${Math.round(performance.now() - registryStarted)}ms`,
    );
  }

  if (registry) {
    const connectStarted = performance.now();
    const daemon = yield* connectToDaemon(registry.socketPath, onMessage).pipe(
      Effect.option,
    );

    if (input.options?.debug) {
      console.error(
        `[opencode-live] daemon registry connect: ${Math.round(performance.now() - connectStarted)}ms`,
      );
    }

    if (Option.isSome(daemon)) {
      return daemon.value;
    }

    yield* Effect.tryPromise(() =>
      fs.rm(
        daemonRegistryFile({ dbHash: input.hash, dataDir: input.dataDir }),
        {
          force: true,
        },
      ),
    ).pipe(Effect.ignore);
  }

  const sock = socketPath(input.hash);
  const spawnStarted = performance.now();
  yield* startDaemon({ ...input, socketPath: sock });
  if (input.options?.debug) {
    console.error(
      `[opencode-live] daemon spawn: ${Math.round(performance.now() - spawnStarted)}ms`,
    );
  }

  const waitStarted = performance.now();
  const daemon = yield* waitForDaemon(sock, onMessage);
  if (input.options?.debug) {
    console.error(
      `[opencode-live] daemon wait: ${Math.round(performance.now() - waitStarted)}ms`,
    );
  }

  if (!daemon) {
    return yield* new DaemonNotReady({ socketPath: sock });
  }

  return daemon;
});

const connectToDaemon = Effect.fn("connectToDaemon")(function* (
  sock: string,
  onMessage: (message: ServerMessage) => void,
) {
  return yield* connectIpc({
    socketPath: sock,
    onMessage,
    onError(error) {
      console.error(`[opencode-live] ipc error: ${error.message}`);
    },
  });
});

const waitForDaemon = Effect.fn("waitForDaemon")(function* (
  sock: string,
  onMessage: (message: ServerMessage) => void,
) {
  return yield* Effect.gen(function* () {
    while (true) {
      const daemon = yield* connectToDaemon(sock, onMessage).pipe(
        Effect.option,
      );

      if (Option.isSome(daemon)) {
        return daemon.value;
      }

      yield* Effect.sleep("100 millis");
    }
  }).pipe(
    Effect.timeoutOrElse({
      duration: "5 seconds",
      orElse: () => Effect.void,
    }),
  );
});

const startDaemon = Effect.fn("startDaemon")(function* (
  input: ConnectionInput & { socketPath: string },
) {
  yield* Effect.try({
    try: () => {
      const bun =
        input.options?.bunPath ??
        (typeof Bun === "undefined"
          ? process.env.BUN_INSTALL
            ? path.join(process.env.BUN_INSTALL, "bin", "bun")
            : "bun"
          : (Bun.which("bun") ?? process.execPath));
      const daemonPath = path.join(import.meta.dirname, "daemon.js");
      const daemonArgs = [
        daemonPath,
        "--db",
        input.dbPath,
        "--hash",
        input.hash,
        "--socket",
        input.socketPath,
        "--data-dir",
        input.dataDir,
      ];
      if (input.options?.idleTimeoutMillis !== undefined) {
        daemonArgs.push(
          "--idle-timeout-ms",
          String(input.options.idleTimeoutMillis),
        );
      }
      const child = spawn(bun, daemonArgs, {
        detached: true,
        stdio: "ignore",
      });
      child.unref();
    },
    catch: (cause) => new DaemonSpawnFailed({ cause }),
  });
});

export default { id: "opencode-live", server };
