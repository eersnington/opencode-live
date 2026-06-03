import fs from "node:fs/promises";
import { Clock, Deferred, Effect } from "effect";
import { daemonRegistryFile, writeDaemonRegistry } from "./daemon-registry.js";
import { createIpcServer, type IpcPeer } from "./ipc.js";
import type { ClientMessage, DbHash } from "./protocol.js";

export type DaemonRuntimeConfig = {
  dbPath: string;
  dbHash: DbHash;
  socketPath: string;
  dataDir?: string;
  daemonPath?: string;
  signals?: readonly NodeJS.Signals[];
  listening?: Deferred.Deferred<void>;
};

type DaemonRuntimeState = {
  server: ReturnType<typeof createIpcServer>;
  peers: Set<IpcPeer>;
  signalHandlers: Array<{
    signal: NodeJS.Signals;
    handler: () => void;
  }>;
};

export const runDaemon = Effect.fn("runDaemon")(function* (
  config: DaemonRuntimeConfig,
) {
  const shutdown = yield* Deferred.make<void>();

  return yield* Effect.acquireUseRelease(
    acquireDaemonRuntime(config, shutdown),
    () => Deferred.await(shutdown),
    (state) => releaseDaemonRuntime(config, state),
  );
});

const acquireDaemonRuntime = Effect.fn("acquireDaemonRuntime")(function* (
  config: DaemonRuntimeConfig,
  shutdown: Deferred.Deferred<void>,
) {
  const daemonPath = config.daemonPath;
  const daemonStat = daemonPath
    ? yield* Effect.tryPromise(() => fs.stat(daemonPath)).pipe(Effect.option)
    : undefined;
  const startedAt = yield* Clock.currentTimeMillis;
  const peers = new Set<IpcPeer>();
  const clients = new Set<IpcPeer>();
  let shuttingDown = false;
  const requestShutdown = () => {
    if (shuttingDown) {
      return;
    }

    shuttingDown = true;
    Deferred.doneUnsafe(shutdown, Effect.void);
  };

  yield* Effect.tryPromise(() =>
    fs.rm(config.socketPath, { force: true }),
  ).pipe(Effect.ignore);
  yield* writeDaemonRegistry(
    {
      pid: process.pid,
      socketPath: config.socketPath,
      dbPath: config.dbPath,
      dbHash: config.dbHash,
      startedAt,
      daemonPath: config.daemonPath,
      daemonMtime:
        daemonStat?._tag === "Some" ? daemonStat.value.mtimeMs : undefined,
    },
    config.dataDir,
  );

  const server = createIpcServer({
    socketPath: config.socketPath,
    onClient(client) {
      peers.add(client);
    },
    onClose(client) {
      peers.delete(client);
      clients.delete(client);
    },
    onError(client, error) {
      client.send({ type: "error", message: error.message });
    },
    onMessage(client, message) {
      handleMessage({ client, clients, message, shutdown: requestShutdown });
    },
  });

  yield* listen(server, config.socketPath);
  if (config.listening) {
    yield* Deferred.succeed(config.listening, undefined);
  }

  const signalHandlers = (config.signals ?? []).map((signal) => {
    const handler = requestShutdown;
    process.on(signal, handler);
    return { signal, handler };
  });

  return { server, peers, signalHandlers } satisfies DaemonRuntimeState;
});

const releaseDaemonRuntime = Effect.fn("releaseDaemonRuntime")(function* (
  config: DaemonRuntimeConfig,
  state: DaemonRuntimeState,
) {
  for (const { signal, handler } of state.signalHandlers) {
    process.off(signal, handler);
  }

  yield* Effect.sync(() => {
    state.server.close();

    for (const peer of state.peers) {
      peer.close();
    }
  });
  yield* Effect.all(
    [
      Effect.promise(() =>
        fs
          .rm(
            daemonRegistryFile({
              dbHash: config.dbHash,
              dataDir: config.dataDir,
            }),
            { force: true },
          )
          .catch(() => undefined),
      ),
      Effect.promise(() =>
        fs.rm(config.socketPath, { force: true }).catch(() => undefined),
      ),
    ],
    { concurrency: "unbounded" },
  ).pipe(Effect.ignore);
});

function listen(
  server: ReturnType<typeof createIpcServer>,
  socketPath: string,
) {
  return Effect.callback<void, Error>((resume) => {
    const onError = (error: Error) => resume(Effect.fail(error));

    server.once("error", onError);
    server.listen(socketPath, () => {
      server.off("error", onError);
      resume(Effect.void);
    });

    return Effect.sync(() => {
      server.off("error", onError);
    });
  });
}

function handleMessage(input: {
  client: IpcPeer;
  clients: Set<IpcPeer>;
  message: ClientMessage;
  shutdown: () => void;
}) {
  if (input.message.type === "hello") {
    input.clients.add(input.client);
    return;
  }

  if (input.message.type === "event") {
    for (const client of input.clients) {
      if (client === input.client) {
        continue;
      }

      client.send(input.message);
    }
    return;
  }

  input.shutdown();
}
