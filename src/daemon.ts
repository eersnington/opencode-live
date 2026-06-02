#!/usr/bin/env bun
import { BunRuntime } from "@effect/platform-bun";
import fs from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { Clock, Effect, Result, Schema } from "effect";
import { daemonRegistryFile, writeDaemonRegistry } from "./daemon-registry.js";
import { createIpcServer, type IpcPeer } from "./ipc.js";
import { DbHashSchema, type ClientMessage, type DbHash } from "./protocol.js";

type DaemonConfig = {
  dbPath: string;
  dbHash: DbHash;
  socketPath: string;
  dataDir?: string;
};

class DaemonArgsInvalid extends Schema.TaggedErrorClass<DaemonArgsInvalid>()(
  "DaemonArgsInvalid",
  { message: Schema.String },
) {}

const daemonProgram = Effect.gen(function* () {
  const config = yield* readDaemonConfig(process.argv.slice(2));
  const daemonPath = fileURLToPath(import.meta.url);
  const daemonStat = yield* Effect.tryPromise(() => fs.stat(daemonPath)).pipe(
    Effect.option,
  );
  const startedAt = yield* Clock.currentTimeMillis;
  const peers = new Set<IpcPeer>();
  const clients = new Set<IpcPeer>();
  let shuttingDown = false;

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
      daemonPath,
      daemonMtime:
        daemonStat._tag === "Some" ? daemonStat.value.mtimeMs : undefined,
    },
    config.dataDir,
  );

  function shutdown() {
    if (shuttingDown) {
      return;
    }

    shuttingDown = true;
    server.close();

    for (const peer of peers) {
      peer.close();
    }

    void Promise.all([
      fs.rm(
        daemonRegistryFile({ dbHash: config.dbHash, dataDir: config.dataDir }),
        {
          force: true,
        },
      ),
      fs.rm(config.socketPath, { force: true }),
    ]).finally(() => process.exit(0));
  }

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
      handleMessage({ client, clients, message, shutdown });
    },
  });

  yield* Effect.sync(() => {
    server.listen(config.socketPath);
    process.on("SIGTERM", shutdown);
    process.on("SIGINT", shutdown);
  });

  return yield* Effect.never;
});

const readDaemonConfig = Effect.fn("readDaemonConfig")(function* (
  argv: string[],
): Effect.fn.Return<DaemonConfig, DaemonArgsInvalid> {
  const { values } = yield* Effect.try({
    try: () =>
      parseArgs({
        args: argv,
        options: {
          db: { type: "string" },
          hash: { type: "string" },
          socket: { type: "string" },
          "data-dir": { type: "string" },
        },
        strict: true,
      }),
    catch: (cause) =>
      new DaemonArgsInvalid({
        message: `Invalid daemon arguments: ${cause}`,
      }),
  });

  if (!values.db || !values.hash || !values.socket) {
    return yield* new DaemonArgsInvalid({
      message:
        "Usage: daemon --db <path> --hash <hash> --socket <socket> [--data-dir <dir>]",
    });
  }

  const dbHash = Schema.decodeUnknownResult(DbHashSchema)(values.hash);

  if (Result.isFailure(dbHash)) {
    return yield* new DaemonArgsInvalid({
      message: `Invalid daemon --hash: ${dbHash.failure}`,
    });
  }

  return {
    dbPath: values.db,
    dbHash: dbHash.success,
    socketPath: values.socket,
    dataDir: values["data-dir"],
  };
});

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

BunRuntime.runMain(daemonProgram);
