#!/usr/bin/env bun
import { BunRuntime } from "@effect/platform-bun";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { Effect, Result, Schema } from "effect";
import { runDaemon } from "./daemon-runtime.js";
import { DbHashSchema, type DbHash } from "./protocol.js";

type DaemonConfig = {
  dbPath: string;
  dbHash: DbHash;
  socketPath: string;
  dataDir?: string;
  idleTimeoutMillis?: number;
};

class DaemonArgsInvalid extends Schema.TaggedErrorClass<DaemonArgsInvalid>()(
  "DaemonArgsInvalid",
  { message: Schema.String },
) {}

const daemonProgram = Effect.gen(function* () {
  const config = yield* readDaemonConfig(process.argv.slice(2));
  const daemonPath = fileURLToPath(import.meta.url);
  return yield* runDaemon({
    ...config,
    daemonPath,
    signals: ["SIGTERM", "SIGINT"],
  });
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
          "idle-timeout-ms": { type: "string" },
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
        "Usage: daemon --db <path> --hash <hash> --socket <socket> [--data-dir <dir>] [--idle-timeout-ms <milliseconds>]",
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
    idleTimeoutMillis: yield* readIdleTimeoutMillis(
      values["idle-timeout-ms"] ?? process.env.OPENCODE_LIVE_IDLE_TIMEOUT_MS,
    ),
  };
});

const readIdleTimeoutMillis = Effect.fn("readIdleTimeoutMillis")(function* (
  value: string | undefined,
): Effect.fn.Return<number | undefined, DaemonArgsInvalid> {
  if (value === undefined) {
    return undefined;
  }

  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 0) {
    return yield* new DaemonArgsInvalid({
      message:
        "Invalid daemon --idle-timeout-ms: expected a non-negative number of milliseconds",
    });
  }

  return parsed;
});

BunRuntime.runMain(daemonProgram);
