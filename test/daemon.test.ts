import { assert, describe, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import { spawn, type ChildProcess } from "node:child_process";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { connectIpc, type IpcPeer } from "../src/ipc.js";
import { DbHashSchema, ProcessIDSchema } from "../src/protocol.js";

const daemonPath = fileURLToPath(new URL("../src/daemon.ts", import.meta.url));
const dbHash = Schema.decodeUnknownSync(DbHashSchema)(
  "11111111111111111111111111111111",
);
const firstProcessID = Schema.decodeUnknownSync(ProcessIDSchema)("proc_1");
const secondProcessID = Schema.decodeUnknownSync(ProcessIDSchema)("proc_2");

describe("daemon", () => {
  it.live("relays events between connected plugin clients", () =>
    Effect.acquireUseRelease(
      Effect.promise(() =>
        fs.mkdtemp(path.join(os.tmpdir(), "opencode-live-daemon-")),
      ),
      (dir) =>
        Effect.gen(function* () {
          const dataDir = path.join(dir, "data");
          const socketPath = path.join(dir, "daemon.sock");
          const dbPath = path.join(dir, "opencode.db");
          const daemon = spawn(
            "bun",
            [
              daemonPath,
              "--db",
              dbPath,
              "--hash",
              dbHash,
              "--socket",
              socketPath,
              "--data-dir",
              dataDir,
            ],
            { stdio: ["ignore", "pipe", "pipe"] },
          );
          let first: IpcPeer | undefined;
          let second: IpcPeer | undefined;
          const received: unknown[] = [];

          return yield* Effect.gen(function* () {
            yield* waitForSocketPath(socketPath);
            first = yield* connectIpc({ socketPath, onMessage() {} });
            second = yield* connectIpc({
              socketPath,
              onMessage(message) {
                received.push(message);
              },
            });
            first.send({
              type: "hello",
              processID: firstProcessID,
              dbPath,
              dbHash,
              directory: "/tmp/project",
            });
            second.send({
              type: "hello",
              processID: secondProcessID,
              dbPath,
              dbHash,
              directory: "/tmp/project",
            });
            yield* Effect.sleep("10 millis");

            const message = {
              type: "event",
              originProcessID: firstProcessID,
              directory: "/tmp/project",
              event: {
                type: "message.updated",
                properties: { sessionID: "ses_1" },
              },
            } as const;
            for (
              let attempt = 0;
              attempt < 10 && received.length === 0;
              attempt++
            ) {
              first.send(message);
              yield* Effect.sleep("20 millis");
            }

            for (
              let attempt = 0;
              attempt < 25 && received.length === 0;
              attempt++
            ) {
              yield* Effect.sleep("10 millis");
            }

            assert.deepStrictEqual(received[0], message);

            first.send({ type: "shutdown" });
            yield* waitForExit(daemon);
          }).pipe(
            Effect.ensuring(
              Effect.sync(() => {
                first?.close();
                second?.close();
                daemon.kill();
              }),
            ),
          );
        }),
      (dir) =>
        Effect.promise(() => fs.rm(dir, { recursive: true, force: true })),
    ),
  );
});

function waitForSocketPath(file: string) {
  return Effect.gen(function* () {
    for (let attempt = 0; attempt < 200; attempt++) {
      const access = yield* Effect.tryPromise(() => fs.access(file)).pipe(
        Effect.option,
      );

      if (access._tag === "Some") {
        return;
      }

      yield* Effect.sleep("10 millis");
    }

    return yield* Effect.die(`Timed out waiting for ${file}`);
  });
}

function waitForExit(child: ChildProcess) {
  return Effect.callback<void, Error>((resume) => {
    const onError = (error: Error) => resume(Effect.fail(error));

    child.once("error", onError);
    child.once("exit", () => {
      child.off("error", onError);
      resume(Effect.void);
    });

    return Effect.sync(() => {
      child.off("error", onError);
    });
  });
}
