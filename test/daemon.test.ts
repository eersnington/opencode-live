import { assert, describe, it } from "@effect/vitest";
import { Deferred, Effect, Fiber, Schema } from "effect";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { runDaemon } from "../src/daemon-runtime.js";
import { connectIpc, type IpcPeer } from "../src/ipc.js";
import { DbHashSchema, ProcessIDSchema } from "../src/protocol.js";

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
          const listening = yield* Deferred.make<void>();
          const daemon = yield* runDaemon({
            dbPath,
            dbHash,
            socketPath,
            dataDir,
            listening,
          }).pipe(Effect.forkScoped);
          let first: IpcPeer | undefined;
          let second: IpcPeer | undefined;
          const received: unknown[] = [];

          return yield* Effect.gen(function* () {
            yield* Deferred.await(listening);
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
            yield* Fiber.join(daemon);
          }).pipe(
            Effect.ensuring(
              Effect.sync(() => {
                first?.close();
                second?.close();
              }),
            ),
          );
        }),
      (dir) =>
        Effect.promise(() => fs.rm(dir, { recursive: true, force: true })),
    ),
  );
});
