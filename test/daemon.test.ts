import { assert, describe, it } from "@effect/vitest";
import { Deferred, Effect, Fiber, Queue, Schema } from "effect";
import { TestClock } from "effect/testing";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { daemonRegistryFile } from "../src/daemon-registry.js";
import { manageIdleShutdown, runDaemon } from "../src/daemon-runtime.js";
import { connectIpc, type IpcPeer } from "../src/ipc.js";
import { DbHashSchema, ProcessIDSchema } from "../src/protocol.js";

const dbHash = Schema.decodeUnknownSync(DbHashSchema)(
  "11111111111111111111111111111111",
);
const firstProcessID = Schema.decodeUnknownSync(ProcessIDSchema)("proc_1");
const secondProcessID = Schema.decodeUnknownSync(ProcessIDSchema)("proc_2");

describe("daemon", () => {
  it.effect("shuts down only after remaining idle for the full timeout", () =>
    Effect.gen(function* () {
      const peerCounts = yield* Queue.sliding<number>(1);
      const shutdown = yield* Deferred.make<void>();
      yield* manageIdleShutdown({
        peerCounts,
        idleTimeoutMillis: 5_000,
        requestShutdown() {
          Deferred.doneUnsafe(shutdown, Effect.void);
        },
      }).pipe(Effect.forkScoped);

      Queue.offerUnsafe(peerCounts, 0);
      yield* TestClock.adjust("4 seconds");
      assert.isFalse(yield* Deferred.isDone(shutdown));

      Queue.offerUnsafe(peerCounts, 1);
      yield* TestClock.adjust("2 seconds");
      assert.isFalse(yield* Deferred.isDone(shutdown));

      Queue.offerUnsafe(peerCounts, 0);
      yield* TestClock.adjust("5 seconds");
      assert.isTrue(yield* Deferred.isDone(shutdown));
    }),
  );

  it.live("exits and cleans daemon files after the last peer stays idle", () =>
    Effect.acquireUseRelease(
      Effect.promise(() =>
        fs.mkdtemp(path.join(os.tmpdir(), "opencode-live-daemon-")),
      ),
      (dir) =>
        Effect.gen(function* () {
          const dataDir = path.join(dir, "data");
          const socketPath = path.join(dir, "daemon.sock");
          const dbPath = path.join(dir, "opencode.db");
          const registryPath = daemonRegistryFile({ dbHash, dataDir });
          const listening = yield* Deferred.make<void>();
          const daemon = yield* runDaemon({
            dbPath,
            dbHash,
            socketPath,
            dataDir,
            listening,
            idleTimeoutMillis: 50,
          }).pipe(Effect.forkScoped);

          yield* Deferred.await(listening);
          const peer = yield* connectIpc({ socketPath, onMessage() {} });
          peer.send({
            type: "hello",
            processID: firstProcessID,
            dbPath,
            dbHash,
            directory: "/tmp/project",
          });
          peer.close();

          yield* Fiber.join(daemon);
          const registryStat = yield* Effect.tryPromise(() =>
            fs.stat(registryPath),
          ).pipe(Effect.option);
          const socketStat = yield* Effect.tryPromise(() =>
            fs.stat(socketPath),
          ).pipe(Effect.option);

          assert.strictEqual(registryStat._tag, "None");
          assert.strictEqual(socketStat._tag, "None");
        }),
      (dir) =>
        Effect.promise(() => fs.rm(dir, { recursive: true, force: true })),
    ),
  );

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
