import { assert, describe, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import fs from "node:fs/promises";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { createIpcServer } from "../src/ipc.js";
import { DbHashSchema, ProcessIDSchema } from "../src/protocol.js";

const dbHash = Schema.decodeUnknownSync(DbHashSchema)(
  "11111111111111111111111111111111",
);
const firstProcessID = Schema.decodeUnknownSync(ProcessIDSchema)("proc_1");

describe("ipc", () => {
  it.live("reports malformed client messages and keeps the socket usable", () =>
    Effect.acquireUseRelease(
      Effect.promise(() =>
        fs.mkdtemp(path.join(os.tmpdir(), "opencode-live-ipc-")),
      ),
      (dir) =>
        Effect.gen(function* () {
          const socketPath = path.join(dir, "daemon.sock");
          const errors: string[] = [];
          const messages: unknown[] = [];
          const server = createIpcServer({
            socketPath,
            onClient() {},
            onClose() {},
            onError(_client, error) {
              errors.push(error.message);
            },
            onMessage(_client, message) {
              messages.push(message);
            },
          });

          return yield* Effect.gen(function* () {
            yield* listen(server, socketPath);
            const socket = yield* connectRaw(socketPath);
            socket.write(
              '{"type":"event","originProcessID":"proc_1","directory":"/tmp/project","event":{"type":"wat","properties":{}}}\n',
            );

            for (
              let attempt = 0;
              attempt < 25 && errors.length === 0;
              attempt++
            ) {
              yield* Effect.sleep("10 millis");
            }
            assert.include(errors[0] ?? "", "type");

            socket.write(
              '{"type":"hello","processID":"proc_1","dbPath":"/tmp/opencode.db","dbHash":"11111111111111111111111111111111","directory":"/tmp/project"}\n',
            );

            for (
              let attempt = 0;
              attempt < 25 && messages.length === 0;
              attempt++
            ) {
              yield* Effect.sleep("10 millis");
            }
            assert.deepStrictEqual(messages[0], {
              type: "hello",
              processID: firstProcessID,
              dbPath: "/tmp/opencode.db",
              dbHash,
              directory: "/tmp/project",
            });
            socket.end();
          }).pipe(Effect.ensuring(Effect.sync(() => server.close())));
        }),
      (dir) =>
        Effect.promise(() => fs.rm(dir, { recursive: true, force: true })),
    ),
  );
});

function listen(server: net.Server, socketPath: string) {
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

function connectRaw(socketPath: string) {
  return Effect.callback<net.Socket, Error>((resume) => {
    const socket = net.createConnection(socketPath);
    const onError = (error: Error) => resume(Effect.fail(error));

    socket.once("error", onError);
    socket.once("connect", () => {
      socket.off("error", onError);
      resume(Effect.succeed(socket));
    });

    return Effect.sync(() => {
      socket.off("error", onError);
      socket.destroy();
    });
  });
}
