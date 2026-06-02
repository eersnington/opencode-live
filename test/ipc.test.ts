import { describe, expect, test } from "bun:test";
import { Effect } from "effect";
import fs from "node:fs/promises";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { Schema } from "effect";
import { connectIpc, createIpcServer, type IpcPeer } from "../src/ipc.js";
import { DbHashSchema, ProcessIDSchema } from "../src/protocol.js";

const dbHash = Schema.decodeUnknownSync(DbHashSchema)(
  "11111111111111111111111111111111",
);
const firstProcessID = Schema.decodeUnknownSync(ProcessIDSchema)("proc_1");
const secondProcessID = Schema.decodeUnknownSync(ProcessIDSchema)("proc_2");

describe("ipc", () => {
  test("broadcasts relay events to other clients", async () => {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), "opencode-live-ipc-"));
    const socketPath = path.join(dir, "daemon.sock");
    const clients = new Set<IpcPeer>();
    const received: unknown[] = [];
    const server = createIpcServer({
      socketPath,
      onClient() {},
      onClose(client) {
        clients.delete(client);
      },
      onMessage(client, message) {
        if (message.type === "hello") {
          clients.add(client);
          return;
        }
        if (message.type === "event") {
          for (const target of clients) {
            if (target !== client) target.send(message);
          }
        }
      },
    });

    try {
      await listen(server, socketPath);
      const first = await Effect.runPromise(
        connectIpc({ socketPath, onMessage() {} }),
      );
      const second = await Effect.runPromise(
        connectIpc({
          socketPath,
          onMessage(message) {
            received.push(message);
          },
        }),
      );
      first.send({
        type: "hello",
        processID: firstProcessID,
        dbPath: "/tmp/opencode.db",
        dbHash,
        directory: "/tmp/project",
      });
      second.send({
        type: "hello",
        processID: secondProcessID,
        dbPath: "/tmp/opencode.db",
        dbHash,
        directory: "/tmp/project",
      });
      await Bun.sleep(10);

      first.send({
        type: "event",
        originProcessID: firstProcessID,
        directory: "/tmp/project",
        event: { type: "message.updated", properties: { sessionID: "ses_1" } },
      });

      expect(await waitForFirst(received)).toEqual({
        type: "event",
        originProcessID: firstProcessID,
        directory: "/tmp/project",
        event: { type: "message.updated", properties: { sessionID: "ses_1" } },
      });
      first.close();
      second.close();
    } finally {
      server.close();
      await fs.rm(dir, { recursive: true, force: true });
    }
  });

  test("reports malformed client messages and keeps the socket usable", async () => {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), "opencode-live-ipc-"));
    const socketPath = path.join(dir, "daemon.sock");
    const errors: string[] = [];
    const messages: unknown[] = [];
    const server = createIpcServer({
      socketPath,
      onClient() {},
      onClose() {},
      onError(_client, error) {
        errors.push(error instanceof Error ? error.message : String(error));
      },
      onMessage(_client, message) {
        messages.push(message);
      },
    });

    try {
      await listen(server, socketPath);
      const socket = await connectRaw(socketPath);
      socket.write(
        '{"type":"event","originProcessID":"proc_1","directory":"/tmp/project","event":{"type":"wat","properties":{}}}\n',
      );

      expect(await waitForFirst(errors)).toContain("type");

      socket.write(
        '{"type":"hello","processID":"proc_1","dbPath":"/tmp/opencode.db","dbHash":"11111111111111111111111111111111","directory":"/tmp/project"}\n',
      );

      expect(await waitForFirst(messages)).toEqual({
        type: "hello",
        processID: firstProcessID,
        dbPath: "/tmp/opencode.db",
        dbHash,
        directory: "/tmp/project",
      });
      socket.end();
    } finally {
      server.close();
      await fs.rm(dir, { recursive: true, force: true });
    }
  });
});

function listen(server: net.Server, socketPath: string) {
  return new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(socketPath, () => {
      server.off("error", reject);
      resolve();
    });
  });
}

function connectRaw(socketPath: string) {
  return new Promise<net.Socket>((resolve, reject) => {
    const socket = net.createConnection(socketPath);
    socket.once("error", reject);
    socket.once("connect", () => {
      socket.off("error", reject);
      resolve(socket);
    });
  });
}

async function waitForFirst(values: unknown[]) {
  for (let attempt = 0; attempt < 25; attempt++) {
    const first = values[0];
    if (first) return first;
    await Bun.sleep(10);
  }
  throw new Error("Timed out waiting for IPC event");
}
