import { describe, expect, test } from "bun:test";
import { Effect, Schema } from "effect";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { connectIpc } from "../src/ipc.js";
import { DbHashSchema, ProcessIDSchema } from "../src/protocol.js";

const daemonPath = fileURLToPath(new URL("../src/daemon.ts", import.meta.url));
const dbHash = Schema.decodeUnknownSync(DbHashSchema)(
  "11111111111111111111111111111111",
);
const firstProcessID = Schema.decodeUnknownSync(ProcessIDSchema)("proc_1");
const secondProcessID = Schema.decodeUnknownSync(ProcessIDSchema)("proc_2");

describe("daemon", () => {
  test("relays events between connected plugin clients", async () => {
    const dir = await fs.mkdtemp(
      path.join(os.tmpdir(), "opencode-live-daemon-"),
    );
    const dataDir = path.join(dir, "data");
    const socketPath = path.join(dir, "daemon.sock");
    const dbPath = path.join(dir, "opencode.db");
    const received: unknown[] = [];
    const daemon = Bun.spawn(
      [
        process.execPath,
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
      { stderr: "pipe", stdout: "pipe" },
    );

    try {
      await waitForPath(socketPath);
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
      await Bun.sleep(10);

      const message = {
        type: "event",
        originProcessID: firstProcessID,
        directory: "/tmp/project",
        event: { type: "message.updated", properties: { sessionID: "ses_1" } },
      } as const;
      for (let attempt = 0; attempt < 10 && received.length === 0; attempt++) {
        first.send(message);
        await Bun.sleep(20);
      }

      expect(await waitForFirst(received)).toEqual(message);

      first.send({ type: "shutdown" });
      await daemon.exited;
      first.close();
      second.close();
    } finally {
      daemon.kill();
      await fs.rm(dir, { recursive: true, force: true });
    }
  });
});

async function waitForPath(file: string) {
  for (let attempt = 0; attempt < 200; attempt++) {
    if (await exists(file)) return;
    await Bun.sleep(10);
  }
  throw new Error(`Timed out waiting for ${file}`);
}

async function exists(file: string) {
  return fs.access(file).then(
    () => true,
    () => false,
  );
}

async function waitForFirst(values: unknown[]) {
  for (let attempt = 0; attempt < 25; attempt++) {
    const first = values[0];
    if (first) return first;
    await Bun.sleep(10);
  }
  throw new Error("Timed out waiting for daemon event");
}
