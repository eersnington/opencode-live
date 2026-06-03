import { describe, expect, test } from "bun:test";
import { Effect, Schema } from "effect";
import { ProcessIDSchema } from "../src/protocol.js";
import { makeRefresh } from "../src/refresh.js";

const processID = Schema.decodeUnknownSync(ProcessIDSchema)("proc_1");

describe("refresh", () => {
  test("prefers importable GlobalBus", async () => {
    const emitted: unknown[] = [];
    const refresh = await Effect.runPromise(
      makeRefresh({
        client: {},
        async importGlobalBus() {
          return {
            GlobalBus: {
              emit(_eventName: "event", event: unknown) {
                emitted.push(event);
                return true;
              },
            },
          };
        },
      }),
    );

    expect(refresh.mode).toBe("global-bus-import");

    await Effect.runPromise(
      refresh.publish({
        type: "event",
        originProcessID: processID,
        directory: "/tmp/project",
        event: {
          id: "evt_1",
          type: "message.updated",
          properties: { sessionID: "ses_1" },
        },
      }),
    );

    expect(emitted[0]).toEqual({
      directory: "/tmp/project",
      project: undefined,
      workspace: undefined,
      payload: {
        id: "evt_1",
        type: "message.updated",
        properties: { sessionID: "ses_1" },
      },
    });
  });

  test("uses captured GlobalBus when import is unavailable", async () => {
    const emitted: unknown[] = [];
    const refresh = await Effect.runPromise(
      makeRefresh({
        client: {},
        importGlobalBus: () => Promise.reject(new Error("not exported")),
        async captureGlobalBus() {
          return {
            emit(_eventName, event) {
              emitted.push(event);
              return true;
            },
          };
        },
      }),
    );

    expect(refresh.mode).toBe("global-bus-capture");

    await Effect.runPromise(
      refresh.publish({
        type: "event",
        originProcessID: processID,
        directory: "/tmp/project",
        event: {
          type: "message.part.delta",
          properties: { messageID: "msg_1", partID: "prt_1", delta: "hi" },
        },
      }),
    );

    expect(emitted).toHaveLength(1);
  });

  test("does not silently fall back to dispose", async () => {
    const original = globalThis.postMessage;
    Reflect.deleteProperty(globalThis, "postMessage");

    try {
      const refresh = await Effect.runPromise(
        makeRefresh({
          client: {
            instance: {
              dispose() {
                throw new Error("dispose should not be called");
              },
            },
          },
          importGlobalBus: () => Promise.reject(new Error("not exported")),
          captureGlobalBus: () => Promise.resolve(undefined),
        }),
      );

      expect(refresh.mode).toBe("none");
    } finally {
      if (original !== undefined) {
        Reflect.set(globalThis, "postMessage", original);
      }
    }
  });

  test("uses worker RPC when GlobalBus paths are unavailable", async () => {
    const messages: string[] = [];
    const original = globalThis.postMessage;
    Reflect.set(globalThis, "postMessage", (message: string) => {
      messages.push(message);
    });

    try {
      const refresh = await Effect.runPromise(
        makeRefresh({
          client: {},
          importGlobalBus: () => Promise.reject(new Error("not exported")),
          captureGlobalBus: () => Promise.resolve(undefined),
        }),
      );

      expect(refresh.mode).toBe("worker-rpc");

      await Effect.runPromise(
        refresh.publish({
          type: "event",
          originProcessID: processID,
          directory: "/tmp/project",
          projectID: "project_1",
          event: {
            id: "evt_1",
            type: "message.updated",
            properties: { sessionID: "ses_1" },
          },
        }),
      );

      expect(JSON.parse(messages[0] ?? "{}")).toEqual({
        type: "rpc.event",
        event: "global.event",
        data: {
          directory: "/tmp/project",
          project: "project_1",
          payload: {
            id: "evt_1",
            type: "message.updated",
            properties: { sessionID: "ses_1" },
          },
        },
      });
    } finally {
      if (original === undefined) {
        Reflect.deleteProperty(globalThis, "postMessage");
      } else {
        Reflect.set(globalThis, "postMessage", original);
      }
    }
  });
});
