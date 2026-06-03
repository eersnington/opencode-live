import { Effect, Schema } from "effect";
import { ProcessIDSchema } from "../src/protocol.js";
import { makeRefresh } from "../src/refresh.js";
import { assert, describe, it } from "@effect/vitest";

const processID = Schema.decodeUnknownSync(ProcessIDSchema)("proc_1");
const WorkerRpcEnvelopeJson = Schema.fromJsonString(Schema.Unknown);

describe("refresh", () => {
  it.effect("prefers importable GlobalBus", () =>
    Effect.gen(function* () {
      const emitted: unknown[] = [];
      const refresh = yield* makeRefresh({
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
      });

      assert.strictEqual(refresh.mode, "global-bus-import");

      yield* refresh.publish({
        type: "event",
        originProcessID: processID,
        directory: "/tmp/project",
        event: {
          id: "evt_1",
          type: "message.updated",
          properties: { sessionID: "ses_1" },
        },
      });

      assert.deepStrictEqual(emitted[0], {
        directory: "/tmp/project",
        project: undefined,
        workspace: undefined,
        payload: {
          id: "evt_1",
          type: "message.updated",
          properties: { sessionID: "ses_1" },
        },
      });
    }),
  );

  it.effect("uses captured GlobalBus when import is unavailable", () =>
    Effect.gen(function* () {
      const emitted: unknown[] = [];
      const refresh = yield* makeRefresh({
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
      });

      assert.strictEqual(refresh.mode, "global-bus-capture");

      yield* refresh.publish({
        type: "event",
        originProcessID: processID,
        directory: "/tmp/project",
        event: {
          type: "message.part.delta",
          properties: { messageID: "msg_1", partID: "prt_1", delta: "hi" },
        },
      });

      assert.deepStrictEqual(emitted[0], {
        directory: "/tmp/project",
        project: undefined,
        workspace: undefined,
        payload: {
          id: undefined,
          type: "message.part.delta",
          properties: { messageID: "msg_1", partID: "prt_1", delta: "hi" },
        },
      });
    }),
  );

  it.effect("passes serverUrl into GlobalBus capture", () =>
    Effect.gen(function* () {
      let seenServerUrl: URL | undefined;
      const refresh = yield* makeRefresh({
        client: {},
        serverUrl: new URL("http://127.0.0.1:54321"),
        importGlobalBus: () => Promise.reject(new Error("not exported")),
        async captureGlobalBus(input) {
          seenServerUrl = input.serverUrl;
          return {
            emit() {
              return true;
            },
          };
        },
      });

      assert.strictEqual(refresh.mode, "global-bus-capture");
      assert.strictEqual(seenServerUrl?.href, "http://127.0.0.1:54321/");
    }),
  );

  it.effect("does not silently fall back to dispose", () =>
    withPostMessage(undefined)(
      Effect.gen(function* () {
        const refresh = yield* makeRefresh({
          client: {
            instance: {
              dispose() {
                throw new Error("dispose should not be called");
              },
            },
          },
          importGlobalBus: () => Promise.reject(new Error("not exported")),
          captureGlobalBus: () => Promise.resolve(undefined),
        });

        assert.strictEqual(refresh.mode, "none");
      }),
    ),
  );

  it.effect("uses worker RPC when GlobalBus paths are unavailable", () =>
    Effect.gen(function* () {
      const messages: string[] = [];

      yield* withPostMessage((message: string) => {
        messages.push(message);
      })(
        Effect.gen(function* () {
          const refresh = yield* makeRefresh({
            client: {},
            importGlobalBus: () => Promise.reject(new Error("not exported")),
            captureGlobalBus: () => Promise.resolve(undefined),
          });

          assert.strictEqual(refresh.mode, "worker-rpc");

          yield* refresh.publish({
            type: "event",
            originProcessID: processID,
            directory: "/tmp/project",
            projectID: "project_1",
            event: {
              id: "evt_1",
              type: "message.updated",
              properties: { sessionID: "ses_1" },
            },
          });
        }),
      );

      assert.deepStrictEqual(
        Schema.decodeUnknownSync(WorkerRpcEnvelopeJson)(messages[0] ?? "{}"),
        {
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
        },
      );
    }),
  );
});

function withPostMessage(value: ((message: string) => void) | undefined) {
  return <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    Effect.acquireUseRelease(
      Effect.sync(() => globalThis.postMessage),
      () =>
        Effect.sync(() => {
          if (value === undefined) {
            Reflect.deleteProperty(globalThis, "postMessage");
            return;
          }

          Reflect.set(globalThis, "postMessage", value);
        }).pipe(Effect.andThen(effect)),
      (original) =>
        Effect.sync(() => {
          if (original === undefined) {
            Reflect.deleteProperty(globalThis, "postMessage");
            return;
          }

          Reflect.set(globalThis, "postMessage", original);
        }),
    );
}
