import { Effect } from "effect";
import { EventEmitter } from "node:events";
import { findBunfsCandidates } from "../src/bunfs-global-bus.js";
import { captureGlobalBus } from "../src/global-bus-capture.js";
import type { GlobalBusEvent } from "../src/global-bus.js";
import { assert, describe, it } from "@effect/vitest";

describe("global bus capture", () => {
  it.live("captures and validates the bus used by global.event", () =>
    Effect.gen(function* () {
      const bus = new EventEmitter();
      const received: GlobalBusEvent[] = [];
      const captured = yield* captureGlobalBus({
        client: {
          global: {
            async event(options?: { signal?: AbortSignal }) {
              const queue = new AsyncEventQueue();
              const handler = (event: GlobalBusEvent) => {
                queue.push(event);
              };
              bus.on("event", handler);
              queue.push({
                payload: {
                  id: "evt_connected",
                  type: "server.connected",
                  properties: {},
                },
              });
              options?.signal?.addEventListener("abort", () => {
                bus.off("event", handler);
                queue.close();
              });
              return { stream: queue };
            },
          },
        },
        timeoutMillis: 100,
      });

      if (!captured) {
        return yield* Effect.die("Expected GlobalBus capture");
      }

      bus.on("event", (event) => {
        received.push(event);
      });
      captured.emit("event", {
        directory: "/tmp/project",
        payload: {
          id: "evt_test",
          type: "message.updated",
          properties: { sessionID: "ses_1" },
        },
      });

      assert.deepStrictEqual(received[0], {
        directory: "/tmp/project",
        payload: {
          id: "evt_test",
          type: "message.updated",
          properties: { sessionID: "ses_1" },
        },
      });
    }),
  );

  it.live(
    "returns undefined when the captured emitter does not echo the probe",
    () =>
      Effect.gen(function* () {
        const bus = new EventEmitter();
        const captured = yield* captureGlobalBus({
          client: {
            global: {
              async event() {
                bus.on("event", () => undefined);
                return { stream: new AsyncEventQueue() };
              },
            },
          },
          timeoutMillis: 10,
        });

        assert.strictEqual(captured, undefined);
      }),
  );

  it.live("waits for delayed global.event subscriptions", () =>
    Effect.gen(function* () {
      const bus = new EventEmitter();
      const captured = yield* captureGlobalBus({
        client: {
          global: {
            async event(options?: { signal?: AbortSignal }) {
              return {
                stream: new DelayedSubscriptionStream(bus, 50, options?.signal),
              };
            },
          },
        },
        timeoutMillis: 150,
      });

      assert.notStrictEqual(captured, undefined);
    }),
  );

  it.live("validates a later candidate when the first candidate is wrong", () =>
    Effect.gen(function* () {
      const wrong = new EventEmitter();
      const bus = new EventEmitter();
      const received: GlobalBusEvent[] = [];
      const captured = yield* captureGlobalBus({
        client: {
          global: {
            async event(options?: { signal?: AbortSignal }) {
              wrong.on("event", () => undefined);
              return {
                stream: new DelayedSubscriptionStream(bus, 0, options?.signal),
              };
            },
          },
        },
        timeoutMillis: 100,
      });

      if (!captured) {
        return yield* Effect.die("Expected later GlobalBus candidate");
      }

      bus.on("event", (event) => {
        received.push(event);
      });
      captured.emit("event", {
        directory: "/tmp/project",
        payload: { id: "evt_test", type: "message.updated", properties: {} },
      });

      assert.strictEqual(received.length, 1);
    }),
  );

  it.live("captures addListener registrations", () =>
    Effect.gen(function* () {
      const bus = new EventEmitter();
      const captured = yield* captureGlobalBus({
        client: {
          global: {
            async event(options?: { signal?: AbortSignal }) {
              const queue = new AsyncEventQueue();
              const handler = (event: GlobalBusEvent) => {
                queue.push(event);
              };
              bus.addListener("event", handler);
              options?.signal?.addEventListener("abort", () => {
                bus.off("event", handler);
                queue.close();
              });
              return { stream: queue };
            },
          },
        },
        timeoutMillis: 100,
      });

      assert.notStrictEqual(captured, undefined);
    }),
  );

  it("finds Bun virtual chunk candidates", () => {
    const binaryText =
      'import{Gp as L,Ir as P}from"/$bunfs/root/chunk-abc123.js";import{X as Y}from"/$bunfs/root/chunk-other.js";'.padEnd(
        10_000,
        ".",
      ) + 'L.on("event",()=>{});P.emit("other",{});';

    assert.deepStrictEqual(findBunfsCandidates(binaryText), [
      {
        exportName: "Gp",
        localName: "L",
        specifier: "/$bunfs/root/chunk-abc123.js",
      },
    ]);
  });

  it.live("validates Bun virtual chunk candidates", () =>
    Effect.gen(function* () {
      const queue = new AsyncEventQueue();
      const bus = {
        emit(_eventName: "event", event: GlobalBusEvent) {
          queue.push(event);
          return true;
        },
      };
      const binaryText =
        'import{Gp as L}from"/$bunfs/root/chunk-abc123.js";L.emit("event",{payload:{}});';
      const captured = yield* captureGlobalBus({
        client: {
          global: {
            async event(options?: { signal?: AbortSignal }) {
              options?.signal?.addEventListener("abort", () => {
                queue.close();
              });
              return { stream: queue };
            },
          },
        },
        timeoutMillis: 100,
        scanBinary: () => Promise.resolve(binaryText),
        importModule: () => Promise.resolve({ Gp: bus }),
      });

      assert.notStrictEqual(captured, undefined);
    }),
  );
});

class AsyncEventQueue implements AsyncIterator<unknown> {
  private readonly values: unknown[] = [];
  private readonly resolvers: ((result: IteratorResult<unknown>) => void)[] =
    [];
  private closed = false;

  push(value: unknown) {
    const resolver = this.resolvers.shift();

    if (resolver) {
      resolver({ done: false, value });
      return;
    }

    this.values.push(value);
  }

  next(): Promise<IteratorResult<unknown>> {
    if (this.values.length > 0) {
      return Promise.resolve({ done: false, value: this.values.shift() });
    }

    if (this.closed) {
      return Promise.resolve({ done: true, value: undefined });
    }

    return new Promise((resolve) => {
      this.resolvers.push(resolve);
    });
  }

  return(value?: unknown): Promise<IteratorResult<unknown>> {
    this.close();
    return Promise.resolve({ done: true, value });
  }

  close() {
    this.closed = true;

    for (const resolve of this.resolvers.splice(0)) {
      resolve({ done: true, value: undefined });
    }
  }
}

class DelayedSubscriptionStream implements AsyncIterator<unknown> {
  private readonly queue = new AsyncEventQueue();
  private readonly handler = (event: GlobalBusEvent) => {
    this.queue.push(event);
  };
  private pulled = false;
  private subscribed = false;
  private timer: ReturnType<typeof setTimeout> | undefined;

  constructor(
    private readonly bus: EventEmitter,
    private readonly delayMillis: number,
    signal?: AbortSignal,
  ) {
    signal?.addEventListener("abort", () => {
      this.unsubscribe();
      this.queue.close();
    });
  }

  next(): Promise<IteratorResult<unknown>> {
    if (!this.pulled) {
      this.pulled = true;
      return Promise.resolve({
        done: false,
        value: {
          payload: {
            id: "evt_connected",
            type: "server.connected",
            properties: {},
          },
        },
      });
    }

    if (!this.subscribed && !this.timer) {
      this.timer = setTimeout(() => {
        this.timer = undefined;
        this.subscribed = true;
        this.bus.on("event", this.handler);
      }, this.delayMillis);
    }

    return this.queue.next();
  }

  return(value?: unknown): Promise<IteratorResult<unknown>> {
    this.unsubscribe();
    return this.queue.return(value);
  }

  private unsubscribe() {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = undefined;
    }

    if (!this.subscribed) {
      return;
    }

    this.subscribed = false;
    this.bus.off("event", this.handler);
  }
}
