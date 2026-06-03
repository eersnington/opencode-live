import { describe, expect, test } from "bun:test";
import { Effect } from "effect";
import { EventEmitter } from "node:events";
import { findBunfsCandidates } from "../src/bunfs-global-bus.js";
import { captureGlobalBus } from "../src/global-bus-capture.js";
import type { GlobalBusEvent } from "../src/global-bus.js";

describe("global bus capture", () => {
  test("captures and validates the bus used by global.event", async () => {
    const bus = new EventEmitter();
    const client = fakeClient(bus);
    const captured = await Effect.runPromise(
      captureGlobalBus({ client, timeoutMillis: 100 }),
    );

    expect(captured).toBeDefined();

    const received: GlobalBusEvent[] = [];
    bus.on("event", (event) => {
      received.push(event);
    });

    captured?.emit("event", {
      directory: "/tmp/project",
      payload: {
        id: "evt_test",
        type: "message.updated",
        properties: { sessionID: "ses_1" },
      },
    });

    expect(received[0]).toEqual({
      directory: "/tmp/project",
      payload: {
        id: "evt_test",
        type: "message.updated",
        properties: { sessionID: "ses_1" },
      },
    });
  });

  test("returns undefined when the captured emitter does not echo the probe", async () => {
    const bus = new EventEmitter();
    const client = {
      global: {
        async event() {
          bus.on("event", () => undefined);
          return { stream: new AsyncEventQueue() };
        },
      },
    };

    await expect(
      Effect.runPromise(captureGlobalBus({ client, timeoutMillis: 10 })),
    ).resolves.toBeUndefined();
  });

  test("captures when global.event subscribes after the first stream pull", async () => {
    const bus = new EventEmitter();
    const client = {
      global: {
        async event(options?: { signal?: AbortSignal }) {
          return {
            stream: new DeferredSubscriptionStream(bus, options?.signal),
          };
        },
      },
    };

    const captured = await Effect.runPromise(
      captureGlobalBus({ client, timeoutMillis: 100 }),
    );

    expect(captured).toBeDefined();
  });

  test("waits for delayed global.event subscriptions", async () => {
    const bus = new EventEmitter();
    const client = {
      global: {
        async event(options?: { signal?: AbortSignal }) {
          return {
            stream: new DelayedSubscriptionStream(bus, 50, options?.signal),
          };
        },
      },
    };

    const captured = await Effect.runPromise(
      captureGlobalBus({ client, timeoutMillis: 150 }),
    );

    expect(captured).toBeDefined();
  });

  test("validates a later candidate when the first candidate is wrong", async () => {
    const wrong = new EventEmitter();
    const bus = new EventEmitter();
    const client = {
      global: {
        async event(options?: { signal?: AbortSignal }) {
          wrong.on("event", () => undefined);
          return {
            stream: new DeferredSubscriptionStream(bus, options?.signal),
          };
        },
      },
    };
    const captured = await Effect.runPromise(
      captureGlobalBus({ client, timeoutMillis: 100 }),
    );
    const received: GlobalBusEvent[] = [];

    bus.on("event", (event) => {
      received.push(event);
    });
    captured?.emit("event", {
      directory: "/tmp/project",
      payload: { id: "evt_test", type: "message.updated", properties: {} },
    });

    expect(received).toHaveLength(1);
  });

  test("captures addListener registrations", async () => {
    const bus = new EventEmitter();
    const client = {
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
    };

    const captured = await Effect.runPromise(
      captureGlobalBus({ client, timeoutMillis: 100 }),
    );

    expect(captured).toBeDefined();
  });

  test("finds Bun virtual chunk candidates", () => {
    const binaryText =
      'import{Gp as L,Ir as P}from"/$bunfs/root/chunk-abc123.js";import{X as Y}from"/$bunfs/root/chunk-other.js";'.padEnd(
        10_000,
        ".",
      ) + 'L.on("event",()=>{});P.emit("other",{});';

    expect(findBunfsCandidates(binaryText)).toEqual([
      {
        exportName: "Gp",
        localName: "L",
        specifier: "/$bunfs/root/chunk-abc123.js",
      },
    ]);
  });

  test("validates Bun virtual chunk candidates", async () => {
    const queue = new AsyncEventQueue();
    const bus = {
      emit(_eventName: "event", event: GlobalBusEvent) {
        queue.push(event);
        return true;
      },
    };
    const client = noSubscriptionClient(queue);
    const binaryText =
      'import{Gp as L}from"/$bunfs/root/chunk-abc123.js";L.emit("event",{payload:{}});';
    const captured = await Effect.runPromise(
      captureGlobalBus({
        client,
        timeoutMillis: 100,
        scanBinary: () => Promise.resolve(binaryText),
        importModule: () => Promise.resolve({ Gp: bus }),
      }),
    );

    expect(captured).toBeDefined();
  });
});

function fakeClient(bus: EventEmitter) {
  return {
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
  };
}

function noSubscriptionClient(queue: AsyncEventQueue) {
  return {
    global: {
      async event(options?: { signal?: AbortSignal }) {
        options?.signal?.addEventListener("abort", () => {
          queue.close();
        });
        return { stream: queue };
      },
    },
  };
}

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
    const value = this.values.shift();

    if (value) {
      return Promise.resolve({ done: false, value });
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

class DeferredSubscriptionStream implements AsyncIterator<unknown> {
  private readonly queue = new AsyncEventQueue();
  private readonly handler = (event: GlobalBusEvent) => {
    this.queue.push(event);
  };
  private pulled = false;
  private subscribed = false;

  constructor(
    private readonly bus: EventEmitter,
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

    if (!this.subscribed) {
      this.subscribed = true;
      this.bus.on("event", this.handler);
    }

    return this.queue.next();
  }

  return(value?: unknown): Promise<IteratorResult<unknown>> {
    this.unsubscribe();
    return this.queue.return(value);
  }

  private unsubscribe() {
    if (!this.subscribed) {
      return;
    }

    this.subscribed = false;
    this.bus.off("event", this.handler);
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
