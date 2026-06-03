import fs from "node:fs/promises";
import { Effect, Option, Schema } from "effect";
import { findBunfsCandidates } from "./bunfs-global-bus.js";
import {
  captureEventEmitterGlobalBusCandidates,
  type EventEmitterGlobalBusCandidate,
} from "./event-emitter-global-bus.js";
import { readGlobalBus, type GlobalBusLike } from "./global-bus.js";

export type CaptureDebug = (message: string) => void;

type CaptureInput = {
  client: unknown;
  timeoutMillis?: number;
  debug?: CaptureDebug;
  scanBinary?: () => Promise<string>;
  importModule?: (specifier: string) => Promise<unknown>;
};

type OpenGlobalEventStream = (
  signal: AbortSignal,
) => Promise<AsyncIterator<unknown> | undefined>;

type PendingRead = Promise<IteratorResult<unknown>>;
type GlobalEventMethod = (options?: {
  signal?: AbortSignal;
}) => unknown | Promise<unknown>;

const probeType = "opencode-live.probe";
const GlobalEventMethodSchema = Schema.declare(
  (value): value is GlobalEventMethod => typeof value === "function",
  { expected: "global.event" },
);
const GlobalEventClientShape = Schema.Struct({
  global: Schema.Struct({
    event: GlobalEventMethodSchema,
  }),
});
const GlobalEventStreamResponseShape = Schema.Struct({
  stream: Schema.Unknown,
});
const ProbeEventShape = Schema.Struct({
  payload: Schema.Struct({
    id: Schema.String,
    type: Schema.Literal(probeType),
  }),
});
const ModuleNamespaceShape = Schema.Record(Schema.String, Schema.Unknown);

export const captureGlobalBus = Effect.fn("captureGlobalBus")(function* (
  input: CaptureInput,
) {
  const openGlobalEventStream = readOpenGlobalEventStream(input.client);

  if (!openGlobalEventStream) {
    input.debug?.("ctx.client.global.event is unavailable");
    return undefined;
  }

  const timeoutMillis = input.timeoutMillis ?? 1_000;
  const controller = new AbortController();
  const emitterCapture = captureEventEmitterGlobalBusCandidates(input.debug);
  const pendingReads: PendingRead[] = [];
  let stream: AsyncIterator<unknown> | undefined;

  return yield* Effect.gen(function* () {
    const opened = yield* Effect.tryPromise(() =>
      openGlobalEventStream(controller.signal),
    ).pipe(Effect.option);

    if (Option.isNone(opened) || !opened.value) {
      input.debug?.("ctx.client.global.event returned an invalid stream");
      return undefined;
    }

    stream = opened.value;
    yield* waitForEventEmitterSubscription({
      candidates: emitterCapture.candidates,
      pendingReads,
      stream,
      timeoutMillis,
    });

    input.debug?.(
      `captured ${emitterCapture.candidates.length} EventEmitter candidates`,
    );

    for (const candidate of emitterCapture.candidates) {
      const verified = yield* busEchoesProbe({
        bus: candidate.bus,
        pendingReads,
        stream,
        timeoutMillis,
      });

      if (!verified) {
        input.debug?.(
          `candidate failed: ${candidate.source}.${candidate.method}`,
        );
        continue;
      }

      input.debug?.(
        `candidate verified: ${candidate.source}.${candidate.method}`,
      );
      return candidate.bus;
    }

    const scanBinary =
      input.scanBinary ?? (() => fs.readFile(process.execPath, "latin1"));
    const binary = yield* Effect.tryPromise(scanBinary).pipe(Effect.option);

    if (Option.isNone(binary)) {
      input.debug?.("Bun virtual chunk scan failed");
      return undefined;
    }

    const candidates = findBunfsCandidates(binary.value);
    input.debug?.(`Bun virtual chunk candidates: ${candidates.length}`);

    const importModule =
      input.importModule ?? ((specifier: string) => import(specifier));
    for (const candidate of candidates) {
      const module = yield* Effect.tryPromise(() =>
        importModule(candidate.specifier),
      ).pipe(Effect.option);

      if (Option.isNone(module)) {
        input.debug?.(
          `Bun virtual chunk import failed for ${candidate.specifier}`,
        );
        continue;
      }

      const bus = readGlobalBusExport(module.value, candidate.exportName);

      if (!bus) {
        continue;
      }

      const verified = yield* busEchoesProbe({
        bus,
        pendingReads,
        stream,
        timeoutMillis,
      });

      if (!verified) {
        input.debug?.(
          `Bun virtual chunk candidate failed: ${candidate.specifier}#${candidate.exportName}`,
        );
        continue;
      }

      input.debug?.(
        `Bun virtual chunk candidate verified: ${candidate.specifier}#${candidate.exportName}`,
      );
      return bus;
    }

    return undefined;
  }).pipe(
    Effect.ensuring(
      Effect.gen(function* () {
        emitterCapture.restore();
        controller.abort();

        const activeStream = stream;

        if (activeStream) {
          yield* Effect.tryPromise(() => closeStream(activeStream)).pipe(
            Effect.ignore,
          );
        }
      }),
    ),
  );
});

const waitForEventEmitterSubscription = Effect.fn(
  "waitForEventEmitterSubscription",
)(function* (input: {
  candidates: EventEmitterGlobalBusCandidate[];
  stream: AsyncIterator<unknown>;
  pendingReads: PendingRead[];
  timeoutMillis: number;
}) {
  const deadline = performance.now() + input.timeoutMillis;

  while (performance.now() < deadline) {
    const read = input.stream.next();
    const result = yield* Effect.promise(() =>
      Promise.race([
        read.then((value) => ({ type: "event" as const, value })),
        Bun.sleep(0).then(() => ({ type: "pending" as const })),
      ]),
    );

    if (result.type === "event") {
      if (result.value.done) {
        return;
      }

      continue;
    }

    input.pendingReads.push(read);
    const waitDeadline = Math.min(deadline, performance.now() + 25);

    while (input.candidates.length === 0 && performance.now() < waitDeadline) {
      yield* Effect.promise(() => Bun.sleep(1));
    }
    return;
  }
});

const busEchoesProbe = Effect.fn("busEchoesProbe")(function* (input: {
  bus: GlobalBusLike;
  stream: AsyncIterator<unknown>;
  pendingReads: PendingRead[];
  timeoutMillis: number;
}) {
  const id = `evt_opencode_live_probe_${crypto.randomUUID()}`;
  input.bus.emit("event", {
    directory: "global",
    payload: {
      id,
      type: probeType,
      properties: {},
    },
  });

  const deadline = performance.now() + input.timeoutMillis;

  while (performance.now() < deadline) {
    const remainingMillis = Math.max(1, deadline - performance.now());
    const event = yield* readNextBefore({
      stream: input.stream,
      pendingReads: input.pendingReads,
      timeoutMillis: remainingMillis,
    });

    if (!event) {
      return false;
    }

    if (isProbeEvent(event, id)) {
      return true;
    }
  }

  return false;
});

const readNextBefore = Effect.fn("readNextBefore")(function* (input: {
  stream: AsyncIterator<unknown>;
  pendingReads: PendingRead[];
  timeoutMillis: number;
}) {
  const read = input.pendingReads.shift() ?? input.stream.next();
  const result = yield* Effect.tryPromise(() =>
    Promise.race([
      read.then((value) => ({ type: "event" as const, value })),
      Bun.sleep(input.timeoutMillis).then(() => ({ type: "timeout" as const })),
    ]),
  ).pipe(Effect.option);

  if (Option.isNone(result)) {
    return undefined;
  }

  if (result.value.type === "timeout") {
    input.pendingReads.unshift(read);
    return undefined;
  }

  return result.value.value.done ? undefined : result.value.value.value;
});

function isProbeEvent(input: unknown, id: string) {
  const event = Schema.decodeUnknownOption(ProbeEventShape)(input);

  if (Option.isNone(event)) {
    return false;
  }

  return event.value.payload.id === id;
}

function readOpenGlobalEventStream(
  input: unknown,
): OpenGlobalEventStream | undefined {
  const client = Schema.decodeUnknownOption(GlobalEventClientShape)(input);

  if (Option.isNone(client)) {
    return undefined;
  }

  return (signal) =>
    Promise.resolve(
      client.value.global.event.call(client.value.global, { signal }),
    ).then(readGlobalEventStream);
}

function readGlobalEventStream(
  input: unknown,
): AsyncIterator<unknown> | undefined {
  const response = Schema.decodeUnknownOption(GlobalEventStreamResponseShape)(
    input,
  );

  if (Option.isNone(response)) {
    return undefined;
  }

  const stream = response.value.stream;

  if (stream === null || stream === undefined) {
    return undefined;
  }

  const target = Object(stream);
  const next = Reflect.get(target, "next");

  if (typeof next !== "function") {
    return undefined;
  }

  return {
    next() {
      return Promise.resolve(next.call(stream));
    },
    return(value?: unknown) {
      const returnStream = Reflect.get(target, "return");

      if (typeof returnStream !== "function") {
        return Promise.resolve({ done: true, value });
      }

      return Promise.resolve(returnStream.call(stream, value));
    },
  };
}

function readGlobalBusExport(module: unknown, exportName: string) {
  const exports = Schema.decodeUnknownOption(ModuleNamespaceShape)(module);

  if (Option.isNone(exports)) {
    return undefined;
  }

  return readGlobalBus(exports.value[exportName]);
}

function closeStream(stream: AsyncIterator<unknown>) {
  const close = stream.return;

  if (typeof close !== "function") {
    return Promise.resolve();
  }

  return Promise.resolve(close.call(stream, undefined)).then(() => undefined);
}
