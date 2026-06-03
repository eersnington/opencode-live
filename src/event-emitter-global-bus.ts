import { EventEmitter as BareEventEmitter } from "events";
import { EventEmitter as NodeEventEmitter } from "node:events";
import { readGlobalBus, type GlobalBusLike } from "./global-bus.js";

type CaptureDebug = (message: string) => void;

export type EventEmitterGlobalBusCandidate = {
  source: string;
  method: "on" | "addListener";
  bus: GlobalBusLike;
};

type PatchTarget = {
  source: string;
  prototype: EventEmitterPrototype;
};

type EventEmitterPrototype = {
  on: EventEmitterMethod;
  addListener: EventEmitterMethod;
};

type EventEmitterMethod = (
  this: object,
  eventName: string | symbol,
  listener: (...args: unknown[]) => void,
) => unknown;

export function captureEventEmitterGlobalBusCandidates(
  debug: CaptureDebug | undefined,
) {
  const candidates: EventEmitterGlobalBusCandidate[] = [];
  const targets = patchTargets();
  const seen = new WeakSet<object>();
  const restorers: Array<() => void> = [];

  debug?.(
    `EventEmitter prototype sources: ${targets.map((target) => target.source).join(", ")}`,
  );

  for (const target of targets) {
    const originalOn = target.prototype.on;
    const originalAddListener = target.prototype.addListener;

    target.prototype.on = makePatchedEmitterMethod({
      candidates,
      method: "on",
      original: originalOn,
      seen,
      source: target.source,
    });
    target.prototype.addListener = makePatchedEmitterMethod({
      candidates,
      method: "addListener",
      original: originalAddListener,
      seen,
      source: target.source,
    });

    restorers.push(() => {
      target.prototype.on = originalOn;
      target.prototype.addListener = originalAddListener;
    });
  }

  return {
    candidates,
    restore() {
      for (let index = restorers.length - 1; index >= 0; index--) {
        restorers[index]?.();
      }
    },
  };
}

function makePatchedEmitterMethod(input: {
  candidates: EventEmitterGlobalBusCandidate[];
  method: "on" | "addListener";
  original: EventEmitterMethod;
  seen: WeakSet<object>;
  source: string;
}): EventEmitterMethod {
  return function patchedEmitterMethod(
    this: object,
    eventName: string | symbol,
    listener: (...args: unknown[]) => void,
  ) {
    if (eventName === "event" && !input.seen.has(this)) {
      const bus = readGlobalBus(this);

      if (bus) {
        input.seen.add(this);
        input.candidates.push({
          source: input.source,
          method: input.method,
          bus,
        });
      }
    }

    return input.original.call(this, eventName, listener);
  };
}

function patchTargets(): PatchTarget[] {
  const targets: PatchTarget[] = [
    { source: "node:events", prototype: NodeEventEmitter.prototype },
  ];

  if (BareEventEmitter.prototype !== NodeEventEmitter.prototype) {
    targets.push({ source: "events", prototype: BareEventEmitter.prototype });
  } else {
    targets[0] = {
      source: "node:events/events",
      prototype: NodeEventEmitter.prototype,
    };
  }

  return targets;
}
