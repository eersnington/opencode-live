import { Option, Schema } from "effect";

export type GlobalBusLike = {
  emit(eventName: "event", event: GlobalBusEvent): boolean;
};

export type GlobalBusEvent = {
  directory?: string;
  project?: string;
  workspace?: string;
  payload: {
    id?: string;
    type: string;
    properties: unknown;
  };
};

const GlobalBusModuleShape = Schema.Struct({
  GlobalBus: Schema.Unknown,
});

export function readGlobalBusModule(input: unknown): GlobalBusLike | undefined {
  const module = Schema.decodeUnknownOption(GlobalBusModuleShape)(input);

  if (Option.isNone(module)) {
    return undefined;
  }

  return readGlobalBus(module.value.GlobalBus);
}

export function readGlobalBus(input: unknown): GlobalBusLike | undefined {
  if (input === null || input === undefined) {
    return undefined;
  }

  const target = Object(input);
  const emit = Reflect.get(target, "emit");

  if (typeof emit !== "function") {
    return undefined;
  }

  return {
    emit(eventName, event) {
      return Boolean(emit.call(input, eventName, event));
    },
  };
}
