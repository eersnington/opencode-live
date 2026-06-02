import fs from "node:fs/promises";
import path from "node:path";
import { Effect, Schema } from "effect";
import { opencodeDataDir } from "./opencode-db.js";
import { DbHashSchema, type DbHash } from "./protocol.js";

const DaemonRegistrySchema = Schema.Struct({
  pid: Schema.Number,
  socketPath: Schema.String,
  dbPath: Schema.String,
  dbHash: DbHashSchema,
  startedAt: Schema.Number,
  daemonPath: Schema.optional(Schema.String),
  daemonMtime: Schema.optional(Schema.Number),
});

const DaemonRegistryJson = Schema.fromJsonString(DaemonRegistrySchema);

export type DaemonRegistry = Schema.Schema.Type<typeof DaemonRegistrySchema>;

export class DaemonRegistryWriteFailed extends Schema.TaggedErrorClass<DaemonRegistryWriteFailed>()(
  "DaemonRegistryWriteFailed",
  { cause: Schema.Defect },
) {}

export const readDaemonRegistry = Effect.fn("readDaemonRegistry")(function* (
  hash: DbHash,
  dataDir?: string,
) {
  const file = daemonRegistryFile({ dbHash: hash, dataDir });
  const text = yield* Effect.tryPromise(() => fs.readFile(file, "utf8")).pipe(
    Effect.option,
  );

  if (text._tag === "None") {
    return undefined;
  }

  const decoded = Schema.decodeUnknownOption(DaemonRegistryJson)(text.value);

  if (decoded._tag === "Some") {
    return decoded.value;
  }

  return undefined;
});

export const writeDaemonRegistry = Effect.fn("writeDaemonRegistry")(function* (
  registry: DaemonRegistry,
  dataDir?: string,
) {
  const file = daemonRegistryFile({ dbHash: registry.dbHash, dataDir });
  const text = `${Schema.encodeUnknownSync(DaemonRegistryJson)(registry)}\n`;

  yield* Effect.tryPromise({
    try: async () => {
      await fs.mkdir(path.dirname(file), { recursive: true });
      await fs.writeFile(file, text);
    },
    catch: (cause) => new DaemonRegistryWriteFailed({ cause }),
  });
});

export function daemonRegistryFile(input: {
  dbHash: DbHash;
  dataDir?: string;
}) {
  return path.join(
    input.dataDir ?? opencodeDataDir(),
    "live",
    "daemons",
    `${input.dbHash}.json`,
  );
}
