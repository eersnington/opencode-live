import os from "node:os";
import path from "node:path";
import { Effect, Schema } from "effect";

export class InMemoryOpencodeDb extends Schema.TaggedErrorClass<InMemoryOpencodeDb>()(
  "InMemoryOpencodeDb",
  {
    message: Schema.String,
  },
) {}

export function opencodeDataDir(env: NodeJS.ProcessEnv = process.env) {
  return path.join(
    env.XDG_DATA_HOME ?? path.join(os.homedir(), ".local", "share"),
    "opencode",
  );
}

export const resolveOpencodeDbPath = Effect.fn("resolveOpencodeDbPath")(
  function* (
    options: {
      dbPath?: string;
      dataDir?: string;
      channel?: string;
      env?: NodeJS.ProcessEnv;
    } = {},
  ): Effect.fn.Return<string, InMemoryOpencodeDb> {
    if (options.dbPath) {
      return path.resolve(options.dbPath);
    }

    const env = options.env ?? process.env;
    const dataDir = options.dataDir ?? opencodeDataDir(env);
    const configured = env.OPENCODE_DB;

    if (configured === ":memory:") {
      return yield* new InMemoryOpencodeDb({
        message: "opencode-live cannot sync an in-memory opencode database",
      });
    }

    if (configured) {
      if (path.isAbsolute(configured)) {
        return configured;
      }

      return path.join(dataDir, configured);
    }

    const channel = options.channel ?? env.OPENCODE_CHANNEL;

    if (channel && channel !== "release") {
      return path.join(
        dataDir,
        `opencode-${channel.replace(/[^A-Za-z0-9._-]/g, "-")}.db`,
      );
    }

    return path.join(dataDir, "opencode.db");
  },
);
