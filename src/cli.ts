#!/usr/bin/env bun
import { BunRuntime, BunServices } from "@effect/platform-bun";
import { Console, Effect, Schema, Stream } from "effect";
import { Command, Flag } from "effect/unstable/cli";
import { ChildProcess } from "effect/unstable/process";

const version = "0.1.2";
const installArgs = ["plugin", "opencode-live", "--global"] as const;
const manualInstallMessage = `Manual install:
  opencode plugin opencode-live --global

If the opencode CLI is unavailable, add this to ~/.config/opencode/opencode.json:
  { "plugin": ["opencode-live"] }

Then restart opencode.`;

class OpencodeCliNotFound extends Schema.TaggedErrorClass<OpencodeCliNotFound>()(
  "OpencodeCliNotFound",
  {},
) {}

class OpencodeInstallFailed extends Schema.TaggedErrorClass<OpencodeInstallFailed>()(
  "OpencodeInstallFailed",
  {
    exitCode: Schema.Number,
    output: Schema.String,
  },
) {}

const install = Command.make(
  "install",
  {
    dryRun: Flag.boolean("dry-run").pipe(
      Flag.withDefault(false),
      Flag.withDescription(
        "Print the install command without changing opencode config",
      ),
    ),
  },
  (config) =>
    Effect.gen(function* () {
      if (config.dryRun) {
        const opencode = yield* Effect.sync(
          () => Bun.which("opencode") ?? "opencode",
        );
        yield* Console.log(
          `Would install opencode-live globally with: ${formatCommand(opencode)}`,
        );
        return;
      }

      const opencode = yield* findOpencodeCli();
      yield* Console.log(
        `Installing opencode-live globally with: ${formatCommand(opencode)}`,
      );

      const result = yield* runOpencodePlugin(opencode);
      if (result.exitCode !== 0) {
        return yield* new OpencodeInstallFailed(result);
      }

      yield* Console.log(
        "opencode-live is installed globally. Restart opencode to load the plugin.",
      );
    }),
).pipe(
  Command.withDescription(
    "Install opencode-live into the global opencode plugin config",
  ),
);

const app = Command.make("opencode-live").pipe(
  Command.withDescription("Install opencode-live into opencode"),
  Command.withSubcommands([install]),
);

const program = Command.run(app, { version }).pipe(
  Effect.catchTags({
    OpencodeCliNotFound: () =>
      Effect.gen(function* () {
        yield* Console.error(
          "Could not find the opencode CLI on PATH, so opencode-live was not installed.",
        );
        yield* Console.error(manualInstallMessage);
        yield* Effect.sync(() => {
          process.exitCode = 1;
        });
      }),
    OpencodeInstallFailed: (error) =>
      Effect.gen(function* () {
        yield* Console.error(
          `opencode plugin install failed with exit code ${error.exitCode}. Your existing config was left to opencode unchanged.`,
        );
        const output = error.output.trim();
        if (output) {
          yield* Console.error(output);
        }
        yield* Console.error(manualInstallMessage);
        yield* Effect.sync(() => {
          process.exitCode = 1;
        });
      }),
  }),
  Effect.provide(BunServices.layer),
);

const findOpencodeCli = Effect.fn("findOpencodeCli")(function* () {
  const opencode = yield* Effect.sync(() => Bun.which("opencode"));

  if (!opencode) {
    return yield* new OpencodeCliNotFound();
  }

  return opencode;
});

const runOpencodePlugin = Effect.fn("runOpencodePlugin")(function* (
  opencode: string,
) {
  const handle = yield* ChildProcess.make(opencode, installArgs, {
    stderr: "pipe",
    stdout: "pipe",
  });
  const [output, exitCode] = yield* Effect.all(
    [Stream.mkString(Stream.decodeText(handle.all)), handle.exitCode],
    { concurrency: "unbounded" },
  );
  return { output, exitCode: Number(exitCode) };
}, Effect.scoped);

function formatCommand(opencode: string) {
  return [opencode, ...installArgs]
    .map((value) => (value.includes(" ") ? JSON.stringify(value) : value))
    .join(" ");
}

BunRuntime.runMain(program);
