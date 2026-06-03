import { Effect } from "effect";
import { spawn, type ChildProcess } from "node:child_process";
import { text } from "node:stream/consumers";
import { fileURLToPath } from "node:url";
import { assert, describe, it } from "@effect/vitest";

const cliPath = fileURLToPath(new URL("../src/cli.ts", import.meta.url));

describe("CLI", () => {
  it.live("dry-run install reports delegated opencode command", () =>
    Effect.gen(function* () {
      const result = yield* runCli(["install", "--dry-run"]);

      assert.strictEqual(result.exitCode, 0);
      assert.include(
        result.stdout,
        "Would install opencode-live globally with",
      );
      assert.include(result.stdout, "opencode plugin opencode-live --global");
    }),
  );
});

function runCli(args: string[]) {
  return Effect.gen(function* () {
    const proc = spawn("bun", [cliPath, ...args], {
      stdio: ["ignore", "pipe", "pipe"],
    });
    const [stdout, stderr, exitCode] = yield* Effect.all(
      [
        Effect.promise(() => text(proc.stdout)),
        Effect.promise(() => text(proc.stderr)),
        waitForExit(proc),
      ],
      { concurrency: "unbounded" },
    );

    return { stdout, stderr, exitCode };
  });
}

function waitForExit(child: ChildProcess) {
  return Effect.callback<number, Error>((resume) => {
    const onError = (error: Error) => resume(Effect.fail(error));

    child.once("error", onError);
    child.once("exit", (code) => {
      child.off("error", onError);
      resume(Effect.succeed(code ?? 1));
    });

    return Effect.sync(() => {
      child.off("error", onError);
      child.kill();
    });
  });
}
