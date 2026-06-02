import { describe, expect, test } from "bun:test";

const cliPath = new URL("../src/cli.ts", import.meta.url).pathname;

describe("CLI", () => {
  test("dry-run install reports delegated opencode command", async () => {
    const result = await runCli(["install", "--dry-run"]);

    expect(result.exitCode).toBe(0);
    expect(result.stdout).toContain("Would install opencode-live globally with");
    expect(result.stdout).toContain("opencode plugin opencode-live --global");
  });
});

async function runCli(args: string[]) {
  const proc = Bun.spawn([process.execPath, cliPath, ...args], {
    stdout: "pipe",
    stderr: "pipe",
  });
  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
    proc.exited,
  ]);
  return { stdout, stderr, exitCode };
}
