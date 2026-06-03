#!/usr/bin/env bun
import { spawn, type ChildProcessByStdio } from "node:child_process";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import type { Readable } from "node:stream";

type BenchmarkOptions = {
  runs: number;
  warmup: number;
  timeoutMillis: number;
  opencode: string;
  livePlugin: string;
  debugLive: boolean;
};

type Mode = "baseline" | "treatment";

type RunSample = {
  mode: Mode;
  elapsedMillis: number;
};

type PairSample = {
  index: number;
  order: [Mode, Mode];
  baseline: number;
  treatment: number;
};

type RunFiles = {
  home: string;
  marker: string;
  sentinelPlugin: string;
  tuiConfig: string;
};

const options = parseOptions(process.argv.slice(2));
const pairs: PairSample[] = [];

console.log(
  `bench:tui runs=${options.runs} warmup=${options.warmup} timeout=${options.timeoutMillis}ms metric=app_ready`,
);

for (let index = 0; index < options.warmup; index++) {
  await runPair({ index, measured: false, options });
}

for (let index = 0; index < options.runs; index++) {
  const pair = await runPair({ index, measured: true, options });
  pairs.push(pair);
  printPair(pair);
}

printSummary(pairs);

async function runPair(input: {
  index: number;
  measured: boolean;
  options: BenchmarkOptions;
}): Promise<PairSample> {
  const order = runOrder(input.index);
  const samples: RunSample[] = [];

  for (const mode of order) {
    samples.push(
      await runTuiStartup({
        mode,
        options: input.options,
      }),
    );
  }

  const baseline = readSample(samples, "baseline");
  const treatment = readSample(samples, "treatment");

  if (!input.measured) {
    console.log(`bench:tui warmup=${input.index + 1} order=${order.join(",")}`);
  }

  return { index: input.index + 1, order, baseline, treatment };
}

async function runTuiStartup(input: {
  mode: Mode;
  options: BenchmarkOptions;
}): Promise<RunSample> {
  const files = await prepareRunFiles(input);
  const started = performance.now();

  try {
    await waitForSemanticReady({ files, options: input.options });
    return {
      mode: input.mode,
      elapsedMillis: performance.now() - started,
    };
  } finally {
    await fs.rm(files.home, { force: true, recursive: true });
  }
}

function waitForSemanticReady(input: {
  files: RunFiles;
  options: BenchmarkOptions;
}) {
  return new Promise<void>((resolve, reject) => {
    let settled = false;
    let output = "";
    const started = performance.now();
    const child = spawnScript({
      command: input.options.opencode,
      args: ["--print-logs", "--log-level", "DEBUG"],
      env: benchmarkEnv(input),
    });

    const timeout = setTimeout(() => {
      finish(
        new Error(
          `Timed out after ${input.options.timeoutMillis}ms waiting for TUI app readiness marker ${input.files.marker}\n` +
            `output tail:\n${tail(output, 4000)}`,
        ),
      );
    }, input.options.timeoutMillis);

    const finish = (result: Error | undefined) => {
      if (settled) {
        return;
      }

      settled = true;
      clearTimeout(timeout);
      stopChild(child.pid);

      if (result) {
        reject(result);
        return;
      }

      resolve();
    };

    const onData = (chunk: Buffer) => {
      output += chunk.toString("utf8");

      if (output.includes(input.files.marker)) {
        finish(undefined);
      }
    };

    child.stdout.on("data", onData);
    child.stderr.on("data", onData);
    child.once("error", finish);
    child.once("exit", (code, signal) => {
      if (settled) {
        return;
      }

      finish(
        new Error(
          `opencode exited before TUI app readiness after ${Math.round(performance.now() - started)}ms: code=${code ?? "none"} signal=${signal ?? "none"}\n` +
            `output tail:\n${tail(output, 4000)}`,
        ),
      );
    });
  });
}

async function prepareRunFiles(input: {
  mode: Mode;
  options: BenchmarkOptions;
}): Promise<RunFiles> {
  const home = await fs.mkdtemp(path.join(os.tmpdir(), "opencode-live-bench-"));
  const config = path.join(home, ".config", "opencode");
  const marker = `[opencode-live-bench] app_ready mode=${input.mode} id=${crypto.randomUUID()}`;
  const sentinelPlugin = path.join(home, "semantic-ready-plugin.js");
  const tuiConfig = path.join(home, "tui.json");

  await fs.mkdir(config, { recursive: true });
  await fs.writeFile(sentinelPlugin, sentinelPluginText(marker));
  await fs.writeFile(
    tuiConfig,
    JSON.stringify({ plugin: [sentinelPlugin] }, undefined, 2),
  );

  return { home, marker, sentinelPlugin, tuiConfig };
}

function sentinelPluginText(marker: string) {
  return `const marker = ${JSON.stringify(marker)};

export default {
  id: "opencode-live-benchmark-ready",
  async tui(api) {
    let emitted = false;
    api.slots.register({
      slots: {
        app() {
          if (!emitted) {
            emitted = true;
            process.stderr.write(marker + "\\n");
          }
          return null;
        },
      },
    });
  },
};
`;
}

function benchmarkEnv(input: { files: RunFiles; options: BenchmarkOptions }) {
  const home = input.files.home;
  const baseConfig = {
    formatter: false,
    lsp: false,
    provider: {
      test: {
        name: "Test",
        id: "test",
        env: [],
        npm: "@ai-sdk/openai-compatible",
        models: {
          "test-model": {
            id: "test-model",
            name: "Test Model",
            attachment: false,
            reasoning: false,
            temperature: false,
            tool_call: true,
            release_date: "2025-01-01",
            limit: { context: 100_000, output: 10_000 },
            cost: { input: 0, output: 0 },
            options: {},
          },
        },
        options: { apiKey: "test-key", baseURL: "http://127.0.0.1:9" },
      },
    },
  };
  const treatmentConfig = {
    ...baseConfig,
    plugin: [
      input.options.debugLive
        ? [input.options.livePlugin, { debug: true }]
        : input.options.livePlugin,
    ],
  };

  return {
    ...process.env,
    HOME: home,
    XDG_CONFIG_HOME: path.join(home, ".config"),
    XDG_DATA_HOME: path.join(home, ".local", "share"),
    XDG_STATE_HOME: path.join(home, ".local", "state"),
    XDG_CACHE_HOME: path.join(home, ".cache"),
    OPENCODE_TEST_HOME: home,
    OPENCODE_TUI_CONFIG: input.files.tuiConfig,
    OPENCODE_CONFIG_CONTENT: input.files.marker.includes("mode=treatment")
      ? JSON.stringify(treatmentConfig)
      : JSON.stringify(baseConfig),
    OPENCODE_AUTH_CONTENT: "{}",
    OPENCODE_DISABLE_PROJECT_CONFIG: "1",
    OPENCODE_DISABLE_AUTOUPDATE: "1",
    OPENCODE_DISABLE_AUTOCOMPACT: "1",
    OPENCODE_DISABLE_MODELS_FETCH: "1",
    OPENCODE_DISABLE_DEFAULT_PLUGINS: "1",
    TERM: process.env.TERM ?? "xterm-256color",
  };
}

function spawnScript(input: {
  command: string;
  args: string[];
  env: NodeJS.ProcessEnv;
}): ChildProcessByStdio<null, Readable, Readable> {
  if (process.platform === "darwin") {
    return spawn("script", ["-q", "/dev/null", input.command, ...input.args], {
      detached: true,
      env: input.env,
      stdio: ["ignore", "pipe", "pipe"],
    });
  }

  return spawn(
    "script",
    ["-q", "-c", shellCommand([input.command, ...input.args]), "/dev/null"],
    {
      detached: true,
      env: input.env,
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
}

function stopChild(pid: number | undefined) {
  if (pid === undefined) {
    return;
  }

  try {
    process.kill(-pid, "SIGINT");
  } catch {
    try {
      process.kill(pid, "SIGINT");
    } catch {
      return;
    }
  }

  setTimeout(() => {
    try {
      process.kill(-pid, "SIGKILL");
    } catch {
      try {
        process.kill(pid, "SIGKILL");
      } catch {
        return;
      }
    }
  }, 1_000).unref?.();
}

function printPair(pair: PairSample) {
  const delta = pair.treatment - pair.baseline;
  console.log(
    `bench:tui run=${pair.index} order=${pair.order.join(",")} baseline=${formatMillis(pair.baseline)} treatment=${formatMillis(pair.treatment)} delta=${formatSignedMillis(delta)}`,
  );
}

function printSummary(results: PairSample[]) {
  const baseline = results.map((pair) => pair.baseline);
  const treatment = results.map((pair) => pair.treatment);
  const delta = results.map((pair) => pair.treatment - pair.baseline);
  const baselineStats = stats(baseline);
  const treatmentStats = stats(treatment);
  const deltaStats = stats(delta);

  printStats("baseline", baselineStats);
  printStats("treatment", treatmentStats);
  printStats("delta", deltaStats, { signed: true });
  console.log(
    `METRIC tui_app_ready_baseline_ms=${Math.round(baselineStats.median)}`,
  );
  console.log(
    `METRIC tui_app_ready_treatment_ms=${Math.round(treatmentStats.median)}`,
  );
  console.log(`METRIC tui_app_ready_delta_ms=${Math.round(deltaStats.median)}`);
}

function printStats(
  label: string,
  value: ReturnType<typeof stats>,
  input: { signed?: boolean } = {},
) {
  const format = input.signed ? formatSignedMillis : formatMillis;
  console.log(
    `bench:tui summary ${label} median=${format(value.median)} mean=${format(value.mean)} best=${format(value.best)} worst=${format(value.worst)}`,
  );
}

function stats(values: number[]) {
  const sorted = [...values].sort(
    (left: number, right: number) => left - right,
  );
  const sum = values.reduce((total, value) => total + value, 0);

  return {
    median: median(sorted),
    mean: sum / values.length,
    best: sorted[0] ?? Number.NaN,
    worst: sorted.at(-1) ?? Number.NaN,
  };
}

function median(sortedValues: number[]) {
  if (sortedValues.length === 0) {
    return Number.NaN;
  }

  const middle = Math.floor(sortedValues.length / 2);

  if (sortedValues.length % 2 === 1) {
    return sortedValues[middle] ?? Number.NaN;
  }

  const left = sortedValues[middle - 1] ?? Number.NaN;
  const right = sortedValues[middle] ?? Number.NaN;
  return (left + right) / 2;
}

function readSample(samples: RunSample[], mode: Mode) {
  const sample = samples.find((item) => item.mode === mode);

  if (!sample) {
    throw new Error(`Missing ${mode} sample`);
  }

  return sample.elapsedMillis;
}

function runOrder(index: number): [Mode, Mode] {
  return index % 2 === 0
    ? ["baseline", "treatment"]
    : ["treatment", "baseline"];
}

function formatMillis(value: number) {
  return `${Math.round(value)}ms`;
}

function formatSignedMillis(value: number) {
  const rounded = Math.round(value);
  return `${rounded >= 0 ? "+" : ""}${rounded}ms`;
}

function tail(input: string, length: number) {
  return input.slice(Math.max(0, input.length - length));
}

function parseOptions(args: string[]): BenchmarkOptions {
  let runs = 10;
  let warmup = 2;
  let timeoutMillis = 15_000;
  let opencode = "opencode";
  let livePlugin = path.resolve(import.meta.dirname, "..");
  let debugLive = true;

  for (let index = 0; index < args.length; index++) {
    const arg = args[index];

    if (arg === "--runs") {
      runs = readPositiveInteger(args[index + 1], "--runs");
      index++;
      continue;
    }

    if (arg === "--warmup") {
      warmup = readNonNegativeInteger(args[index + 1], "--warmup");
      index++;
      continue;
    }

    if (arg === "--timeout-ms") {
      timeoutMillis = readPositiveInteger(args[index + 1], "--timeout-ms");
      index++;
      continue;
    }

    if (arg === "--opencode") {
      opencode = readString(args[index + 1], "--opencode");
      index++;
      continue;
    }

    if (arg === "--live-plugin") {
      livePlugin = path.resolve(readString(args[index + 1], "--live-plugin"));
      index++;
      continue;
    }

    if (arg === "--no-live-debug") {
      debugLive = false;
      continue;
    }

    throw new Error(`Unknown option: ${arg ?? ""}`);
  }

  return { runs, warmup, timeoutMillis, opencode, livePlugin, debugLive };
}

function readPositiveInteger(input: string | undefined, option: string) {
  const value = readInteger(input, option);

  if (value <= 0) {
    throw new Error(`${option} must be greater than zero`);
  }

  return value;
}

function readNonNegativeInteger(input: string | undefined, option: string) {
  const value = readInteger(input, option);

  if (value < 0) {
    throw new Error(`${option} must be zero or greater`);
  }

  return value;
}

function readInteger(input: string | undefined, option: string) {
  const value = Number(readString(input, option));

  if (!Number.isInteger(value)) {
    throw new Error(`${option} must be an integer`);
  }

  return value;
}

function readString(input: string | undefined, option: string) {
  if (!input) {
    throw new Error(`Missing value for ${option}`);
  }

  return input;
}

function shellCommand(parts: string[]) {
  return parts.map(shellQuote).join(" ");
}

function shellQuote(input: string) {
  return `'${input.replaceAll("'", `'\\''`)}'`;
}
