import type { TuiPlugin } from "@opencode-ai/plugin/tui";
import { Effect, Exit } from "effect";
import { readDaemonRegistry } from "./daemon-registry.js";
import { opencodeDataDir, resolveOpencodeDbContext } from "./opencode-db.js";

const tui: TuiPlugin = async (api) => {
  api.keymap.registerLayer({
    commands: [
      {
        namespace: "palette",
        name: "opencode-live.status",
        title: "opencode-live: Show daemon PID",
        desc: "Show the opencode-live daemon PID",
        category: "Sync",
        slashName: "oc-live",
        async run() {
          const dataDir = opencodeDataDir();
          const status = await Effect.runPromiseExit(
            Effect.gen(function* () {
              const db = yield* resolveOpencodeDbContext({ dataDir });
              const registry = yield* readDaemonRegistry(db.dbHash, dataDir);

              if (!registry) {
                return {
                  variant: "warning" as const,
                  message: `opencode-live has no daemon PID recorded for ${db.dbPath}`,
                };
              }

              return {
                variant: "info" as const,
                message: `opencode-live daemon PID ${registry.pid}`,
              };
            }),
          );

          if (Exit.isSuccess(status)) {
            api.ui.toast(status.value);
            return;
          }

          api.ui.toast({
            variant: "error",
            message:
              "opencode-live cannot read daemon PID because the current opencode database could not be resolved.",
          });
        },
      },
    ],
  });
};

export default { id: "opencode-live", tui };
