# Changelog

## 0.1.3 - 2026-06-04

- Add daemon idle shutdown after all peers disconnect, preventing orphaned background daemons.
- Guard IPC message and write-buffer sizes to protect daemon memory usage.
- Add the `/oc-live` TUI command to show the current opencode-live daemon PID.
- Expose the TUI plugin target through package exports so `opencode plugin opencode-live --global` installs both server and TUI entries.
- Remove the package root server entrypoint and document manual TUI installation.

## 0.1.2 - 2026-06-03

- Add support for syncing desktop app sessions with TUI and web clients.
- Support desktop sessions protected by opencode server Basic auth.

## 0.1.1 - 2026-06-03

- Add `bunx opencode-live install` for installing the plugin through opencode.
- Add `bunx opencode-live install --dry-run` to preview the delegated install command.
- Sync opencode chat session events across multiple local clients sharing the same `opencode.db`.
- Works with TUI and web clients in any combination when they share the same session/database.
- Desktop app support is pending validation.
