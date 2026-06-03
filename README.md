# opencode-live

Sync local opencode chat sessions across running opencode runtimes that share one `opencode.db`.

`opencode-live` installs as an opencode server plugin. Each running opencode process connects to one local daemon for the active database, relays native chat events over local IPC, and republishes them into the receiving process through opencode's internal event path when available.

## Install

```sh
bunx opencode-live install
```

The installer delegates to:

```sh
opencode plugin opencode-live --global
```

Dry run:

```sh
bunx opencode-live install --dry-run
```

Restart every running opencode instance after install so each process loads the plugin.

## Development

```sh
bun install
bun run typecheck
bun test
bun run lint
bun run format:check
bun run build
```

This repo uses Effect V4. Follow best practices shown in the `effect-smol` repository:

```text
https://github.com/effect-TS/effect-smol
```

Useful Effect checks:

```sh
bun run effect:check
bun run effect:diagnostics
```
