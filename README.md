# opencode-live

Sync your chat sessions across opencode runtimes. 

## Commands

The CLI remains only to make the project state explicit:

```sh
bunx opencode-live install --dry-run
```

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
