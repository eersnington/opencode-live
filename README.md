# opencode-live

Keep opencode chat sessions reactively synced across multiple clients sharing the same `opencode.db`.

Open a chat session across multiple opencode TUI, web, or desktop clients at the same time. When one client receives a relayable chat event, peers with a working refresh path update; streaming text included when opencode emits live deltas.

---

## Install

```sh
bunx opencode-live install
# This delegates it to use `opencode plugin opencode-live --global` internally
```

```sh
bunx opencode-live install --dry-run
# Dry run (prints the delegated command without executing)
```

Restart every running opencode instance after install so each process loads the plugin.

### Manual Installation:

```json
{
  "$schema": "https://opencode.ai/config.json",
  "plugin": ["opencode-live"]
}
```

---

## How it works

Every opencode runtime that loads the plugin does three things:

1. Locates the active `opencode.db`
2. Connects to a local daemon keyed to that database
3. Relays native chat events to every other runtime sharing that database

```text
  opencode A (TUI)          opencode B (web/desktop)
  ----------------          ------------------------
       |                         |
       | loads plugin            | loads plugin
       v                         v
  +----------+              +----------+
  |  plugin  |              |  plugin  |
  +----+-----+              +----+-----+
       |                         |
       +------------+------------+
                    |
                    v
          +-------------------+
          |   opencode-live   |
          |    local daemon   |
          +-------------------+
```

The daemon is local only. It is keyed by the resolved database path so two opencode processes using different databases never interfere with each other. `opencode.db` remains opencode's source of truth; `opencode-live` uses the path for daemon and IPC scoping.

```text
resolved opencode.db path
          |
          v
md5(path.resolve(db path))
          |
          +--> Unix/macOS: os.tmpdir()/opencode-live-<hash>.sock
          +--> Windows:    \\.\pipe\opencode-live-<hash>
```

---

## Event flow

When a source runtime receives a native opencode chat event, the plugin forwards it over IPC. The receiving runtime already shares the same database; it does not need an import endpoint or a second copy of any rows.

```text
  source opencode runtime
          |
          |  native event fires
          |  message.updated / message.part.delta / todo.updated / ...
          v
  source live plugin
          |
          |  local IPC socket / named pipe
          v
  opencode-live daemon
          |
          |  fan out to connected peers
          |  (source connection excluded)
          v
  receiver live plugin
          |
          |  republish as native opencode UI event
          v
   receiver TUI / web / desktop reducers update
```

Each relayed event includes an `originProcessID`; receivers also ignore events from their own process if they ever see them.

Receiving clients are refreshed through opencode's own event paths. No custom rendering code. No parallel state.

---

## Refresh strategy

The plugin probes for opencode's internal event bus and uses the first path that works:

```text
  +-----------------------------------------------------------------+
  |  1. global-bus-import                                           |
  |     import opencode's private GlobalBus directly                |
  |     -> works on source builds                                   |
  +-----------------------------------------------------------------+
  |  2. global-bus-capture                                          |
  |     subscribe to client.global.event(), capture an emitter      |
  |     serverUrl /global/event SSE, or Bun virtual module          |
  |     candidate, then verify with a probe                         |
  |     -> used when direct import is unavailable                   |
  +-----------------------------------------------------------------+
  |  3. worker-rpc                                                  |
  |     post opencode's own rpc.event / global.event message        |
  |     -> works inside TUI worker threads                          |
  +-----------------------------------------------------------------+
  |  4. none                                                        |
  |     no viable refresh path found; incoming relay events no-op   |
  +-----------------------------------------------------------------+
```

All three active modes republish relayable opencode events, including `message.part.delta` when opencode emits it. Desktop support uses the same capture mode when the opencode server URL exposes `/global/event`.

---

## Streaming

When opencode emits live `message.part.delta` events, `opencode-live` relays them before final message persistence:

```text
  source plugin                daemon              receiver plugin
       |                          |                      |
       |  message.part.delta      |                      |
       | -----------------------> |                      |
       |                          |  broadcast           |
       |                          | -------------------> |
       |                          |                      |  republish same event
       |                          |                      |  through refresh path
       |                          |                      v
       |                          |               receiver TUI/web/desktop
       |                          |               streams live text
```

The plugin does not synthesize missing parts; it republishes the event it received.

---

## Development

```sh
bun install
bun run typecheck
bun run test
bun run lint
bun run format:check
bun run build
```

This repo uses Effect V4. Follow the practices shown in the [`effect-smol`](https://github.com/effect-TS/effect-smol) repository.

```sh
bun run effect:check
bun run effect:diagnostics
```
