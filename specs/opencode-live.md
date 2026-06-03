# opencode-live PRD

## Goal

`opencode-live` keeps the same opencode chat session live across multiple local opencode clients that share one `opencode.db`.

Targets:

- TUI to TUI.
- TUI to web.
- TUI to desktop, if desktop uses the same opencode event stream/runtime behavior.
- Persisted messages update without reopening or manual refresh.
- Streaming assistant text appears in other clients before final persistence when the private event-bus path works.

## Constraints

- Publish through npm.
- Users keep running normal opencode commands after install.
- No upstream opencode commits.
- No forked opencode distribution.
- No cross-machine sync.
- No network daemon API.
- `opencode.db` stays authoritative.
- The package may use opencode private internals if that is what makes realtime sync work.

## Package Shape

- `opencode-live` CLI with `install` only.
- Server plugin entrypoint loaded by opencode.
- Detached local daemon, one per `opencode.db` hash.
- Local IPC between plugin instances and daemon.
- SQLite `live_sync_*` tables/triggers inside `opencode.db`.

Install command:

```sh
bunx opencode-live install
```

Delegates to:

```sh
opencode plugin opencode-live --global
```

Manual fallback:

```json
{
  "$schema": "https://opencode.ai/config.json",
  "plugin": ["opencode-live"]
}
```

Users must restart every running opencode instance after install.

## Runtime Flow

```text
opencode A
  -> loads opencode-live plugin
  -> writes normal chat rows to opencode.db
  -> forwards streaming message.part.delta to daemon

daemon
  -> watches live_sync_change from SQLite triggers
  -> coalesces durable changes by sessionID
  -> broadcasts session.changed and part.delta over local IPC

opencode B
  -> receives IPC
  -> reads current rows from shared opencode.db
  -> emits native opencode UI events locally
  -> TUI/web/desktop update through existing reducers
```

## UI Update Strategy

Primary path: get opencode's private global event bus from the plugin process.

First try source-build import:

```ts
const { GlobalBus } = await import("opencode/bus/global");
```

If that succeeds, emit the same event shape opencode already sends to TUI/web:

```ts
GlobalBus.emit("event", {
  directory,
  project,
  workspace,
  payload: {
    type: "message.part.updated",
    properties: { sessionID, part, time },
  },
});
```

Native event shapes to emit:

```ts
{ type: "session.updated", properties: { sessionID, info } }
{ type: "session.deleted", properties: { sessionID, info } }
{ type: "message.updated", properties: { sessionID, info } }
{ type: "message.removed", properties: { sessionID, messageID } }
{ type: "message.part.updated", properties: { sessionID, part, time } }
{ type: "message.part.removed", properties: { sessionID, messageID, partID } }
{ type: "message.part.delta", properties: { sessionID, messageID, partID, field, delta } }
{ type: "todo.updated", properties: { sessionID, todos } }
```

If source-build import fails, use the binary-safe capture path:

```ts
// Temporarily patch EventEmitter.prototype.on.
// Open client.global.event({ signal }).
// Capture the emitter used by GlobalBus.on("event", handler).
// Validate it by emitting an opencode-live probe and receiving it on the SSE stream.
```

If both global bus paths fail inside the TUI worker, post opencode's own worker RPC envelope:

```ts
globalThis.postMessage(
  JSON.stringify({
    type: "rpc.event",
    event: "global.event",
    data: { directory, project, workspace, payload },
  }),
);
```

Do not automatically fall back to `client.instance.dispose`. It triggers `server.instance.disposed`, reloads/reinitializes the instance, does not replay native chat events, and does not solve realtime chat sync.

## Database Contract

Create only plugin-owned tables/triggers:

- `live_sync_change`: durable trigger-written change log.
- `live_sync_cursor`: daemon cursor.
- `live_sync_row_state`: row fingerprints for duplicate suppression.

Watch these opencode tables when present:

- `session`
- `message`
- `part`
- `todo`
- `session_message`

Do not import rows through fake APIs. The DB is already shared. Receiving clients read current rows and emit UI events.

## IPC Contract

IPC is local-only and scoped by DB hash.

Messages:

- `hello`: plugin process identity and DB context.
- `event`: native opencode event relay.
- `shutdown`: daemon shutdown request.
- `error`: daemon error.

Every relayed message includes an origin process ID so clients ignore their own echoes.

## Streaming Contract

- Source plugin observes native `message.part.delta` events.
- Source plugin sends `part.delta` to daemon.
- Daemon broadcasts to other clients.
- Receiver emits `message.part.delta` through `GlobalBus` if private bus mode is active.
- If receiver has not seen the target part yet, emit `message.part.updated` first to seed the UI store.

TUI and web only render a delta if the target part already exists locally.

## Capability Modes

- `global-bus-import`: private `opencode/bus/global` import works. Persisted sync and streaming sync are enabled.
- `global-bus-capture`: binary-safe EventEmitter/SSE capture works. Persisted sync and streaming sync are enabled.
- `worker-rpc`: TUI worker RPC bridge is available. Persisted sync and streaming sync are enabled for runtimes listening to worker `global.event` messages.
- `none`: no viable refresh path. Realtime sync is unavailable and must be reported clearly.

Runtime probing is mandatory because source opencode can expose internals while installed binary packages may not.

## Avoid

- Nonexistent session-import endpoints.
- `/sync/replay` as a UI publish path.
- `/tui/publish` for chat state.
- Direct receive-side writes into opencode chat rows.
- TUI-only tricks that do not update web/desktop.

## Acceptance Criteria

- Two TUI clients on the same session show persisted message changes without reopening.
- TUI and web on the same session show persisted message changes without manual refresh.
- Streaming text appears in another client before final persistence in either global bus mode.
- Attachments keep opencode's native part format and render unchanged.
- `bunx opencode-live install --dry-run` prints the delegated install command.

## Validation

```sh
bun install
bun run typecheck
bun test
bun run lint
bun run build
bun ./dist/src/cli.js install --dry-run
```

Behavior validation should use real opencode processes sharing a temp `opencode.db`.
