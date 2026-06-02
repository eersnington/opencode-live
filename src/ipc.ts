import net from "node:net";
import os from "node:os";
import path from "node:path";
import { Effect, Result } from "effect";
import {
  decodeClientMessageLine,
  decodeServerMessageLine,
  encodeMessage,
  type ClientMessage,
  type DbHash,
  type ProtocolMessageDecodeError,
  type ServerMessage,
  type WireMessage,
} from "./protocol.js";

export type IpcPeer = {
  send(message: WireMessage): void;
  close(): void;
};

export function socketPath(hash: DbHash) {
  if (process.platform === "win32") {
    return `\\\\.\\pipe\\opencode-live-${hash}`;
  }

  return path.join(os.tmpdir(), `opencode-live-${hash}.sock`);
}

export const connectIpc = Effect.fn("connectIpc")(function* (input: {
  socketPath: string;
  onMessage: (message: ServerMessage) => void;
  onError?: (error: ProtocolMessageDecodeError) => void;
}) {
  return yield* Effect.callback<IpcPeer, Error>((resume) => {
    const socket = net.createConnection(input.socketPath);
    let buffer = "";
    let settled = false;

    const fail = (error: Error) => {
      if (settled) {
        return;
      }

      settled = true;
      resume(Effect.fail(error));
    };

    socket.once("connect", () => {
      if (settled) {
        return;
      }

      settled = true;
      resume(
        Effect.succeed({
          send(message) {
            socket.write(encodeMessage(message));
          },
          close() {
            socket.end();
          },
        }),
      );
    });
    socket.once("error", fail);
    socket.on("data", (chunk) => {
      buffer += chunk.toString("utf8");
      const lines = buffer.split("\n");
      buffer = lines.pop() ?? "";

      for (const line of lines) {
        const decoded = decodeServerMessageLine(line);

        if (Result.isFailure(decoded)) {
          input.onError?.(decoded.failure);
          continue;
        }

        if (decoded.success) {
          input.onMessage(decoded.success);
        }
      }
    });
    return Effect.sync(() => socket.destroy());
  });
});

export function createIpcServer(input: {
  socketPath: string;
  onClient: (client: IpcPeer) => void;
  onMessage: (client: IpcPeer, message: ClientMessage) => void;
  onClose: (client: IpcPeer) => void;
  onError?: (client: IpcPeer, error: ProtocolMessageDecodeError) => void;
}) {
  return net.createServer((socket) => {
    let buffer = "";
    const client: IpcPeer = {
      send(message) {
        socket.write(encodeMessage(message));
      },
      close() {
        socket.end();
      },
    };
    input.onClient(client);
    socket.on("data", (chunk) => {
      buffer += chunk.toString("utf8");
      const lines = buffer.split("\n");
      buffer = lines.pop() ?? "";

      for (const line of lines) {
        const decoded = decodeClientMessageLine(line);

        if (Result.isFailure(decoded)) {
          input.onError?.(client, decoded.failure);
          continue;
        }

        if (decoded.success) {
          input.onMessage(client, decoded.success);
        }
      }
    });
    socket.on("close", () => input.onClose(client));
  });
}
