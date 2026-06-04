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
  type ProtocolError,
  ProtocolMessageTooLarge,
  type ServerMessage,
  type WireMessage,
} from "./protocol.js";

const defaultMaxIpcLineBytes = 16 * 1024 * 1024;
const defaultMaxIpcWriteBufferBytes = 16 * 1024 * 1024;

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
  onError?: (error: ProtocolError) => void;
  maxLineBytes?: number;
  maxWriteBufferBytes?: number;
}) {
  return yield* Effect.callback<IpcPeer, Error>((resume) => {
    const socket = net.createConnection(input.socketPath);
    const maxLineBytes = input.maxLineBytes ?? defaultMaxIpcLineBytes;
    const maxWriteBufferBytes =
      input.maxWriteBufferBytes ?? defaultMaxIpcWriteBufferBytes;
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
            writeMessage({ socket, message, maxWriteBufferBytes });
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

      if (Buffer.byteLength(buffer, "utf8") > maxLineBytes) {
        input.onError?.(
          new ProtocolMessageTooLarge({
            direction: "server",
            maxBytes: maxLineBytes,
          }),
        );
        socket.destroy();
        return;
      }

      for (const line of lines) {
        if (Buffer.byteLength(line, "utf8") > maxLineBytes) {
          input.onError?.(
            new ProtocolMessageTooLarge({
              direction: "server",
              maxBytes: maxLineBytes,
            }),
          );
          socket.destroy();
          return;
        }

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
  onError?: (client: IpcPeer, error: ProtocolError) => void;
  maxLineBytes?: number;
  maxWriteBufferBytes?: number;
}) {
  const maxLineBytes = input.maxLineBytes ?? defaultMaxIpcLineBytes;
  const maxWriteBufferBytes =
    input.maxWriteBufferBytes ?? defaultMaxIpcWriteBufferBytes;

  return net.createServer((socket) => {
    let buffer = "";
    const client: IpcPeer = {
      send(message) {
        writeMessage({ socket, message, maxWriteBufferBytes });
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

      if (Buffer.byteLength(buffer, "utf8") > maxLineBytes) {
        input.onError?.(
          client,
          new ProtocolMessageTooLarge({
            direction: "client",
            maxBytes: maxLineBytes,
          }),
        );
        socket.destroy();
        return;
      }

      for (const line of lines) {
        if (Buffer.byteLength(line, "utf8") > maxLineBytes) {
          input.onError?.(
            client,
            new ProtocolMessageTooLarge({
              direction: "client",
              maxBytes: maxLineBytes,
            }),
          );
          socket.destroy();
          return;
        }

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

function writeMessage(input: {
  socket: net.Socket;
  message: WireMessage;
  maxWriteBufferBytes: number;
}) {
  input.socket.write(encodeMessage(input.message));

  if (input.socket.writableLength > input.maxWriteBufferBytes) {
    input.socket.destroy();
  }
}
