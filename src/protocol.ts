import { Result, Schema } from "effect";

export const DbHashSchema = Schema.String.check(
  Schema.isPattern(/^[a-f0-9]{32}$/),
).pipe(Schema.brand("DbHash"));
export type DbHash = Schema.Schema.Type<typeof DbHashSchema>;

export const ProcessIDSchema = Schema.String.check(Schema.isNonEmpty()).pipe(
  Schema.brand("ProcessID"),
);
export type ProcessID = Schema.Schema.Type<typeof ProcessIDSchema>;

export const AllowedEventTypeSchema = Schema.Literals([
  "session.updated",
  "session.deleted",
  "message.updated",
  "message.removed",
  "message.part.updated",
  "message.part.removed",
  "message.part.delta",
  "todo.updated",
]);

const LiveEventSchema = Schema.Struct({
  id: Schema.optional(Schema.String),
  type: AllowedEventTypeSchema,
  properties: Schema.Unknown,
});

const HelloMessageSchema = Schema.Struct({
  type: Schema.Literal("hello"),
  processID: ProcessIDSchema,
  dbPath: Schema.String,
  dbHash: DbHashSchema,
  directory: Schema.String,
  projectID: Schema.optional(Schema.String),
  workspaceID: Schema.optional(Schema.String),
});

const RelayEventMessageSchema = Schema.Struct({
  type: Schema.Literal("event"),
  originProcessID: ProcessIDSchema,
  directory: Schema.String,
  projectID: Schema.optional(Schema.String),
  workspaceID: Schema.optional(Schema.String),
  event: LiveEventSchema,
});

const ShutdownMessageSchema = Schema.Struct({
  type: Schema.Literal("shutdown"),
});

const ErrorMessageSchema = Schema.Struct({
  type: Schema.Literal("error"),
  message: Schema.String,
});

const ClientMessageSchema = Schema.Union([
  HelloMessageSchema,
  RelayEventMessageSchema,
  ShutdownMessageSchema,
]);

const ServerMessageSchema = Schema.Union([
  RelayEventMessageSchema,
  ErrorMessageSchema,
]);

const WireMessageSchema = Schema.Union([
  HelloMessageSchema,
  RelayEventMessageSchema,
  ShutdownMessageSchema,
  ErrorMessageSchema,
]);

const ClientMessageLine = Schema.fromJsonString(ClientMessageSchema);
const ServerMessageLine = Schema.fromJsonString(ServerMessageSchema);
const WireMessageLine = Schema.fromJsonString(WireMessageSchema);

export class ProtocolMessageDecodeError extends Schema.TaggedErrorClass<ProtocolMessageDecodeError>()(
  "ProtocolMessageDecodeError",
  {
    direction: Schema.Literals(["client", "server"]),
    line: Schema.String,
    message: Schema.String,
  },
) {}

export type AllowedEventType = Schema.Schema.Type<
  typeof AllowedEventTypeSchema
>;
export type LiveEvent = Schema.Schema.Type<typeof LiveEventSchema>;
export type HelloMessage = Schema.Schema.Type<typeof HelloMessageSchema>;
export type RelayEventMessage = Schema.Schema.Type<
  typeof RelayEventMessageSchema
>;
export type ShutdownMessage = Schema.Schema.Type<typeof ShutdownMessageSchema>;
export type ErrorMessage = Schema.Schema.Type<typeof ErrorMessageSchema>;
export type ClientMessage = Schema.Schema.Type<typeof ClientMessageSchema>;
export type ServerMessage = Schema.Schema.Type<typeof ServerMessageSchema>;
export type WireMessage = Schema.Schema.Type<typeof WireMessageSchema>;

export function encodeMessage(message: WireMessage) {
  return `${Schema.encodeUnknownSync(WireMessageLine)(message)}\n`;
}

export function decodeClientMessageLine(
  line: string,
): Result.Result<ClientMessage | undefined, ProtocolMessageDecodeError> {
  if (!line.trim()) {
    return Result.succeed(undefined);
  }

  const decoded = Schema.decodeUnknownResult(ClientMessageLine)(line, {
    onExcessProperty: "error",
  });

  if (Result.isSuccess(decoded)) {
    return Result.succeed(decoded.success);
  }

  return Result.fail(
    new ProtocolMessageDecodeError({
      direction: "client",
      line,
      message: `Invalid client IPC message: ${decoded.failure}`,
    }),
  );
}

export function decodeServerMessageLine(
  line: string,
): Result.Result<ServerMessage | undefined, ProtocolMessageDecodeError> {
  if (!line.trim()) {
    return Result.succeed(undefined);
  }

  const decoded = Schema.decodeUnknownResult(ServerMessageLine)(line, {
    onExcessProperty: "error",
  });

  if (Result.isSuccess(decoded)) {
    return Result.succeed(decoded.success);
  }

  return Result.fail(
    new ProtocolMessageDecodeError({
      direction: "server",
      line,
      message: `Invalid server IPC message: ${decoded.failure}`,
    }),
  );
}
