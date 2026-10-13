## Feature Requests

Abstraction over WebSockets requests:
 - KTOR-448 Socket.io like WebSockets
 - KTOR-790 SockJS support
 - KTOR-612 WebSocket client with STOMP

Both Socket.io and SockJS also include HTTP fallbacks when websockets are not supported on the network.  For these 
features, I advise adding a custom protocol abstraction to cover interop and potentially allowing fallback logic.

Usability improvements:
 - KTOR-3605 Provide Flow for WebSockets API
 - KTOR-4323 Websockets with Kotlinx serialization: Support custom serializer
 - KTOR-5779 Simplify Serialization from Websocket
 - KTOR-5869 Add secure equivalent of HttpClient.webSocketSession
 - KTOR-6267 Allow Websocket client to access handshake headers (ex: Sec-WebSocket-Protocol)
 - KTOR-7549 WebSockets: Provide extension points to get notified on WebSocket connection open and close
 - KTOR-7718 WebSockets: Enhanced WebSocket Header Customization
 - KTOR-9228 WebSocketStream API support

Access to headers and open/close events seem like pretty obvious improvements to pursue.  Flows and serialization can
play into the custom protocol layer.

## Critique

Overall, our WebSocket API is well-designed, with incoming and outgoing channels forming a strong basis for building
upon.  But, we can further simplify the API and allow for more extensibility.

My main gripes with the current design include:

1. Unnecessary interfaces `WebSocketSession <- DefaultWebSocketSession, ClientWebSocketSession, ServerWebSocketSession`:
 - It's confusing how these all fit together
 - They needlessly expose aspects of the implementation
 - They're needlessly stateful
 - They can easily be flattened into the two-way `Channel<Frame>` without all the extra implementation details
2. Action at a distance
 - In order to work with WebSockets, this requires to manipulate the client
 - This includes the `WebSockets { contentConverter }` property, which is later used in `receiveDeserialized` and `sendSerialized`
 - All implementation details of the connection should be isolated to the connection itself
3. `UrlBuilder` problems
 - The API relies quite a bit on the `UrlBuilder`, which has several problems

I think we can address all these issues and extend functionality to include connection persistence and fallback logic 
in one go.

## Message Protocol Abstraction

Two of the three web socket abstraction feature requests are not actually built on top of web sockets exclusively.  For 
example, Socket.io also uses long-polling and WebTransport in its sessions.  To handle these kinds of protocols, we'll 
need an abstraction of our own for general two-way message communication.

## Design Overview

The abstraction comes down to one idea: **a connection is a pair of channels with a lifetime**. Everything else is
a function that creates a connection or turns one connection into another. Transports, formats, protocols like
STOMP, reconnects and fallbacks are all separate pieces that compose with each other.

### Modules

| Module   | Contents                                                                                     |
|----------|----------------------------------------------------------------------------------------------|
| `core`   | `MessageConnection`, `ConnectionLayer`, `MessageCodec`, `textFormat`, in-memory `pair()`, WebSocket session adapter (JVM + JS) |
| `client` | `ClientConnector`, `WebSocketTransport`, `orElse`, `withReconnect`, `ReconnectDelay`, STOMP     |
| `server` | `ServerConnector`, `WebSocketEndpoint`, `Route.accept`                                         |

### Core concepts

**`MessageConnection<Out, In>`** (`core`): the single connection type shared by every transport, layer, client
and server.

```kotlin
interface MessageConnection<in Out, out In> : CoroutineScope {
    val incoming: ReceiveChannel<In>
    val outgoing: SendChannel<Out>
    val attributes: Attributes                  // handshake results, added to by each layer
    suspend fun close(cause: Throwable? = null)  // graceful: pending outgoing messages are delivered first
}
typealias Duplex<M> = MessageConnection<M, M>
```

- `incoming` and `outgoing` are ordinary channels, not wrappers. Select, `for` loops, `consumeAsFlow()` and
  `receiveAsFlow()` all work as usual.
- The connection's `Job` lives exactly as long as the connection. Helper coroutines (heart-beats, message copying)
  run as its children and stop when it ends. `close()` ends it gracefully and `cancel()` aborts it.
- When the connection ends, `incoming` is closed. If it ended with a failure, that failure is the close cause.
- The two type parameters let each direction carry its own message type, e.g. requests out and events in.

**`ConnectionLayer<A, B>`** (`core`): turns a `Duplex<A>` into a `Duplex<B>`. A layer doesn't know how the
connection was made, so the same layer works on the client and on the server. Layers compose with `+`.
`MessageCodec<Wire, M>` covers the common case: one codec instance per connection, so it can keep state, and
`decode` returns any number of messages (none for keep-alives, several for a batched wire message).
`textFormat<M>(Json)` is a codec layer for kotlinx.serialization over text frames.

**`ClientConnector<Out, In>`** (`client`): a factory that dials a connection from a `ConnectRequest` (URL,
headers, attributes). Connectors are built by starting from a transport and wrapping it:

- `WebSocketTransport`: the base transport. Its `WebSocketConnector(subprotocols)` constructor sets the offered
  subprotocols.
- `.with(layer)` / `.withFormat<M>(Json)`: applies a layer to each new connection.
- `.stomp { … }`: STOMP handshake, framing and heart-beats.
- `.orElse(fallback)`: tries another connector if this one fails to connect.
- `.withReconnect { … }`: re-dials whenever the connection ends. This must be the outermost wrapper.

**`ServerConnector<Out, In>`** (`server`): the server-side counterpart. It installs a transport on a `Route` and
hands each accepted connection to a handler. It supports `.with(layer)` and `.withFormat<M>(Json)` too, using the
same layer types as the client.

### How the pieces fit

```
              client                                       server
  ┌─────────────────────────────┐                ┌─────────────────────────────┐
  │ withReconnect { … }         │  client-only   │                             │
  │ orElse(fallback)            │                │                             │
  ├─────────────────────────────┤                ├─────────────────────────────┤
  │ stomp { … }                 │ shared layers  │ with(layer)                 │
  │ with(textFormat<M>(Json))   │ ◄────────────► │ with(textFormat<M>(Json))   │
  ├─────────────────────────────┤                ├─────────────────────────────┤
  │ WebSocketTransport          │   transports   │ WebSocketEndpoint           │
  └─────────────────────────────┘                └─────────────────────────────┘
                 │                                              │
                 └───────────── MessageConnection<Out, In> ─────┘
```

Each wrapper owns the connection beneath it. Closing the outermost connection closes the whole stack, and a failure
at the bottom ends every connection layered above it.

### Usage

Plain WebSockets with typed messages:

```kotlin
val client = HttpClient { install(WebSockets) }
val chat = client.connect(WebSocketTransport.withFormat<ChatMessage>(Json), "wss://example.com/chat") {
    header(HttpHeaders.Authorization, "Bearer $token")
}
chat.outgoing.send(ChatMessage("hello"))
chat.incoming.consumeAsFlow().collect { println(it.text) }
```

The same format on the server:

```kotlin
routing {
    route("/chat") {
        accept(WebSocketEndpoint.withFormat<ChatMessage>(Json)) {
            for (message in incoming) outgoing.send(message)   // closed when the handler returns
        }
    }
}
```

A resilient connection with back-off and state events:

```kotlin
val connector = WebSocketTransport.withFormat<Event>(Json).withReconnect {
    delay = { ReconnectDelay.exponential(jitter = 0.2).limitAttempts(10) }
    retryIf = { it !is FatalConnectionFailure }
    whileDisconnected = DisconnectedSendPolicy.SUSPEND
}
val events = client.connect(connector, "wss://example.com/events")
events.onReconnect { connection -> connection.outgoing.send(Event.Resume(lastSeenId)) }
launch { events.state.collect { state -> updateStatusBar(state) } }
```

STOMP over WebSockets, with subscriptions restored after a reconnect:

```kotlin
val session = client.stompSession("wss://broker/stomp", StompWebSocket.stomp { login = "me"; passcode = "…" }.withReconnect())
val news = session.subscribe("/topic/news", ack = StompAckMode.CLIENT)
session.send("/topic/news", "Hello", receipt = true)
for (message in news) { handle(message.bodyText); news.ack(message) }
```

Testing a layer without a server:

```kotlin
val (client, server) = MessageConnection.pair<Frame, Frame>()
val typed = textFormat<ChatMessage>(Json).wrap(client)
```

## Benefits

- **One connection type everywhere.** Transports, layers, client and server all use `MessageConnection`, so code
  that consumes messages doesn't care whether it's WebSockets, long-polling or an in-memory pair. It also doesn't
  care which layers sit underneath.
- **Composition instead of configuration.** Each feature is a small wrapper with one responsibility. You add new
  behaviour (compression, logging, a Socket.io-style protocol) by writing another layer or connector, without
  touching the core or the `HttpClient` setup.
- **Standard coroutine semantics.** Plain channels mean select, flows, iteration and backpressure work as usual.
  Structured concurrency gives each connection a clear owner, so helper coroutines can't leak, and failures
  propagate the same way at every level.
- **No action at a distance.** Connection behaviour lives in the connector value, not in client plugin settings.
  This includes the serialization format, subprotocols, STOMP credentials and the reconnect policy. Two
  connections from the same client can use completely different setups.
- **Information flows both ways.** `ConnectRequest` carries URL, headers and attributes in; `attributes` carries
  handshake results out (response headers, negotiated subprotocol, STOMP `CONNECTED` frame).
- **Client and server share code.** Formats and protocol layers are written once and used on both sides.
- **Resilience built in, not bolted on.** Reconnects keep the same channels. They are paced by pluggable
  `ReconnectDelay` strategies, report `ConnectionState`, sort failures into retryable and fatal ones, and let
  layers above restore their state through `onReconnect`.
- **Testable.** `MessageConnection.pair()` and `ConnectionContext` let you test layers and reconnect logic with
  virtual time, without starting a server.
- **Small surface.** Most user-facing API is extension functions on a few interfaces. Implementation types such as
  `PipedConnection` stay internal.

## Addressing User Needs

| Request                                         | How this design addresses it                                                                                     |
|-------------------------------------------------|-------------------------------------------------------------------------------------------------------------------|
| KTOR-612 WebSocket client with STOMP            | `StompWebSocket.stomp { … }` connector and `HttpClient.stompSession`, with subscriptions, receipts, acks, transactions and heart-beats |
| KTOR-448 Socket.io / KTOR-790 SockJS            | Not implemented yet. Each would be a protocol layer over one or more transports, with HTTP fallbacks via `orElse` (e.g. `WebSocketTransport.orElse(LongPolling)`) |
| KTOR-3605 Flow API                              | `incoming` is a plain `ReceiveChannel`: `consumeAsFlow()` / `receiveAsFlow()` work directly                          |
| KTOR-4323 custom serializer                     | `withFormat(Json, MyType.serializer())` / `textFormat(format, serializer)`; any `MessageCodec` for other formats      |
| KTOR-5779 simplify serialization                | Typed connections: `outgoing.send(message)` / `incoming.receive()` take and return `M`, with no `sendSerialized` or client-wide converter |
| KTOR-5869 secure `webSocketSession` equivalent  | One `connect(connector, url)` entry point; the URL scheme (`ws`/`wss`) selects TLS                                |
| KTOR-6267 access handshake headers              | `ConnectionAttributes.ResponseHeaders` and `WebSocketAttributes.Subprotocol` on `connection.attributes`            |
| KTOR-7549 open/close notifications              | `ReconnectingConnection.state` (`Connecting`, `Connected`, `Reconnecting`, `Closed`), `onReconnect` hooks, and the connection `Job` for close |
| KTOR-7718 header customization                  | `ConnectRequestBuilder.header(...)` per connection; subprotocols per connector                                   |
| KTOR-9228 WebSocketStream                       | Another transport implementing `ClientConnector<Frame, Frame>`. Every layer above it works unchanged             |

## Open Questions

- Messages already accepted by a connection that then drops are lost; acknowledged delivery or replay would need
  a protocol-level layer.
- STOMP framing is client-only for now; the codec could move to `core` to support a server-side broker.
- Non-HTTP transports (raw TCP, WebRTC) need their own `ConnectionContext` types.
- Should `ConnectionState` events be available for non-reconnecting connections too, e.g. through a common
  `observe()` layer?

