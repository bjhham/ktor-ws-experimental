package io.ktor.client.messaging

import io.ktor.messaging.*
import io.ktor.server.application.*
import io.ktor.server.testing.*
import io.ktor.server.websocket.*
import io.ktor.utils.io.ExperimentalKtorApi
import kotlinx.coroutines.channels.ClosedReceiveChannelException
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import io.ktor.client.plugins.websocket.WebSockets as ClientWebSockets
import io.ktor.server.websocket.WebSockets as ServerWebSockets

@OptIn(ExperimentalKtorApi::class)
class StompTest {

    @Test
    fun `encodes and decodes frames with escaped headers`() {
        val frame = StompFrame(StompCommand.SEND, mapOf("destination" to "/a:b", "weird" to "line\nbreak\\"), "hi")
        val decoded = StompFrameDecoder().feed(frame.encode()).single()
        assertEquals(StompCommand.SEND, decoded.command)
        assertEquals("/a:b", decoded.destination)
        assertEquals("line\nbreak\\", decoded["weird"])
        assertEquals("2", decoded["content-length"])
        assertEquals("hi", decoded.bodyText)
    }

    @Test
    fun `content-length allows NUL bytes in the body`() {
        val body = byteArrayOf(1, 0, 2, 0)
        val decoded = StompFrameDecoder().feed(StompFrame(StompCommand.SEND, emptyMap(), body).encode()).single()
        assertContentEquals(body, decoded.body)
    }

    @Test
    fun `decodes batched, fragmented frames and skips heart-beats`() {
        val bytes = "\n\r\nMESSAGE\r\nid:1\n\nfirst\u0000\nRECEIPT\nreceipt-id:7\n\n\u0000".encodeToByteArray()
        val decoder = StompFrameDecoder()
        val frames = bytes.toList().chunked(5).flatMap { decoder.feed(it.toByteArray()) }
        assertEquals(listOf(StompCommand.MESSAGE, StompCommand.RECEIPT), frames.map { it.command })
        assertEquals("first", frames[0].bodyText)
        assertEquals("1", frames[0]["id"])
        assertEquals("7", frames[1].receiptId)
    }

    @Test
    fun `CONNECTED headers are not unescaped and repeated headers keep the first value`() {
        val frame = StompFrameDecoder().feed("CONNECTED\nserver:a\\cb\nserver:other\n\n\u0000".encodeToByteArray()).single()
        assertEquals("a\\cb", frame["server"])
    }

    @Test
    fun `rejects undefined escapes and unknown commands`() {
        assertFailsWith<StompProtocolException> {
            StompFrameDecoder().feed("MESSAGE\nx:\\t\n\n\u0000".encodeToByteArray())
        }
        assertFailsWith<StompProtocolException> {
            StompFrameDecoder().feed("HELLO\n\n\u0000".encodeToByteArray())
        }
    }

    @Test
    fun `negotiates heart-beats`() {
        val client = StompHeartBeat(outgoing = 1.seconds, incoming = 2.seconds)
        assertEquals(
            StompHeartBeat(outgoing = 5.seconds, incoming = 2.seconds),
            StompHeartBeat.negotiate(client, StompHeartBeat.parse("500,5000")),
        )
        assertEquals(StompHeartBeat.NONE, StompHeartBeat.negotiate(client, StompHeartBeat.parse("0,0")))
    }

    // -- over websockets ----------------------------------------------------------------------------------------------

    /**
     * A minimal broker: echoes SEND frames to subscribers of the destination and honours receipts.
     * With [dropFirstConnectionOnSend], the first connection is closed instead of handling its first SEND.
     */
    private fun ApplicationTestBuilder.stompBroker(
        connected: Map<String, String> = mapOf("version" to "1.2", "heart-beat" to "0,0"),
        silent: Boolean = false,
        dropFirstConnectionOnSend: Boolean = false,
    ) {
        application { install(ServerWebSockets) }
        var connections = 0
        routing {
            webSocket("/stomp", protocol = "v12.stomp") {
                val connection = ++connections
                val decoder = StompFrameDecoder()
                val subscriptions = mutableMapOf<String, String>()
                var messageIds = 0
                for (wsFrame in incoming) {
                    for (frame in decoder.feed(wsFrame.data)) {
                        if (dropFirstConnectionOnSend && connection == 1 && frame.command == StompCommand.SEND)
                            return@webSocket
                        when (frame.command) {
                            StompCommand.CONNECT -> {
                                val response = if (frame["login"] == "intruder")
                                    StompFrame(StompCommand.ERROR, mapOf("message" to "Bad credentials"))
                                else StompFrame(StompCommand.CONNECTED, connected)
                                send(response.toWebSocketFrame())
                            }
                            StompCommand.SUBSCRIBE -> subscriptions[frame.destination!!] = frame["id"]!!
                            StompCommand.UNSUBSCRIBE -> subscriptions.values.remove(frame["id"])
                            StompCommand.SEND -> subscriptions[frame.destination]?.let { id ->
                                val message = StompFrame(StompCommand.MESSAGE, mapOf(
                                    "subscription" to id,
                                    "message-id" to "m${messageIds++}",
                                    "destination" to frame.destination!!,
                                    "ack" to "a$messageIds",
                                ) + listOfNotNull(frame.contentType?.let { "content-type" to it }), frame.body)
                                send(message.toWebSocketFrame())
                            }
                            else -> {}
                        }
                        if (!silent) frame["receipt"]?.let {
                            send(StompFrame(StompCommand.RECEIPT, mapOf("receipt-id" to it)).toWebSocketFrame())
                        }
                        if (frame.command == StompCommand.DISCONNECT) return@webSocket
                    }
                }
            }
        }
    }

    private fun ApplicationTestBuilder.wsClient() =
        createClient { install(ClientWebSockets) }

    @Test
    fun `subscribe, send and receive over a stomp session`() = testApplication {
        stompBroker()
        val session = wsClient().stompSession("/stomp")
        val subscription = session.subscribe("/topic/greetings", ack = StompAckMode.CLIENT, receipt = true)
        session.send("/topic/greetings", "Hello, STOMP", receipt = true)

        val message = subscription.receive()
        assertEquals("Hello, STOMP", message.bodyText)
        assertEquals("/topic/greetings", message.destination)
        assertEquals("text/plain;charset=utf-8", message.contentType)
        subscription.ack(message)

        subscription.unsubscribe(receipt = true)
        assertTrue(subscription.receiveCatching().isClosed)
        session.disconnect()
        assertTrue(!session.isActive)
    }

    @Test
    fun `raw stomp connection exposes the CONNECTED frame and subprotocol`() = testApplication {
        stompBroker(connected = mapOf("version" to "1.2", "server" to "test/1.0", "heart-beat" to "0,0"))
        val connection = wsClient().connect(WebSocket.withStomp(), "/stomp")
        val connected = assertNotNull(connection.stompConnected)
        assertEquals("1.2", connected.version)
        assertEquals("test/1.0", connected.server)
        assertEquals("v12.stomp", connection.attributes[WebSocketAttributes.Subprotocol])
        connection.close()
    }

    @Test
    fun `session restores subscriptions after reconnecting`() = testApplication {
        stompBroker(dropFirstConnectionOnSend = true)
        val connector = WebSocket.withStomp { heartBeat = StompHeartBeat.NONE }
            .withReconnect { delay = { ReconnectDelay.fixed(10.milliseconds) } }
        val session = wsClient().stompSession("/stomp", connector)
        val subscription = session.subscribe("/topic/news", receipt = true)

        // the broker drops the connection instead of handling this frame, so its receipt never arrives
        assertFailsWith<StompConnectionLostException> { session.send("/topic/news", "dropped", receipt = true) }
        session.send("/topic/news", "delivered", receipt = true)
        assertEquals("delivered", subscription.receive().bodyText)
        session.disconnect()
    }

    @Test
    fun `rejected credentials are not retried by default`() {
        val rejected = StompConnectRejectedException(StompFrame(StompCommand.ERROR, mapOf("message" to "Bad credentials")))
        assertTrue(!ReconnectConfig().retryIf(rejected))
        assertTrue(ReconnectConfig().retryIf(StompHeartBeatTimeoutException(1.seconds)))
    }

    @Test
    fun `ERROR during the handshake fails the connection`() = testApplication {
        stompBroker()
        val error = assertFailsWith<StompErrorException> {
            wsClient().connect(WebSocket.withStomp { login = "intruder" }, "/stomp")
        }
        assertEquals("Bad credentials", error.message)
    }

    @Test
    fun `missing heart-beats close the connection`() = testApplication {
        stompBroker(connected = mapOf("version" to "1.2", "heart-beat" to "50,0"))
        val protocol = WebSocket.withStomp {
            heartBeat = StompHeartBeat(incoming = 50.milliseconds)
        }
        val connection = wsClient().connect(protocol, "/stomp")
        assertFailsWith<StompHeartBeatTimeoutException> { connection.incoming.receive() }
    }

    @Test
    fun `closing the session closes subscriptions and rejects new work`() = testApplication {
        stompBroker()
        val session = wsClient().stompSession("/stomp")
        val subscription = session.subscribe("/topic/x", receipt = true)
        session.close()
        assertFailsWith<ClosedReceiveChannelException> { subscription.receive() }
        assertFailsWith<StompSessionClosedException> { session.subscribe("/topic/y") }
        assertFailsWith<Exception> { session.send("/topic/x", "lost", receipt = true) }
    }
}
