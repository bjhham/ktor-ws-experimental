package io.ktor.client.messaging

import io.ktor.client.HttpClient
import io.ktor.server.testing.*
import io.ktor.server.websocket.*
import io.ktor.utils.io.*
import io.ktor.websocket.*
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import io.ktor.client.plugins.websocket.WebSockets as ClientWebSockets

@OptIn(ExperimentalKtorApi::class)
class WebSocketsTest {

    @Test
    fun `send and receive over websockets`() = testApplication {
        install(WebSockets)
        routing {
            webSocket {
                send(Frame.Text("Hello"))
                assertEquals("Hello", (incoming.receive() as Frame.Text).readText())
            }
        }

        val connection = clientWithWs.connect(WebSocket)
        for (message in connection.incoming) {
            if (message !is Frame.Text) continue
            val number = message.readText().toInt()
            val response = buildString {
                if (number % 3 == 0) append("Fizz")
                if (number % 5 == 0) append("Buzz")
            }.takeIf { it.isNotEmpty() } ?: continue
            connection.outgoing.send(Frame.Text(response))
        }

        assertEquals("Hello", (connection.incoming.receive() as Frame.Text).readText())
        connection.outgoing.send(Frame.Text("Hello"))
        connection.close()
    }

    @Test
    fun `send and receive over websockets with formatting`() = testApplication {
        install(WebSockets)
        routing {
            webSocket {
                send(Frame.Text(Json.encodeToString(TextMessage("Hello"))))
                val reply = Json.decodeFromString<TextMessage>((incoming.receive() as Frame.Text).readText())
                assertEquals("Hello", reply.text)
            }
        }
        val connectorWithFormat = WebSocket.withFormat<TextMessage>(Json)
        val connection = clientWithWs.connect(connectorWithFormat)
        assertEquals("Hello", connection.incoming.receive().text)
        connection.outgoing.send(TextMessage("Hello"))
        connection.close()
    }

    @Test
    fun `remote close ends the connection`() = testApplication {
        install(WebSockets)
        routing {
            webSocket {
                incoming.onReceive
                send(Frame.Text("Bye"))
            }
        }
        val connection = clientWithWs.connect(WebSocket)
        assertEquals("Bye", (connection.incoming.receive() as Frame.Text).readText())
        assertEquals(true, connection.incoming.receiveCatching().isClosed)
    }

    private val ApplicationTestBuilder.clientWithWs get(): HttpClient =
        createClient { install(ClientWebSockets) }

    @Test
    fun `missing plugin is reported`() = testApplication {
        install(WebSockets)
        val error = assertFailsWith<IllegalStateException> { client.connect(WebSocket) }
        assertContains(error.message.orEmpty(), "install(WebSockets)")
    }
}

@Serializable
data class TextMessage(val text: String)
