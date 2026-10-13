package io.ktor.server.messaging

import io.ktor.client.messaging.WebSocketTransport
import io.ktor.client.messaging.connect
import io.ktor.client.messaging.with
import io.ktor.messaging.*
import io.ktor.server.routing.*
import io.ktor.server.testing.*
import io.ktor.server.websocket.*
import io.ktor.utils.io.*
import io.ktor.websocket.*
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import io.ktor.client.plugins.websocket.WebSockets as ClientWebSockets

@Serializable
data class Shout(val text: String)

@OptIn(ExperimentalKtorApi::class)
class ServerConnectorTest {

    /** The same layer is used on both sides of the connection. */
    private val shouts = textFormat<Shout>(Json)

    @Test
    fun `server and client share connection layers`() = testApplication {
        install(WebSockets)
        routing {
            route("/shout") {
                accept(WebSocketEndpoint.with(shouts)) {
                    for (shout in incoming) outgoing.send(Shout(shout.text.uppercase()))
                }
            }
        }
        val client = createClient { install(ClientWebSockets) }
        val connection = client.connect(WebSocketTransport.with(shouts), "/shout")
        connection.outgoing.send(Shout("hello"))
        assertEquals(Shout("HELLO"), connection.incoming.receive())
        connection.close()
    }

    @Test
    fun `the connection closes when the handler returns`() = testApplication {
        install(WebSockets)
        routing {
            route("/once") {
                accept(WebSocketEndpoint) {
                    outgoing.send(Frame.Text("only"))
                }
            }
        }
        val client = createClient { install(ClientWebSockets) }
        val connection = client.connect(WebSocketTransport, "/once")
        assertEquals("only", (connection.incoming.receive() as Frame.Text).readText())
        assertTrue(connection.incoming.receiveCatching().isClosed)
    }
}
