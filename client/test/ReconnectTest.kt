package io.ktor.client.messaging

import io.ktor.messaging.*
import io.ktor.utils.io.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.test.currentTime
import kotlinx.coroutines.test.runTest
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

@OptIn(ExperimentalCoroutinesApi::class, ExperimentalKtorApi::class)
class ReconnectTest {

    /** Hands out in-memory connections; the server ends of accepted connections arrive on [servers]. */
    private class FakeServer(private val refuse: (attempt: Int) -> Throwable? = { null }) {
        val servers = Channel<ChannelDuplex<String>>(Channel.UNLIMITED)
        var attempts = 0

        val connector = ClientConnector<String, String> { context, _ ->
            refuse(++attempts)?.let { throw it }
            val (client, server) = ChannelConnection.pair<String, String>(context.coroutineContext)
            servers.trySend(server)
            client
        }
    }

    private suspend fun <Out, In> ReconnectingConnector<Out, In>.open(scope: CoroutineScope) =
        connect(ConnectionContext(scope.coroutineContext), ConnectRequest(""))

    @Test
    fun `exponential delay grows, caps and resets`() = runTest {
        val delay = ExponentialReconnectDelay(initial = 100.milliseconds, max = 1.seconds)
        val waits = List(6) { delay.next() }
        assertEquals(listOf(100, 200, 400, 800, 1000, 1000).map { it.milliseconds }, waits)
        delay.reset()
        assertEquals(100.milliseconds, delay.next())
    }

    @Test
    fun `exponential delay jitter stays within bounds`() {
        val delay = ExponentialReconnectDelay(initial = 1.seconds, max = 1.seconds, jitter = 0.5, random = Random(42))
        repeat(100) {
            val wait = delay.next()
            assertTrue(wait in 500.milliseconds..1500.milliseconds, "unexpected wait $wait")
        }
    }

    @Test
    fun `fixed delay waits the interval`() = runTest {
        ReconnectDelay.fixed(250.milliseconds).delay()
        assertEquals(250, currentTime)
    }

    @Test
    fun `reconnects with back-off after failed attempts`() = runTest {
        val server = FakeServer(refuse = { if (it in 2..3) IllegalStateException("connection refused") else null })
        val connection = server.connector
            .withReconnect { delay = { ReconnectDelay.exponential(initial = 100.milliseconds) } }
            .open(backgroundScope)

        val first = server.servers.receive()
        first.outgoing.send("a")
        first.close()
        assertEquals("a", connection.incoming.receive())

        server.servers.receive().outgoing.send("b")
        assertEquals("b", connection.incoming.receive())
        assertEquals(4, server.attempts)
        assertEquals(100L + 200L + 400L, currentTime)
        connection.close()
    }

    @Test
    fun `delay is reset after a successful reconnect`() = runTest {
        val server = FakeServer()
        val connection = server.connector
            .withReconnect { delay = { ExponentialReconnectDelay(initial = 100.milliseconds) } }
            .open(backgroundScope)
        repeat(3) { i ->
            val current = server.servers.receive()
            current.outgoing.send("m$i")
            assertEquals("m$i", connection.incoming.receive())
            current.close()
        }
        server.servers.receive()
        // every reconnect starts from the initial delay, since the previous connection delivered a message
        assertEquals(300, currentTime)
        connection.close()
    }

    @Test
    fun `gives up after max attempts and closes`() = runTest {
        val server = FakeServer(refuse = { if (it > 1) IllegalStateException("connection refused") else null })
        val connection = server.connector
            .withReconnect { delay = { ReconnectDelay.fixed(1.seconds).limitAttempts(2) } }
            .open(backgroundScope)
        server.servers.receive().close()

        assertFailsWith<ReconnectAttemptsExceededException> { connection.incoming.receive() }
        assertIs<ConnectionState.Closed>(connection.state.value)
        assertFailsWith<ReconnectAttemptsExceededException> { connection.outgoing.send("x") }
        assertEquals(2000, currentTime)
    }

    @Test
    fun `fatal failures are not retried`() = runTest {
        val server = FakeServer(refuse = { if (it > 1) FatalConnectionException("bad credentials") else null })
        val connection = server.connector
            .withReconnect { delay = { ReconnectDelay.fixed(10.milliseconds) } }
            .open(backgroundScope)
        server.servers.receive().close()

        assertFailsWith<FatalConnectionException> { connection.incoming.receive() }
        assertEquals(2, server.attempts)
    }

    @Test
    fun `messages sent while disconnected are delivered after reconnecting`() = runTest {
        val server = FakeServer()
        val connection = server.connector
            .withReconnect { delay = { ReconnectDelay.fixed(10.milliseconds) } }
            .open(backgroundScope)
        server.servers.receive().close()
        connection.state.first { it is ConnectionState.Reconnecting }

        connection.outgoing.send("hello")
        assertEquals("hello", server.servers.receive().incoming.receive())
        assertEquals(10, currentTime)
        connection.close()
    }

    @Test
    fun `messages sent while disconnected can be dropped`() = runTest {
        val server = FakeServer()
        val connection = server.connector
            .withReconnect {
                delay = { ReconnectDelay.fixed(10.milliseconds) }
                whileDisconnected = DisconnectedSendPolicy.DROP
            }
            .open(backgroundScope)
        server.servers.receive().close()
        connection.state.first { it is ConnectionState.Reconnecting }

        connection.outgoing.send("lost")
        val next = server.servers.receive()
        connection.state.first { it is ConnectionState.Connected }
        connection.outgoing.send("kept")
        assertEquals("kept", next.incoming.receive())
        connection.close()
    }

    @Test
    fun `reconnect hooks run before messages flow and state reports reconnects`() = runTest {
        val server = FakeServer()
        val connection = server.connector
            .withReconnect { delay = { ReconnectDelay.fixed(10.milliseconds) } }
            .open(backgroundScope)
        connection.onReconnect { it.outgoing.send("restore") }
        assertEquals(ConnectionState.Connected::class, connection.state.value::class)

        server.servers.receive().close()
        connection.outgoing.send("after")
        val next = server.servers.receive()
        assertEquals("restore", next.incoming.receive())
        assertEquals("after", next.incoming.receive())
        assertEquals(1, (connection.state.value as ConnectionState.Connected).reconnects)
        connection.close()
    }

    @Test
    fun `closing stops reconnecting`() = runTest {
        val server = FakeServer()
        val connection = server.connector
            .withReconnect { delay = { ReconnectDelay.fixed(1.seconds) } }
            .open(backgroundScope)
        server.servers.receive().close()
        connection.state.first { it is ConnectionState.Reconnecting }

        connection.close()
        assertTrue(connection.incoming.receiveCatching().isClosed)
        assertFailsWith<Exception> { connection.outgoing.send("x") }
        assertEquals(1, server.attempts)
        assertEquals(0, currentTime)
    }

    @Test
    fun `closing flushes and closes the current connection`() = runTest {
        val server = FakeServer()
        val connection = server.connector.withReconnect().open(backgroundScope)
        val current = server.servers.receive()
        connection.outgoing.send("bye")
        connection.close()
        assertEquals("bye", current.incoming.receive())
        assertTrue(current.incoming.receiveCatching().isClosed)
        assertIs<ConnectionState.Closed>(connection.state.value)
    }
}
