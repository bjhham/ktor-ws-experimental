package io.ktor.messaging

import io.ktor.websocket.*
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.cancel
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class ChannelConnectionTest {

    @Test
    fun `pair delivers messages in both directions`() = runTest {
        val (left, right) = ChannelConnection.pair<String, Int>(backgroundScope.coroutineContext)
        left.outgoing.send("one")
        right.outgoing.send(1)
        assertEquals("one", right.incoming.receive())
        assertEquals(1, left.incoming.receive())
        left.close()
    }

    @Test
    fun `close delivers pending messages and ends the other side`() = runTest {
        val (left, right) = ChannelConnection.pair<String, String>(backgroundScope.coroutineContext)
        launch {
            repeat(3) { left.outgoing.send("m$it") }
            left.close()
        }
        assertEquals(listOf("m0", "m1", "m2"), right.incoming.toList())
        right.coroutineContext.job.join()
        assertTrue(right.coroutineContext.job.isCompleted)
        assertTrue(left.incoming.receiveCatching().isClosed)
    }

    @Test
    fun `failures reach the other side as the close cause`() = runTest {
        val (left, right) = ChannelConnection.pair<String, String>(backgroundScope.coroutineContext)
        left.cancel(CancellationException("abort", IllegalStateException("boom")))
        val error = assertFailsWith<IllegalStateException> { right.incoming.receive() }
        assertEquals("boom", error.message)
        assertFailsWith<IllegalStateException> { right.outgoing.send("late") }
    }

    @Test
    fun `layers map messages and own the underlying connection`() = runTest {
        val (client, server) = ChannelConnection.pair<String, String>(backgroundScope.coroutineContext)
        val numbers = client.mapMessages<String, Int>(encode = { it.toString() }, decode = { it.toIntOrNull() })
        numbers.outgoing.send(42)
        assertEquals("42", server.incoming.receive())
        server.outgoing.send("not a number")
        server.outgoing.send("7")
        assertEquals(7, numbers.incoming.receive())

        numbers.close()
        assertTrue(server.incoming.receiveCatching().isClosed)
        assertTrue(client.incoming.receiveCatching().isClosed)
    }

    @Test
    fun `a layer drains remaining messages when the underlying connection ends`() = runTest {
        val (client, server) = ChannelConnection.pair<String, String>(backgroundScope.coroutineContext)
        val upper = client.mapMessages<String, String>(encode = { it }, decode = { it.uppercase() })
        server.outgoing.send("a")
        server.outgoing.send("b")
        server.close()
        assertEquals(listOf("A", "B"), upper.incoming.toList())
    }

    @Test
    fun `a failing layer cancels the underlying connection`() = runTest {
        val (client, server) = ChannelConnection.pair<String, String>(backgroundScope.coroutineContext)
        val strict = client.mapMessages<String, Int>(encode = { it.toString() }, decode = { it.toInt() })
        server.outgoing.send("x")
        assertFailsWith<NumberFormatException> { strict.incoming.receive() }
        client.coroutineContext.job.join()
        assertTrue(client.coroutineContext.job.isCancelled)
        assertTrue(server.incoming.receiveCatching().isClosed)
    }

    @Serializable
    data class Greeting(val text: String)

    @Test
    fun `text format layer serializes text frames`() = runTest {
        val (client, server) = ChannelConnection.pair<Frame, Frame>(backgroundScope.coroutineContext)
        val greetings = textFormat<Greeting>(Json).wrap(client)
        greetings.outgoing.send(Greeting("hi"))
        assertEquals("""{"text":"hi"}""", (server.incoming.receive() as Frame.Text).readText())
        server.outgoing.send(Frame.Binary(true, byteArrayOf(1)))
        server.outgoing.send(Frame.Text("""{"text":"hello"}"""))
        assertEquals(Greeting("hello"), greetings.incoming.receive())
        greetings.close()
    }
}

private suspend fun <E> kotlinx.coroutines.channels.ReceiveChannel<E>.toList(): List<E> {
    val list = mutableListOf<E>()
    for (element in this) list += element
    return list
}
