package io.ktor.messaging

import io.ktor.websocket.*
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.cancel
import kotlinx.serialization.KSerializer
import kotlinx.serialization.StringFormat
import kotlinx.serialization.serializer

/**
 * Turns an existing connection into another one, e.g. by changing the message format or running a protocol
 * on top of it. Layers don't know how the connection was established, so the same layer works with client
 * connectors and server endpoints alike.
 *
 * The returned connection owns [connection]: closing it closes [connection] as well.
 */
fun interface ConnectionLayer<A, B> {
    suspend fun wrap(connection: ChannelDuplex<A>): ChannelDuplex<B>
}

/** Applies [next] after this layer. */
operator fun <A, B, C> ConnectionLayer<A, B>.plus(next: ConnectionLayer<B, C>): ConnectionLayer<A, C> {
    val first = this
    return ConnectionLayer { connection -> next.wrap(first.wrap(connection)) }
}

/** Wraps [connection] with this layer, cancelling [connection] if wrapping fails. */
suspend fun <A, B> ConnectionLayer<A, B>.wrapOrCancel(connection: ChannelDuplex<A>): ChannelDuplex<B> =
    try {
        wrap(connection)
    } catch (e: Throwable) {
        connection.cancel(CancellationException("Could not apply connection layer", e))
        throw e
    }

/**
 * Converts between the [Wire] messages of a connection and application messages [M].
 *
 * A codec instance belongs to a single connection, so it may keep state, e.g. for messages that span
 * several wire messages. [decode] may return any number of messages, including none (e.g. for keep-alives).
 */
interface MessageCodec<Wire, M> {
    fun encode(message: M): Wire
    fun decode(wire: Wire): List<M>
}

/** A connection layer applying a fresh codec from [factory] to every connection. */
fun <Wire, M> codecLayer(factory: () -> MessageCodec<Wire, M>): ConnectionLayer<Wire, M> =
    ConnectionLayer { connection -> connection.withCodec(factory()) }

/** Encodes and decodes the messages of this connection with [codec]. */
fun <Wire, M> ChannelDuplex<Wire>.withCodec(codec: MessageCodec<Wire, M>): ChannelDuplex<M> = layer(
    receive = { from, to -> for (wire in from) for (message in codec.decode(wire)) to.send(message) },
    send = { from, to -> for (message in from) to.send(codec.encode(message)) },
)

/** Maps the messages of this connection; incoming messages mapped to `null` are skipped. */
fun <Wire, M> ChannelDuplex<Wire>.mapMessages(encode: (M) -> Wire, decode: (Wire) -> M?): ChannelDuplex<M> =
    withCodec(object : MessageCodec<Wire, M> {
        override fun encode(message: M): Wire = encode(message)
        override fun decode(wire: Wire): List<M> = listOfNotNull(decode(wire))
    })

/** Exchanges [M] messages as text frames serialized with [format]; non-text frames are ignored. */
inline fun <reified M> textFormat(format: StringFormat): ConnectionLayer<Frame, M> =
    textFormat(format, serializer<M>())

/** Exchanges [M] messages as text frames serialized with [format]; non-text frames are ignored. */
fun <M> textFormat(format: StringFormat, serializer: KSerializer<M>): ConnectionLayer<Frame, M> =
    codecLayer { TextFrameCodec(format, serializer) }

private class TextFrameCodec<M>(
    private val format: StringFormat,
    private val serializer: KSerializer<M>,
) : MessageCodec<Frame, M> {
    override fun encode(message: M): Frame =
        Frame.Text(format.encodeToString(serializer, message))

    override fun decode(wire: Frame): List<M> = when (wire) {
        is Frame.Text -> listOf(format.decodeFromString(serializer, wire.readText()))
        else -> emptyList()
    }
}
