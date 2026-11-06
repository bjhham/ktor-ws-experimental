package io.ktor.messaging

import io.ktor.util.*
import io.ktor.websocket.*
import kotlinx.atomicfu.atomic
import kotlinx.coroutines.cancel
import kotlin.coroutines.CoroutineContext

/** Attributes describing a WebSocket connection. */
object WebSocketAttributes {
    /** The subprotocol selected by the server, if any. */
    val Subprotocol: AttributeKey<String> = AttributeKey("WebSocketSubprotocol")
}

/**
 * Exposes this WebSocket session as a [ChannelDuplex] of frames. The returned connection owns the session:
 * closing it sends a close frame, cancelling it cancels the session.
 */
fun DefaultWebSocketSession.toConnection(
    parent: CoroutineContext = coroutineContext,
    attributes: Attributes = Attributes(concurrent = true),
): ChannelDuplex<Frame> {
    val session = this
    val closed = atomic(false)
    return ChannelConnection(
        parent = parent,
        attributes = attributes,
        receive = { incoming -> for (frame in session.incoming) incoming.send(frame) },
        send = { outgoing -> for (frame in outgoing) session.outgoing.send(frame) },
        onClose = { cause ->
            closed.value = true
            val reason = if (cause == null) CloseReason(CloseReason.Codes.NORMAL, "")
                else CloseReason(CloseReason.Codes.INTERNAL_ERROR, cause.message ?: "")
            runCatching { session.close(reason) }
        },
        onRelease = { if (!closed.value) session.cancel() },
    )
}
