@file:OptIn(ExperimentalKtorApi::class)
package io.ktor.server.messaging

import io.ktor.messaging.*
import io.ktor.server.routing.*
import io.ktor.server.websocket.*
import io.ktor.utils.io.*
import io.ktor.websocket.*
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext
import kotlinx.serialization.KSerializer
import kotlinx.serialization.StringFormat
import kotlinx.serialization.serializer

/**
 * Accepts incoming [ChannelConnection]s on a route: a transport (e.g. [WebSocketEndpoint]),
 * optionally wrapped with the same [ConnectionLayer]s as client connectors ([with]).
 */
@ExperimentalKtorApi
fun interface ServerConnector<Out, In> {
    /** Installs the transport on [route]; [handler] is called for every accepted connection. */
    fun install(route: Route, handler: suspend (ChannelConnection<Out, In>) -> Unit)
}

/**
 * Accepts connections with [connector] on this route. The connection is closed when [handler] returns,
 * after delivering the messages it sent; if [handler] fails, the connection is closed with that failure.
 */
@ExperimentalKtorApi
fun <Out, In> Route.listen(
    connector: ServerConnector<Out, In>,
    handler: suspend ChannelConnection<Out, In>.() -> Unit,
): Route {
    connector.install(this) { connection ->
        var failure: Throwable? = null
        try {
            connection.handler()
        } catch (e: Throwable) {
            failure = e
            throw e
        } finally {
            withContext(NonCancellable) { connection.close(failure) }
        }
    }
    return this
}

/** Applies [layer] to every connection accepted by this connector. */
@ExperimentalKtorApi
fun <A, B> ServerConnector<A, A>.with(layer: ConnectionLayer<A, B>): ServerConnector<B, B> {
    val base = this
    return ServerConnector { route, handler ->
        base.install(route) { connection -> handler(layer.wrapOrCancel(connection)) }
    }
}

/** Plain WebSocket endpoint; requires the server `WebSockets` plugin. */
object WebSocketEndpoint : WebSocketAcceptor()

/** WebSocket endpoint; with a [protocol], only clients offering that subprotocol are accepted. */
open class WebSocketAcceptor(val protocol: String? = null) : ServerConnector<Frame, Frame> {
    override fun install(route: Route, handler: suspend (ChannelConnection<Frame, Frame>) -> Unit) {
        route.webSocket(protocol = protocol) {
            val connection = toConnection()
            protocol?.let { connection.attributes.put(WebSocketAttributes.Subprotocol, it) }
            handler(connection)
        }
    }
}

/** Exchanges [M] messages as text frames serialized with [format]. */
inline fun <reified M> ServerConnector<Frame, Frame>.withFormat(
    format: StringFormat,
    serializer: KSerializer<M> = serializer<M>(),
): ServerConnector<M, M> = with(textFormat(format, serializer))
