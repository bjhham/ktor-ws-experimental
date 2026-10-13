@file:OptIn(ExperimentalKtorApi::class)
package io.ktor.client.messaging

import io.ktor.client.plugins.*
import io.ktor.client.plugins.websocket.*
import io.ktor.client.request.*
import io.ktor.http.*
import io.ktor.messaging.*
import io.ktor.util.*
import io.ktor.utils.io.*
import io.ktor.websocket.*
import kotlinx.serialization.KSerializer
import kotlinx.serialization.StringFormat
import kotlinx.serialization.serializer

/** Plain WebSocket transport with default settings. */
object WebSocketTransport : WebSocketConnector()

/**
 * WebSocket transport. Non-empty [subprotocols] are offered to the server through the
 * `Sec-WebSocket-Protocol` header; the selected one is available as [WebSocketAttributes.Subprotocol].
 *
 * The [HttpClient][io.ktor.client.HttpClient] must have the [WebSockets] plugin installed,
 * which also holds the WebSocket settings (ping interval, maximum frame size, extensions).
 */
open class WebSocketConnector(
    val subprotocols: List<String> = emptyList(),
) : ClientConnector<Frame, Frame> {
    override suspend fun connect(context: ConnectionContext, request: ConnectRequest): Duplex<Frame> {
        val client = context.httpClient
        checkNotNull(client.pluginOrNull(WebSockets)) {
            "The WebSockets plugin is not installed: create the client with HttpClient { install(WebSockets) }"
        }
        val session = client.webSocketSession {
            url(request.url)
            headers.appendAll(request.headers)
            if (subprotocols.isNotEmpty())
                header(HttpHeaders.SecWebSocketProtocol, subprotocols.joinToString(","))
        }
        val responseHeaders = session.call.response.headers
        val attributes = Attributes(concurrent = true).apply {
            put(ConnectionAttributes.Url, request.url)
            put(ConnectionAttributes.ResponseHeaders, responseHeaders)
            responseHeaders[HttpHeaders.SecWebSocketProtocol]?.let { put(WebSocketAttributes.Subprotocol, it) }
        }
        return session.toConnection(context.coroutineContext, attributes)
    }
}

/** Exchanges [M] messages as text frames serialized with [format]. */
inline fun <reified M> ClientConnector<Frame, Frame>.withFormat(
    format: StringFormat,
    serializer: KSerializer<M> = serializer<M>(),
): ClientConnector<M, M> = with(textFormat(format, serializer))
