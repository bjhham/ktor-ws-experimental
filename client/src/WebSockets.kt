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
val WebSocket = ClientConnector { context, request ->
    val client = context.httpClient.takeIf { it.pluginOrNull(WebSockets) != null }
        ?: context.httpClient.config { install(WebSockets) }
    val session = client.webSocketSession {
        url(request.url)
        headers.appendAll(request.headers)
    }
    val responseHeaders = session.call.response.headers
    val attributes = Attributes(concurrent = true).apply {
        put(ConnectionAttributes.Url, request.url)
        put(ConnectionAttributes.ResponseHeaders, responseHeaders)
        responseHeaders[HttpHeaders.SecWebSocketProtocol]?.let {
            put(WebSocketAttributes.Subprotocol, it)
        }
    }
    session.toConnection(context.coroutineContext, attributes)
}

/** Exchanges [M] messages as text frames serialized with [format]. */
inline fun <reified M> ClientConnector<Frame, Frame>.withFormat(
    format: StringFormat,
    serializer: KSerializer<M> = serializer<M>(),
): ClientConnector<M, M> = with(textFormat(format, serializer))
