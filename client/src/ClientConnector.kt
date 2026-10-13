package io.ktor.client.messaging

import io.ktor.client.*
import io.ktor.http.*
import io.ktor.messaging.*
import io.ktor.util.*
import io.ktor.utils.io.*
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.isActive
import kotlin.coroutines.CoroutineContext

/**
 * Establishes [MessageConnection]s: a transport (e.g. [WebSocketTransport]), optionally wrapped with
 * [ConnectionLayer]s ([with]) and client-only behaviour such as [withReconnect] or [orElse].
 */
@ExperimentalKtorApi
fun interface ClientConnector<Out, In> {
    suspend fun connect(context: ConnectionContext, request: ConnectRequest): MessageConnection<Out, In>
}

/**
 * Where connections are made from. Connections become children of [coroutineContext]'s job.
 * Transports that need more than a coroutine scope look for a specific context type, e.g. HTTP-based
 * transports require an [HttpClientConnectionContext].
 */
open class ConnectionContext(final override val coroutineContext: CoroutineContext) : CoroutineScope {
    /** A copy of this context whose connections become children of [job]. */
    open fun withParent(job: Job): ConnectionContext = ConnectionContext(coroutineContext + job)
}

class HttpClientConnectionContext(
    val client: HttpClient,
    coroutineContext: CoroutineContext = client.coroutineContext,
) : ConnectionContext(coroutineContext) {
    override fun withParent(job: Job): ConnectionContext = HttpClientConnectionContext(client, coroutineContext + job)
}

/** The [HttpClient] of this context; fails for contexts without one. */
val ConnectionContext.httpClient: HttpClient
    get() = (this as? HttpClientConnectionContext)?.client
        ?: throw IllegalArgumentException("This transport requires an HttpClient, but the context is $this")

/** Where and how to connect. [url] may be relative, to be resolved by the transport (e.g. by `DefaultRequest`). */
class ConnectRequest(
    val url: String,
    val headers: Headers = Headers.Empty,
    /** Settings for transports and layers. */
    val attributes: Attributes = Attributes(concurrent = true),
)

class ConnectRequestBuilder(var url: String = "") {
    val headers: HeadersBuilder = HeadersBuilder()
    val attributes: Attributes = Attributes(concurrent = true)

    fun header(name: String, value: String) = headers.append(name, value)

    fun build(): ConnectRequest = ConnectRequest(url, headers.build(), attributes)
}

/** Attributes describing a client connection. */
object ConnectionAttributes {
    val Url: AttributeKey<String> = AttributeKey("ConnectionUrl")
    /** Headers of the HTTP response that established the connection, if any. */
    val ResponseHeaders: AttributeKey<Headers> = AttributeKey("ConnectionResponseHeaders")
}

/** Connects to [url] using [connector]. */
@ExperimentalKtorApi
suspend fun <Out, In> HttpClient.connect(
    connector: ClientConnector<Out, In>,
    url: String = "",
    block: ConnectRequestBuilder.() -> Unit = {},
): MessageConnection<Out, In> =
    connector.connect(HttpClientConnectionContext(this), ConnectRequestBuilder(url).apply(block).build())

/** Applies [layer] to every connection made by this connector. */
@ExperimentalKtorApi
fun <A, B> ClientConnector<A, A>.with(layer: ConnectionLayer<A, B>): ClientConnector<B, B> {
    val base = this
    return ClientConnector { context, request -> layer.wrapOrCancel(base.connect(context, request)) }
}

/** Uses [fallback] when this connector fails to connect, e.g. when WebSockets are blocked. */
@ExperimentalKtorApi
fun <Out, In> ClientConnector<Out, In>.orElse(fallback: ClientConnector<Out, In>): ClientConnector<Out, In> {
    val primary = this
    return ClientConnector { context, request ->
        try {
            primary.connect(context, request)
        } catch (e: Throwable) {
            rethrowIfCancelled(e)
            try {
                fallback.connect(context, request)
            } catch (second: Throwable) {
                second.addSuppressed(e)
                throw second
            }
        }
    }
}

/** Rethrows [e] if it is a cancellation of the current coroutine rather than a failure. */
internal suspend fun rethrowIfCancelled(e: Throwable) {
    if (e is CancellationException && !currentCoroutineContext().isActive) throw e
}
