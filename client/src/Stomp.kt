@file:OptIn(ExperimentalKtorApi::class)
package io.ktor.client.messaging

import io.ktor.client.*
import io.ktor.http.*
import io.ktor.messaging.*
import io.ktor.util.*
import io.ktor.utils.io.*
import io.ktor.websocket.*
import kotlinx.atomicfu.atomic
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ClosedReceiveChannelException
import kotlinx.coroutines.channels.ClosedSendChannelException
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

// ---------------------------------------------------------------------------------------------------------------------
// Protocol entry points
// ---------------------------------------------------------------------------------------------------------------------

/** WebSocket sub-protocols registered for STOMP, most recent version first. */
val STOMP_SUBPROTOCOLS: List<String> = listOf("v12.stomp", "v11.stomp", "v10.stomp")

/** WebSocket transport that negotiates the STOMP sub-protocols. */
object StompWebSocket : WebSocketConnector(STOMP_SUBPROTOCOLS)

/** STOMP over WebSockets with default settings; use [StompWebSocket].[stomp] to customise. */
val STOMP: ClientConnector<StompFrame, StompFrame> = StompWebSocket.stomp()

/**
 * Runs STOMP on top of this WebSocket transport.
 *
 * Connecting performs the `CONNECT` / `CONNECTED` handshake and negotiates heart-beats; the resulting connection
 * sends and receives raw [StompFrame]s (heart-beats are handled internally and never surface). The `CONNECTED`
 * frame is available as [StompAttributes.Connected]. For subscriptions, receipts and acknowledgements,
 * use a [StompSession].
 */
fun ClientConnector<Frame, Frame>.stomp(configure: StompConfig.() -> Unit = {}): ClientConnector<StompFrame, StompFrame> {
    val config = StompConfig().apply(configure)
    val transport = this
    return ClientConnector { context, request ->
        val base = transport.connect(context, request)
        try {
            base.stompHandshake(config, defaultHost(request.url))
        } catch (e: Throwable) {
            base.cancel(CancellationException("STOMP handshake failed", e))
            throw e
        }
    }
}

/** Connects to a STOMP endpoint and starts a [StompSession] on it. */
suspend fun HttpClient.stompSession(
    url: String = "",
    connector: ClientConnector<StompFrame, StompFrame> = STOMP,
    block: ConnectRequestBuilder.() -> Unit = {},
): StompSession = StompSession(connect(connector, url, block))

class StompConfig {
    /** Virtual host sent in the `host` header; defaults to the host of the connection URL. */
    var host: String? = null
    var login: String? = null
    var passcode: String? = null
    var acceptVersion: String = "1.1,1.2"
    /** Desired heart-beats; the effective values are negotiated with the server. */
    var heartBeat: StompHeartBeat = StompHeartBeat(outgoing = 10.seconds, incoming = 10.seconds)
    /** The connection is considered dead after `incoming heart-beat * tolerance` without any data. */
    var heartBeatTolerance: Double = 2.0
    var connectTimeout: Duration = 10.seconds
    var maxFrameSize: Int = DEFAULT_MAX_FRAME_SIZE
    /** Additional `CONNECT` headers. */
    val headers: MutableMap<String, String> = linkedMapOf()
}

/** Heart-beat intervals; [Duration.ZERO] disables the respective direction. */
data class StompHeartBeat(
    val outgoing: Duration = Duration.ZERO,
    val incoming: Duration = Duration.ZERO,
) {
    fun toHeader(): String = "${outgoing.inWholeMilliseconds},${incoming.inWholeMilliseconds}"

    companion object {
        val NONE = StompHeartBeat()

        fun parse(header: String?): StompHeartBeat {
            val parts = header?.split(',')?.map { it.trim().toLongOrNull() } ?: return NONE
            if (parts.size != 2 || parts.any { it == null || it < 0 })
                throw StompProtocolException("Invalid heart-beat header: $header")
            return StompHeartBeat(parts[0]!!.milliseconds, parts[1]!!.milliseconds)
        }

        /** Combines the [client] wishes with the [server] `heart-beat` header, as described by the spec. */
        fun negotiate(client: StompHeartBeat, server: StompHeartBeat) = StompHeartBeat(
            outgoing = if (client.outgoing.isPositive() && server.incoming.isPositive())
                maxOf(client.outgoing, server.incoming) else Duration.ZERO,
            incoming = if (client.incoming.isPositive() && server.outgoing.isPositive())
                maxOf(client.incoming, server.outgoing) else Duration.ZERO,
        )
    }
}

/** Attributes of a STOMP connection. */
object StompAttributes {
    /** The server's `CONNECTED` frame. */
    val Connected: AttributeKey<StompFrame> = AttributeKey("StompConnected")
    /** The negotiated heart-beats. */
    val HeartBeat: AttributeKey<StompHeartBeat> = AttributeKey("StompHeartBeat")
}

/** The server's `CONNECTED` frame, for STOMP connections. */
val MessageConnection<*, *>.stompConnected: StompFrame?
    get() = attributes.getOrNull(StompAttributes.Connected)

val StompFrame.version: String? get() = this[StompHeaders.VERSION]
val StompFrame.server: String? get() = this[StompHeaders.SERVER]
val StompFrame.sessionId: String? get() = this[StompHeaders.SESSION]

private fun defaultHost(url: String): String =
    runCatching { Url(url).host }.getOrNull()?.takeIf { it.isNotEmpty() } ?: "localhost"

private suspend fun Duplex<Frame>.stompHandshake(config: StompConfig, host: String): Duplex<StompFrame> {
    val base = this
    base.attributes.getOrNull(WebSocketAttributes.Subprotocol)?.let {
        if (it !in STOMP_SUBPROTOCOLS) throw StompConnectException("The server selected an unsupported subprotocol: $it")
    }
    val decoder = StompFrameDecoder(config.maxFrameSize)
    val connectHeaders = linkedMapOf(
        StompHeaders.ACCEPT_VERSION to config.acceptVersion,
        StompHeaders.HOST to (config.host ?: host),
        StompHeaders.HEART_BEAT to config.heartBeat.toHeader(),
    )
    config.login?.let { connectHeaders[StompHeaders.LOGIN] = it }
    config.passcode?.let { connectHeaders[StompHeaders.PASSCODE] = it }
    connectHeaders.putAll(config.headers)
    base.outgoing.send(StompFrame(StompCommand.CONNECT, connectHeaders).toWebSocketFrame())

    val received = try {
        withTimeoutOrNull(config.connectTimeout) { base.incoming.receiveStompFrames(decoder) }
    } catch (e: ClosedReceiveChannelException) {
        throw StompConnectException("Connection closed during the STOMP handshake", e)
    } ?: throw StompConnectException("No CONNECTED frame received within ${config.connectTimeout}")

    val connected = received.first()
    when (connected.command) {
        StompCommand.CONNECTED -> {}
        StompCommand.ERROR -> throw StompConnectRejectedException(connected)
        else -> throw StompProtocolException("Expected CONNECTED, but received ${connected.command}")
    }
    val heartBeat = StompHeartBeat.negotiate(config.heartBeat, StompHeartBeat.parse(connected[StompHeaders.HEART_BEAT]))
    val readTimeout = heartBeat.incoming.takeIf { it.isPositive() }?.times(config.heartBeatTolerance)
    base.attributes.put(StompAttributes.Connected, connected)
    base.attributes.put(StompAttributes.HeartBeat, heartBeat)

    return base.layer(
        receive = { from, to ->
            if (heartBeat.outgoing.isPositive()) launch { sendHeartBeats(base.outgoing, heartBeat.outgoing) }
            for (frame in received.drop(1)) to.send(frame)
            var open = true
            while (open) {
                val result = if (readTimeout == null) from.receiveCatching()
                    else withTimeoutOrNull(readTimeout) { from.receiveCatching() }
                        ?: throw StompHeartBeatTimeoutException(readTimeout)
                if (result.isClosed) {
                    result.exceptionOrNull()?.let { throw it }
                    open = false
                } else {
                    val bytes = result.getOrThrow().stompBytes() ?: continue
                    for (frame in decoder.feed(bytes)) to.send(frame)
                }
            }
        },
        send = { from, to -> for (frame in from) to.send(frame.toWebSocketFrame()) },
    )
}

private suspend fun ReceiveChannel<Frame>.receiveStompFrames(decoder: StompFrameDecoder): List<StompFrame> {
    while (true) {
        val bytes = receive().stompBytes() ?: continue
        val frames = decoder.feed(bytes)
        if (frames.isNotEmpty()) return frames
    }
}

private suspend fun sendHeartBeats(outgoing: SendChannel<Frame>, interval: Duration) {
    try {
        while (true) {
            delay(interval)
            outgoing.send(Frame.Text("\n"))
        }
    } catch (e: CancellationException) {
        throw e
    } catch (_: Throwable) {
        // the connection is gone; its receiving side reports how it ended
    }
}

private fun Frame.stompBytes(): ByteArray? = when (this) {
    is Frame.Text, is Frame.Binary -> data
    else -> null
}

/** Encodes as a text frame when the body is valid UTF-8, otherwise as a binary frame. */
internal fun StompFrame.toWebSocketFrame(): Frame {
    val bytes = encode()
    val isText = runCatching { body.decodeToString(throwOnInvalidSequence = true) }.isSuccess
    return if (isText) Frame.Text(true, bytes) else Frame.Binary(true, bytes)
}

// ---------------------------------------------------------------------------------------------------------------------
// Session
// ---------------------------------------------------------------------------------------------------------------------

enum class StompAckMode(val header: String) {
    AUTO("auto"),
    CLIENT("client"),
    CLIENT_INDIVIDUAL("client-individual"),
}

/**
 * High-level STOMP client on top of a [StompFrame] connection: routes `MESSAGE` frames to their
 * [StompSubscription], completes receipts and handles acknowledgements, transactions and graceful disconnects.
 *
 * An `ERROR` frame from the server is fatal: the session closes with a [StompErrorException].
 *
 * On a [ReconnectingConnection] (`STOMP.withReconnect()`), the session restores its subscriptions on every
 * new connection; receipts pending when the connection dropped fail with [StompConnectionLostException].
 */
class StompSession(private val connection: Duplex<StompFrame>) {
    /** `null` once the session is closed. */
    private val subscriptions = atomic<Map<String, StompSubscription>?>(emptyMap())
    private val receipts = atomic<Map<String, CompletableDeferred<StompFrame>>?>(emptyMap())
    private val nextId = atomic(0)
    private val closed = atomic(false)
    private val closeCause = atomic<Throwable?>(null)

    private val dispatcher: Job = connection.launch(start = CoroutineStart.ATOMIC) { dispatch() }

    init {
        @Suppress("UNCHECKED_CAST")
        (connection as? ReconnectingConnection<StompFrame, StompFrame>)?.onReconnect { restore(it) }
    }

    val isActive: Boolean get() = !closed.value

    private suspend fun dispatch() {
        var cause: Throwable? = null
        try {
            for (frame in connection.incoming) {
                when (frame.command) {
                    StompCommand.MESSAGE ->
                        frame.subscription?.let { subscriptions.value?.get(it) }?.messages?.trySend(frame)
                    StompCommand.RECEIPT ->
                        frame.receiptId?.let { removeReceipt(it) }?.complete(frame)
                    StompCommand.ERROR -> {
                        val error = StompErrorException(frame)
                        frame.receiptId?.let { removeReceipt(it) }?.completeExceptionally(error)
                        cause = error
                        break
                    }
                    else -> {}
                }
            }
        } catch (e: Throwable) {
            cause = e
        } finally {
            close(cause)
        }
    }

    /** Runs on a re-established connection: pending receipts can't arrive anymore, subscriptions must be renewed. */
    private suspend fun restore(newConnection: MessageConnection<StompFrame, StompFrame>) {
        val lost = StompConnectionLostException()
        while (true) {
            val current = receipts.value ?: return
            if (receipts.compareAndSet(current, emptyMap())) {
                current.values.forEach { it.completeExceptionally(lost) }
                break
            }
        }
        for (subscription in subscriptions.value?.values.orEmpty())
            newConnection.outgoing.send(subscription.subscribeFrame)
    }

    suspend fun send(
        destination: String,
        body: String,
        contentType: String? = "text/plain;charset=utf-8",
        headers: Map<String, String> = emptyMap(),
        receipt: Boolean = false,
    ) = send(destination, body.encodeToByteArray(), contentType, headers, receipt)

    suspend fun send(
        destination: String,
        body: ByteArray,
        contentType: String? = null,
        headers: Map<String, String> = emptyMap(),
        receipt: Boolean = false,
    ) {
        val frameHeaders = linkedMapOf(StompHeaders.DESTINATION to destination)
        contentType?.let { frameHeaders[StompHeaders.CONTENT_TYPE] = it }
        frameHeaders.putAll(headers)
        send(StompFrame(StompCommand.SEND, frameHeaders, body), receipt)
    }

    /** Sends a raw [frame]; with [receipt], suspends until the server confirms it with a `RECEIPT` frame. */
    suspend fun send(frame: StompFrame, receipt: Boolean = false) {
        if (!receipt) return connection.outgoing.send(frame)
        val id = "receipt-${nextId.incrementAndGet()}"
        val deferred = CompletableDeferred<StompFrame>()
        if (!registerReceipt(id, deferred)) throw closedException()
        try {
            connection.outgoing.send(frame.withHeaders(StompHeaders.RECEIPT to id))
            deferred.await()
        } finally {
            removeReceipt(id)
        }
    }

    /**
     * Subscribes to [destination]. Messages are buffered without limit until consumed from the returned
     * subscription, so that a slow consumer never stalls the other subscriptions or pending receipts.
     */
    suspend fun subscribe(
        destination: String,
        ack: StompAckMode = StompAckMode.AUTO,
        headers: Map<String, String> = emptyMap(),
        receipt: Boolean = false,
    ): StompSubscription {
        val id = "sub-${nextId.incrementAndGet()}"
        val frame = StompFrame(StompCommand.SUBSCRIBE, headers + mapOf(
            StompHeaders.ID to id,
            StompHeaders.DESTINATION to destination,
            StompHeaders.ACK to ack.header,
        ))
        val subscription = StompSubscription(this, id, destination, ack, frame)
        if (!registerSubscription(subscription)) throw closedException()
        try {
            send(frame, receipt)
        } catch (e: Throwable) {
            removeSubscription(id)
            subscription.messages.close(e)
            throw e
        }
        return subscription
    }

    suspend fun unsubscribe(subscription: StompSubscription, receipt: Boolean = false) {
        if (removeSubscription(subscription.id) == null) return
        subscription.messages.close()
        send(StompFrame(StompCommand.UNSUBSCRIBE, mapOf(StompHeaders.ID to subscription.id)), receipt)
    }

    suspend fun ack(message: StompFrame, transaction: String? = null) =
        send(acknowledgement(StompCommand.ACK, message, transaction))

    suspend fun nack(message: StompFrame, transaction: String? = null) =
        send(acknowledgement(StompCommand.NACK, message, transaction))

    private fun acknowledgement(command: StompCommand, message: StompFrame, transaction: String?): StompFrame {
        val headers = linkedMapOf<String, String>()
        val ackId = message[StompHeaders.ACK]
        if (ackId != null) {
            headers[StompHeaders.ID] = ackId // STOMP 1.2
        } else {
            headers[StompHeaders.MESSAGE_ID] = message.messageId
                ?: throw IllegalArgumentException("Message has neither an ack nor a message-id header")
            message.subscription?.let { headers[StompHeaders.SUBSCRIPTION] = it } // STOMP 1.1
        }
        transaction?.let { headers[StompHeaders.TRANSACTION] = it }
        return StompFrame(command, headers)
    }

    /**
     * Runs [block] inside a transaction: `BEGIN` before, `COMMIT` after, or `ABORT` if [block] fails.
     * Pass the transaction id to [send] (as the `transaction` header), [ack] or [nack] to include them.
     */
    suspend fun <T> transaction(block: suspend (transaction: String) -> T): T {
        val id = "tx-${nextId.incrementAndGet()}"
        val header = mapOf(StompHeaders.TRANSACTION to id)
        send(StompFrame(StompCommand.BEGIN, header))
        val result = try {
            block(id)
        } catch (e: Throwable) {
            runCatching { send(StompFrame(StompCommand.ABORT, header)) }
            throw e
        }
        send(StompFrame(StompCommand.COMMIT, header))
        return result
    }

    /** Sends `DISCONNECT`, waits up to [timeout] for its receipt, then closes the connection and the session. */
    suspend fun disconnect(timeout: Duration = 5.seconds) {
        try {
            withTimeoutOrNull(timeout) { send(StompFrame(StompCommand.DISCONNECT), receipt = true) }
        } catch (e: CancellationException) {
            throw e
        } catch (_: Throwable) {
            // the connection is gone already
        }
        try {
            connection.close()
        } finally {
            close()
        }
    }

    /** Closes the session immediately, closing all subscriptions and failing pending receipts. */
    fun close(cause: Throwable? = null) {
        if (!closed.compareAndSet(false, true)) return
        closeCause.value = cause
        val error = closedException()
        subscriptions.getAndSet(null)?.values?.forEach { it.messages.close(cause) }
        receipts.getAndSet(null)?.values?.forEach { it.completeExceptionally(error) }
        connection.cancel(CancellationException("STOMP session closed", cause))
        dispatcher.cancel()
    }

    private fun closedException(): Throwable =
        closeCause.value ?: StompSessionClosedException()

    private fun registerReceipt(id: String, deferred: CompletableDeferred<StompFrame>): Boolean {
        while (true) {
            val current = receipts.value ?: return false
            if (receipts.compareAndSet(current, current + (id to deferred))) return true
        }
    }

    private fun removeReceipt(id: String): CompletableDeferred<StompFrame>? {
        while (true) {
            val current = receipts.value ?: return null
            val removed = current[id] ?: return null
            if (receipts.compareAndSet(current, current - id)) return removed
        }
    }

    private fun registerSubscription(subscription: StompSubscription): Boolean {
        while (true) {
            val current = subscriptions.value ?: return false
            if (subscriptions.compareAndSet(current, current + (subscription.id to subscription))) return true
        }
    }

    private fun removeSubscription(id: String): StompSubscription? {
        while (true) {
            val current = subscriptions.value ?: return null
            val removed = current[id] ?: return null
            if (subscriptions.compareAndSet(current, current - id)) return removed
        }
    }
}

/** The `MESSAGE` frames of one subscription. */
class StompSubscription internal constructor(
    private val session: StompSession,
    val id: String,
    val destination: String,
    val ackMode: StompAckMode,
    /** Sent again when the connection is re-established. */
    internal val subscribeFrame: StompFrame,
    internal val messages: Channel<StompFrame> = Channel(Channel.UNLIMITED),
) : ReceiveChannel<StompFrame> by messages {
    suspend fun unsubscribe(receipt: Boolean = false) = session.unsubscribe(this, receipt)
    suspend fun ack(message: StompFrame, transaction: String? = null) = session.ack(message, transaction)
    suspend fun nack(message: StompFrame, transaction: String? = null) = session.nack(message, transaction)
}


// ---------------------------------------------------------------------------------------------------------------------
// Frames
// ---------------------------------------------------------------------------------------------------------------------

enum class StompCommand {
    // client
    CONNECT, STOMP, SEND, SUBSCRIBE, UNSUBSCRIBE, ACK, NACK, BEGIN, COMMIT, ABORT, DISCONNECT,
    // server
    CONNECTED, MESSAGE, RECEIPT, ERROR;

    /** `CONNECT` and `CONNECTED` frames do not escape their headers, for compatibility with STOMP 1.0. */
    internal val escapesHeaders: Boolean
        get() = this != CONNECT && this != CONNECTED
}

object StompHeaders {
    const val ACCEPT_VERSION = "accept-version"
    const val ACK = "ack"
    const val CONTENT_LENGTH = "content-length"
    const val CONTENT_TYPE = "content-type"
    const val DESTINATION = "destination"
    const val HEART_BEAT = "heart-beat"
    const val HOST = "host"
    const val ID = "id"
    const val LOGIN = "login"
    const val MESSAGE = "message"
    const val MESSAGE_ID = "message-id"
    const val PASSCODE = "passcode"
    const val RECEIPT = "receipt"
    const val RECEIPT_ID = "receipt-id"
    const val SERVER = "server"
    const val SESSION = "session"
    const val SUBSCRIPTION = "subscription"
    const val TRANSACTION = "transaction"
    const val VERSION = "version"
}

class StompFrame(
    val command: StompCommand,
    val headers: Map<String, String> = emptyMap(),
    val body: ByteArray = EMPTY_BODY,
) {
    constructor(command: StompCommand, headers: Map<String, String>, body: String) :
        this(command, headers, body.encodeToByteArray())

    operator fun get(name: String): String? = headers[name]

    val bodyText: String get() = body.decodeToString()
    val destination: String? get() = headers[StompHeaders.DESTINATION]
    val contentType: String? get() = headers[StompHeaders.CONTENT_TYPE]
    val messageId: String? get() = headers[StompHeaders.MESSAGE_ID]
    val subscription: String? get() = headers[StompHeaders.SUBSCRIPTION]
    val receiptId: String? get() = headers[StompHeaders.RECEIPT_ID]

    fun withHeaders(vararg pairs: Pair<String, String>): StompFrame =
        StompFrame(command, headers + pairs, body)

    override fun equals(other: Any?): Boolean =
        other is StompFrame && command == other.command && headers == other.headers && body.contentEquals(other.body)

    override fun hashCode(): Int =
        (command.hashCode() * 31 + headers.hashCode()) * 31 + body.contentHashCode()

    override fun toString(): String =
        "StompFrame($command, headers=$headers, body=${body.size} bytes)"
}

private val EMPTY_BODY = ByteArray(0)
private const val NUL: Byte = 0
private const val LF: Byte = '\n'.code.toByte()
private const val CR: Byte = '\r'.code.toByte()
private const val DEFAULT_MAX_FRAME_SIZE = 8 * 1024 * 1024

/** Encodes this frame, adding a `content-length` header for non-empty bodies when missing. */
fun StompFrame.encode(): ByteArray {
    val escape = command.escapesHeaders
    val head = buildString {
        append(command.name).append('\n')
        for ((name, value) in headers) {
            if (!escape) require(name.none { it == '\n' || it == '\r' || it == ':' } && value.none { it == '\n' || it == '\r' }) {
                "$command headers cannot contain line breaks or colons in names: $name"
            }
            append(if (escape) escapeHeader(name) else name).append(':')
            append(if (escape) escapeHeader(value) else value).append('\n')
        }
        if (body.isNotEmpty() && StompHeaders.CONTENT_LENGTH !in headers)
            append(StompHeaders.CONTENT_LENGTH).append(':').append(body.size).append('\n')
        append('\n')
    }.encodeToByteArray()
    return head + body + NUL
}

private fun escapeHeader(text: String): String = buildString(text.length) {
    for (c in text) when (c) {
        '\\' -> append("\\\\")
        '\r' -> append("\\r")
        '\n' -> append("\\n")
        ':' -> append("\\c")
        else -> append(c)
    }
}

private fun unescapeHeader(text: String): String {
    if ('\\' !in text) return text
    return buildString(text.length) {
        var i = 0
        while (i < text.length) {
            val c = text[i++]
            if (c != '\\') {
                append(c)
                continue
            }
            if (i == text.length) throw StompProtocolException("Unterminated escape sequence in header: $text")
            when (val e = text[i++]) {
                '\\' -> append('\\')
                'r' -> append('\r')
                'n' -> append('\n')
                'c' -> append(':')
                else -> throw StompProtocolException("Undefined escape sequence \\$e in header: $text")
            }
        }
    }
}

/**
 * Incremental STOMP frame decoder: frames may be split across, or batched within, transport messages.
 * Heart-beat end-of-lines between frames are skipped.
 */
class StompFrameDecoder(private val maxFrameSize: Int = DEFAULT_MAX_FRAME_SIZE) {
    private var buffer = EMPTY_BODY

    fun feed(bytes: ByteArray): List<StompFrame> {
        buffer = if (buffer.isEmpty()) bytes else buffer + bytes
        val frames = mutableListOf<StompFrame>()
        var position = 0
        while (true) {
            position = skipEndOfLines(buffer, position)
            if (position >= buffer.size) break
            val (frame, end) = parseFrame(buffer, position) ?: break
            frames += frame
            position = end
        }
        buffer = if (position >= buffer.size) EMPTY_BODY else buffer.copyOfRange(position, buffer.size)
        if (buffer.size > maxFrameSize)
            throw StompProtocolException("Frame exceeds the maximum size of $maxFrameSize bytes")
        return frames
    }

    private fun skipEndOfLines(bytes: ByteArray, start: Int): Int {
        var i = start
        while (i < bytes.size) {
            when {
                bytes[i] == LF -> i++
                bytes[i] == CR && i + 1 < bytes.size && bytes[i + 1] == LF -> i += 2
                else -> return i
            }
        }
        return i
    }

    /** Returns the line starting at [start] (without its EOL) and the position after it, or `null` if incomplete. */
    private fun readLine(bytes: ByteArray, start: Int): Pair<String, Int>? {
        var end = start
        while (end < bytes.size && bytes[end] != LF) end++
        if (end == bytes.size) return null
        val contentEnd = if (end > start && bytes[end - 1] == CR) end - 1 else end
        return bytes.decodeToString(start, contentEnd) to end + 1
    }

    private fun parseFrame(bytes: ByteArray, start: Int): Pair<StompFrame, Int>? {
        var (line, position) = readLine(bytes, start) ?: return null
        val command = StompCommand.entries.firstOrNull { it.name == line }
            ?: throw StompProtocolException("Unknown STOMP command: ${line.take(32)}")
        val unescape = command.escapesHeaders
        val headers = linkedMapOf<String, String>()
        while (true) {
            val (header, next) = readLine(bytes, position) ?: return null
            position = next
            if (header.isEmpty()) break
            val colon = header.indexOf(':')
            if (colon < 0) throw StompProtocolException("Malformed header line: $header")
            val name = header.substring(0, colon).let { if (unescape) unescapeHeader(it) else it }
            val value = header.substring(colon + 1).let { if (unescape) unescapeHeader(it) else it }
            // repeated headers: only the first occurrence is used
            if (name !in headers) headers[name] = value
        }
        val contentLength = headers[StompHeaders.CONTENT_LENGTH]?.let {
            it.toIntOrNull()?.takeIf { length -> length >= 0 }
                ?: throw StompProtocolException("Invalid content-length: $it")
        }
        val bodyEnd = if (contentLength != null) {
            if (contentLength > maxFrameSize)
                throw StompProtocolException("Frame exceeds the maximum size of $maxFrameSize bytes")
            if (position + contentLength >= bytes.size) return null
            if (bytes[position + contentLength] != NUL)
                throw StompProtocolException("Frame body is not terminated by NUL after content-length bytes")
            position + contentLength
        } else {
            var nul = position
            while (nul < bytes.size && bytes[nul] != NUL) nul++
            if (nul == bytes.size) return null
            nul
        }
        return StompFrame(command, headers, bytes.copyOfRange(position, bodyEnd)) to bodyEnd + 1
    }
}

// ---------------------------------------------------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------------------------------------------------

open class StompException(message: String, cause: Throwable? = null) : Exception(message, cause)

/** The server sent an `ERROR` [frame]. */
open class StompErrorException(val frame: StompFrame) : StompException(
    frame[StompHeaders.MESSAGE] ?: frame.bodyText.ifBlank { "STOMP ERROR frame received" }
)

/**
 * The server answered `CONNECT` with an `ERROR` [frame], typically for rejected credentials;
 * [withReconnect] does not retry this by default.
 */
class StompConnectRejectedException(frame: StompFrame) : StompErrorException(frame), FatalConnectionFailure

/** The connection dropped before the server confirmed a receipt. */
class StompConnectionLostException : StompException("The connection was lost before the receipt arrived")

class StompProtocolException(message: String) : StompException(message)

class StompConnectException(message: String, cause: Throwable? = null) : StompException(message, cause)

class StompHeartBeatTimeoutException(val timeout: Duration) :
    StompException("No data received from the server within $timeout")

class StompSessionClosedException : StompException("STOMP session is closed")
