@file:OptIn(ExperimentalKtorApi::class)
package io.ktor.client.messaging

import io.ktor.messaging.*
import io.ktor.util.*
import io.ktor.utils.io.*
import kotlinx.atomicfu.atomic
import kotlinx.atomicfu.update
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ClosedSendChannelException
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlin.coroutines.CoroutineContext

/**
 * Wraps this connector so that dropped connections are transparently re-established.
 *
 * The initial connection is made eagerly and fails fast. Afterwards, whenever the current connection ends,
 * a new one is opened, pacing every attempt with the configured [ReconnectDelay].
 *
 * Apply this outermost: `WebSocketTransport.stomp().withReconnect()` re-runs the STOMP handshake for every new
 * connection, whereas layers applied *after* `withReconnect` would not notice that the connection was replaced.
 */
fun <Out, In> ClientConnector<Out, In>.withReconnect(
    configure: ReconnectConfig.() -> Unit = {},
): ReconnectingConnector<Out, In> = ReconnectingConnector(this, ReconnectConfig().apply(configure))

/** Connects to [url] using a reconnecting [connector], exposing its [ReconnectingConnection.state]. */
suspend fun <Out, In> io.ktor.client.HttpClient.connect(
    connector: ReconnectingConnector<Out, In>,
    url: String = "",
    block: ConnectRequestBuilder.() -> Unit = {},
): ReconnectingConnection<Out, In> =
    connector.connect(HttpClientConnectionContext(this), ConnectRequestBuilder(url).apply(block).build())

class ReconnectConfig {
    /** Creates the delay for each connection made through the connector. */
    var delay: () -> ReconnectDelay = { ReconnectDelay.exponential() }

    /**
     * Whether a failure (a dropped connection or a failed attempt) should be followed by another attempt.
     * Connections that end normally are always re-established. By default, [FatalConnectionFailure]s stop.
     */
    var retryIf: (Throwable) -> Boolean = { it !is FatalConnectionFailure }

    /** Buffer for outgoing messages, used while disconnected or while the connection is slow. */
    var outgoingCapacity: Int = Channel.RENDEZVOUS

    /** What happens to messages sent while disconnected. */
    var whileDisconnected: DisconnectedSendPolicy = DisconnectedSendPolicy.SUSPEND
}

enum class DisconnectedSendPolicy {
    /** Senders wait (once the outgoing buffer is full) until a connection is available again. */
    SUSPEND,

    /** Messages sent while disconnected are discarded. */
    DROP,
}

/** Marks failures that retrying cannot fix, e.g. rejected credentials; they stop [withReconnect] by default. */
interface FatalConnectionFailure

open class FatalConnectionException(message: String, cause: Throwable? = null) :
    Exception(message, cause), FatalConnectionFailure

sealed interface ConnectionState {
    data object Connecting : ConnectionState

    /** Connected; [reconnects] counts the connections established after the initial one. */
    data class Connected(val attributes: Attributes, val reconnects: Int) : ConnectionState

    /** Waiting for, or making, connection [attempt] after the previous connection ended with [cause]. */
    data class Reconnecting(val attempt: Int, val cause: Throwable?) : ConnectionState

    data class Closed(val cause: Throwable?) : ConnectionState
}

class ReconnectingConnector<Out, In> internal constructor(
    private val base: ClientConnector<Out, In>,
    private val config: ReconnectConfig,
) : ClientConnector<Out, In> {
    override suspend fun connect(context: ConnectionContext, request: ConnectRequest): ReconnectingConnection<Out, In> =
        ReconnectingConnection.open(context, config) { connectionContext -> base.connect(connectionContext, request) }
}

/**
 * A connection that replaces its underlying connection whenever it ends. [incoming] and [outgoing] stay the same
 * across reconnects; [attributes] are those of the current underlying connection.
 *
 * - [ReconnectDelay.delay] is called before every attempt and [ReconnectDelay.reset] once a re-established
 *   connection successfully sends or receives. When the delay throws, or [ReconnectConfig.retryIf] rejects
 *   a failure, this connection ends with that exception as the cause.
 * - A message whose delivery was interrupted by a dropped connection is sent again on the next one.
 *   Messages already accepted by the underlying connection when it dropped are lost.
 * - [onReconnect] hooks run on each new connection before messages flow again, to restore state.
 */
class ReconnectingConnection<Out, In> private constructor(
    context: ConnectionContext,
    config: ReconnectConfig,
    private val connect: suspend (ConnectionContext) -> ChannelConnection<Out, In>,
) : ChannelConnection<Out, In> {
    private val job = Job(context.coroutineContext[Job])
    override val coroutineContext: CoroutineContext = context.coroutineContext + job

    /** Parent of the underlying connections; a supervisor, so that their failures don't end this connection. */
    private val connections = SupervisorJob(job)
    private val connectionContext = context.withParent(connections)

    private val delay = config.delay()
    private val retryIf = config.retryIf
    private val whileDisconnected = config.whileDisconnected

    private val incomingChannel = Channel<In>(Channel.BUFFERED)
    private val outgoingChannel = Channel<Out>(config.outgoingCapacity)
    override val incoming: ReceiveChannel<In> get() = incomingChannel
    override val outgoing: SendChannel<Out> get() = outgoingChannel

    private val stateFlow = MutableStateFlow<ConnectionState>(ConnectionState.Connecting)
    val state: StateFlow<ConnectionState> = stateFlow.asStateFlow()

    private val current = atomic<ChannelConnection<Out, In>?>(null)
    override val attributes: Attributes get() = current.value?.attributes ?: EMPTY_ATTRIBUTES

    private val hooks = atomic(emptyList<suspend (ChannelConnection<Out, In>) -> Unit>())
    private val pendingReset = atomic(false)

    private class Closing(val cause: Throwable?)

    private val closing = atomic<Closing?>(null)
    private val reconnecting = atomic<Deferred<*>?>(null)
    private val terminated = atomic(false)
    private lateinit var supervisor: Job

    /** A message taken from [outgoing] whose delivery has not been confirmed; touched by one coroutine at a time. */
    private var pending: Any? = NO_MESSAGE

    internal companion object {
        private val NO_MESSAGE = Any()
        private val EMPTY_ATTRIBUTES = Attributes()

        suspend fun <Out, In> open(
            context: ConnectionContext,
            config: ReconnectConfig,
            connect: suspend (ConnectionContext) -> ChannelConnection<Out, In>,
        ): ReconnectingConnection<Out, In> {
            val connection = ReconnectingConnection(context, config, connect)
            val initial = try {
                connect(connection.connectionContext)
            } catch (e: Throwable) {
                connection.cancel(CancellationException("Initial connection failed", e))
                throw e
            }
            connection.start(initial)
            return connection
        }
    }

    /** Registers [hook] to run on every re-established connection, before messages flow again. */
    fun onReconnect(hook: suspend (connection: ChannelConnection<Out, In>) -> Unit) {
        hooks.update { it + hook }
    }

    private fun start(initial: ChannelConnection<Out, In>) {
        job.invokeOnCompletion { cause -> terminate(cause?.connectionFailure()) }
        current.value = initial
        stateFlow.value = ConnectionState.Connected(initial.attributes, reconnects = 0)
        supervisor = launch(start = CoroutineStart.ATOMIC) {
            try {
                supervise(initial)
            } catch (e: Throwable) {
                // when cancelled, this connection is terminating already
                if (isActive) terminate(e)
            }
        }
    }

    private suspend fun supervise(initial: ChannelConnection<Out, In>) {
        var connection = initial
        var reconnects = 0
        while (true) {
            current.value = connection
            stateFlow.value = ConnectionState.Connected(connection.attributes, reconnects)
            val outcome = try {
                if (reconnects > 0) for (hook in hooks.value) hook(connection)
                pump(connection)
            } catch (e: Throwable) {
                rethrowIfCancelled(e)
                Outcome.Dropped(e)
            }
            when (outcome) {
                Outcome.Closed -> return
                is Outcome.Dropped -> {
                    connection.cancel()
                    val cause = outcome.cause
                    if (cause != null && !retryIf(cause)) return terminate(cause)
                    connection = reconnectUnlessClosing(cause) ?: return
                    reconnects++
                    pendingReset.value = true
                }
            }
        }
    }

    private sealed interface Outcome {
        data object Closed : Outcome
        class Dropped(val cause: Throwable?) : Outcome
    }

    /** Copies messages between this connection and [connection] until either side ends. */
    private suspend fun pump(connection: ChannelConnection<Out, In>): Outcome = coroutineScope {
        val outcome = CompletableDeferred<Outcome>()
        val receiving = launch {
            val cause = try {
                for (message in connection.incoming) {
                    incomingChannel.send(message)
                    onSuccess()
                }
                null
            } catch (e: Throwable) {
                if (!isActive) return@launch
                e
            }
            outcome.complete(Outcome.Dropped(cause))
        }
        val sending = launch {
            try {
                while (true) {
                    if (pending === NO_MESSAGE) {
                        val next = outgoingChannel.receiveCatching()
                        if (next.isClosed) {
                            outcome.complete(Outcome.Closed)
                            return@launch
                        }
                        pending = next.getOrThrow()
                    }
                    @Suppress("UNCHECKED_CAST")
                    connection.outgoing.send(pending as Out)
                    pending = NO_MESSAGE
                    onSuccess()
                }
            } catch (_: ClosedSendChannelException) {
                // the connection is gone; the receiving side reports how it ended
            } catch (e: Throwable) {
                if (isActive) outcome.complete(Outcome.Dropped(e))
            }
        }
        val result = outcome.await()
        sending.cancel()
        receiving.cancel()
        if (result == Outcome.Closed) connection.close(closing.value?.cause)
        result
    }

    private suspend fun onSuccess() {
        if (pendingReset.compareAndSet(true, false)) delay.reset()
    }

    /** Reconnects, returning `null` when closed by the user or when reconnecting gave up. */
    private suspend fun reconnectUnlessClosing(cause: Throwable?): ChannelConnection<Out, In>? = coroutineScope {
        val attempt = async { reconnect(cause) }
        reconnecting.value = attempt
        if (closing.value != null) attempt.cancel()
        try {
            attempt.await()
        } catch (e: CancellationException) {
            if (closing.value == null) throw e
            null
        } finally {
            reconnecting.value = null
        }
    }

    private suspend fun reconnect(cause: Throwable?): ChannelConnection<Out, In>? = coroutineScope {
        val dropper = if (whileDisconnected == DisconnectedSendPolicy.DROP) {
            pending = NO_MESSAGE
            launch { for (ignored in outgoingChannel) {} }
        } else null
        try {
            var attempt = 0
            var lastCause = cause
            while (true) {
                stateFlow.value = ConnectionState.Reconnecting(++attempt, lastCause)
                try {
                    delay.delay()
                } catch (e: Throwable) {
                    rethrowIfCancelled(e)
                    terminate(e)
                    return@coroutineScope null
                }
                try {
                    return@coroutineScope connect(connectionContext)
                } catch (e: Throwable) {
                    rethrowIfCancelled(e)
                    if (!retryIf(e)) {
                        terminate(e)
                        return@coroutineScope null
                    }
                    lastCause = e
                }
            }
            @Suppress("UNREACHABLE_CODE")
            null
        } finally {
            dropper?.cancel()
        }
    }

    override suspend fun close(cause: Throwable?) {
        if (closing.compareAndSet(null, Closing(cause))) {
            outgoingChannel.close()
            reconnecting.value?.cancel()
        }
        supervisor.join()
        terminate(closing.value?.cause)
    }

    private fun terminate(cause: Throwable?) {
        if (!terminated.compareAndSet(false, true)) return
        stateFlow.value = ConnectionState.Closed(cause)
        incomingChannel.close(cause)
        outgoingChannel.close(cause)
        current.value?.cancel()
        connections.cancel()
        if (cause == null) job.complete()
        else job.cancel(CancellationException("Connection failed", cause))
    }
}
