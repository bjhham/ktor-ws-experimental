package io.ktor.messaging

import io.ktor.util.*
import kotlinx.atomicfu.atomic
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ClosedSendChannelException
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext

interface AttributesHolder {
    val attributes: Attributes
}

interface AsyncCloseable {
    suspend fun close(cause: Throwable? = null)
}

interface Receiver<out In>: AsyncCloseable, AttributesHolder {
    val incoming: ReceiveChannel<In>
}

interface Broadcast<in Out>: AsyncCloseable, AttributesHolder {
    val outgoing: SendChannel<Out>
}

/**
 * A two-way connection: [Out] messages are sent through [outgoing], [In] messages arrive on [incoming].
 *
 * Lifecycle:
 * - The connection is a [CoroutineScope] whose [Job] lives exactly as long as the connection. Coroutines that
 *   work on behalf of the connection (and connections layered on top of it) should be launched in this scope.
 * - [incoming] is closed when the connection ends: normally when the remote side closed it, or with the
 *   failure as its cause. Messages that already arrived can still be received after that.
 * - [close] closes the whole connection gracefully: pending [outgoing] messages are delivered first.
 *   Closing [outgoing] has the same effect, without waiting.
 * - Cancelling the scope (`connection.cancel()`) aborts the connection immediately.
 */
interface ChannelConnection<in Out, out In> : Receiver<In>, Broadcast<Out>, CoroutineScope {
    companion object
}

/** A connection with the same message type in both directions. */
typealias ChannelDuplex<M> = ChannelConnection<M, M>

/**
 * Creates a connection whose channels are served by two worker coroutines.
 *
 * - [receive] fills `incoming` (typically from an underlying transport) and returns when the source ends;
 *   the connection then ends too. Throwing fails the connection.
 * - [send] drains `outgoing` until it is closed. Throwing [ClosedSendChannelException] means the underlying
 *   transport is gone and only stops sending; other exceptions fail the connection.
 * - [onClose] gracefully closes the underlying resource once all outgoing messages are delivered.
 * - [onRelease] is called exactly once, after the connection [Job] completed, to release the underlying resource.
 *   It runs after [onClose] for graceful closes, so it should be idempotent.
 *
 * Coroutines launched from the [receive] or [send] scopes are cancelled when the connection ends.
 */
fun <Out, In> ChannelConnection(
    parent: CoroutineContext,
    attributes: Attributes = Attributes(concurrent = true),
    incomingCapacity: Int = Channel.BUFFERED,
    outgoingCapacity: Int = Channel.RENDEZVOUS,
    receive: suspend CoroutineScope.(incoming: SendChannel<In>) -> Unit,
    send: suspend CoroutineScope.(outgoing: ReceiveChannel<Out>) -> Unit,
    onClose: suspend (cause: Throwable?) -> Unit = {},
    onRelease: (cause: Throwable?) -> Unit = {},
): ChannelConnection<Out, In> =
    PipedConnection(parent, attributes, incomingCapacity, outgoingCapacity, receive, send, onClose, onRelease)
        .also { it.start() }

/**
 * Builds a connection on top of this one. The new connection owns this one: closing it closes this connection,
 * and when this connection ends, the new one ends once [receive] has processed the remaining messages.
 * Its [Job] is a child of this connection's job.
 *
 * - [receive] reads from this connection's `incoming` and writes to the new connection's `incoming`;
 *   coroutines launched in its scope (e.g. heart-beats) are cancelled when the new connection ends.
 * - [send] reads from the new connection's `outgoing` and writes to this connection's `outgoing`.
 */
fun <BaseOut, BaseIn, Out, In> ChannelConnection<BaseOut, BaseIn>.layer(
    attributes: Attributes = this.attributes,
    receive: suspend CoroutineScope.(from: ReceiveChannel<BaseIn>, to: SendChannel<In>) -> Unit,
    send: suspend CoroutineScope.(from: ReceiveChannel<Out>, to: SendChannel<BaseOut>) -> Unit,
): ChannelConnection<Out, In> {
    val base = this
    return ChannelConnection(
        parent = base.coroutineContext,
        attributes = attributes,
        receive = { to -> receive(base.incoming, to) },
        send = { from -> send(from, base.outgoing) },
        onClose = { cause -> base.close(cause) },
        onRelease = { cause -> base.cancel(cause?.let { CancellationException("Layered connection failed", it) }) },
    )
}

/**
 * Creates two connected in-memory endpoints: what one sends, the other receives.
 * Closing or failing one side ends the other one as well. Useful for tests and in-process communication.
 */
fun <A, B> ChannelConnection.Companion.pair(
    parent: CoroutineContext = EmptyCoroutineContext,
    capacity: Int = Channel.BUFFERED,
): Pair<ChannelConnection<A, B>, ChannelConnection<B, A>> {
    val aToB = Channel<A>(capacity)
    val bToA = Channel<B>(capacity)
    fun <O, I> endpoint(out: Channel<O>, into: Channel<I>) = ChannelConnection(
        parent = parent,
        receive = { incoming -> for (message in into) incoming.send(message) },
        send = { outgoing -> for (message in outgoing) out.send(message) },
        onClose = { cause -> out.close(cause) },
        onRelease = { cause ->
            out.close(cause)
            into.close()
        },
    )
    return endpoint(aToB, bToA) to endpoint(bToA, aToB)
}

/** Strips the cancellation wrappers coroutines add around a failure; `null` for plain cancellation. */
fun Throwable.connectionFailure(): Throwable? {
    var current: Throwable = this
    while (current is CancellationException) current = current.cause ?: return null
    return current
}

internal class PipedConnection<Out, In>(
    parent: CoroutineContext,
    override val attributes: Attributes,
    incomingCapacity: Int,
    outgoingCapacity: Int,
    private val receiver: suspend CoroutineScope.(SendChannel<In>) -> Unit,
    private val sender: suspend CoroutineScope.(ReceiveChannel<Out>) -> Unit,
    private val onClose: suspend (Throwable?) -> Unit,
    private val onRelease: (Throwable?) -> Unit,
) : ChannelConnection<Out, In> {
    private val job = Job(parent[Job])
    override val coroutineContext: CoroutineContext = parent + job

    /** Parent of the worker coroutines, so that they can be stopped without cancelling layers on top. */
    private val workers = Job(job)
    private val workerScope = CoroutineScope(coroutineContext + workers)

    override val incoming: ReceiveChannel<In>
        field = Channel<In>(incomingCapacity)
    override val outgoing: SendChannel<Out>
        field = Channel<Out>(outgoingCapacity)

    private class Closing(val cause: Throwable?)

    private val closing = atomic<Closing?>(null)
    private val terminated = atomic(false)
    private val shutDown = atomic(false)
    private lateinit var sendJob: Job

    fun start() {
        job.invokeOnCompletion { cause ->
            val failure = cause?.connectionFailure()
            terminate(failure)
            onRelease(failure)
        }
        workerScope.launch {
            val failure = try {
                receiver(incoming)
                null
            } catch (e: Throwable) {
                // when cancelled, the connection is terminating already
                if (!isActive) return@launch
                e
            }
            terminate(failure)
        }
        sendJob = workerScope.launch {
            try {
                sender(outgoing)
            } catch (e: ClosedSendChannelException) {
                // the underlying transport is gone; the receiving side reports how the connection ended
                outgoing.close(e)
                return@launch
            } catch (e: Throwable) {
                if (isActive) terminate(e)
                return@launch
            }
            shutdown(closing.value?.cause)
        }
    }

    private suspend fun CoroutineScope.shutdown(cause: Throwable?) {
        if (!shutDown.compareAndSet(false, true)) return
        try {
            onClose(cause)
        } catch (e: Throwable) {
            if (isActive) terminate(e)
        }
    }

    override suspend fun close(cause: Throwable?) {
        if (closing.compareAndSet(null, Closing(cause))) outgoing.close()
        sendJob.join()
        terminate(closing.value?.cause)
    }

    private fun terminate(cause: Throwable?) {
        if (!terminated.compareAndSet(false, true)) return
        incoming.close(cause)
        outgoing.close(cause)
        workers.cancel()
        if (cause == null) job.complete()
        else job.cancel(CancellationException("Connection failed", cause))
    }
}
