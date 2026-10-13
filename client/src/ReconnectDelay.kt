package io.ktor.client.messaging

import kotlinx.atomicfu.atomic
import kotlinx.atomicfu.getAndUpdate
import kotlin.math.pow
import kotlin.random.Random
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * Paces the reconnect attempts of [withReconnect].
 *
 * [delay] is called before every connection attempt, and [reset] once a re-established connection
 * successfully sends or receives a message. Throwing from [delay] stops reconnecting; the channel is
 * then closed with the thrown exception as its cause.
 */
interface ReconnectDelay {
    suspend fun delay()
    suspend fun reset()

    companion object {
        /** Waits the same [interval] before every attempt. */
        fun fixed(interval: Duration): ReconnectDelay =
            FixedReconnectDelay(interval)

        /** Waits [initial], growing by [multiplier] on each consecutive attempt, up to [max]. */
        fun exponential(
            initial: Duration = 100.milliseconds,
            max: Duration = 30.seconds,
            multiplier: Double = 2.0,
            jitter: Double = 0.0,
        ): ReconnectDelay =
            ExponentialReconnectDelay(initial, max, multiplier, jitter)
    }
}

/**
 * Gives up after [maxAttempts] consecutive attempts without a successful [ReconnectDelay.reset],
 * by throwing [ReconnectAttemptsExceededException].
 */
fun ReconnectDelay.limitAttempts(maxAttempts: Int): ReconnectDelay {
    require(maxAttempts >= 0) { "maxAttempts must not be negative: $maxAttempts" }
    val base = this
    return object : ReconnectDelay {
        private val attempts = atomic(0)

        override suspend fun delay() {
            if (attempts.incrementAndGet() > maxAttempts)
                throw ReconnectAttemptsExceededException(maxAttempts)
            base.delay()
        }

        override suspend fun reset() {
            attempts.value = 0
            base.reset()
        }
    }
}

class ReconnectAttemptsExceededException(val maxAttempts: Int) :
    IllegalStateException("Gave up reconnecting after $maxAttempts attempts")

/** Waits the same [interval] before every reconnect attempt. */
class FixedReconnectDelay(val interval: Duration) : ReconnectDelay {
    init {
        require(!interval.isNegative()) { "interval must not be negative: $interval" }
    }

    override suspend fun delay() =
        kotlinx.coroutines.delay(interval)

    override suspend fun reset() {}
}

/**
 * Exponential back-off: the n-th consecutive attempt waits `initial * multiplier^n`, capped at [max].
 *
 * With a non-zero [jitter], each wait is randomly scaled within `±jitter` of its nominal value,
 * so that many clients dropped at once do not reconnect in lock-step.
 */
class ExponentialReconnectDelay(
    val initial: Duration = 100.milliseconds,
    val max: Duration = 30.seconds,
    val multiplier: Double = 2.0,
    val jitter: Double = 0.0,
    private val random: Random = Random.Default,
) : ReconnectDelay {
    init {
        require(initial.isPositive()) { "initial must be positive: $initial" }
        require(max >= initial) { "max must not be less than initial: $max < $initial" }
        require(multiplier >= 1.0) { "multiplier must be at least 1: $multiplier" }
        require(jitter in 0.0..1.0) { "jitter must be within 0..1: $jitter" }
    }

    private val attempt = atomic(0)

    /** Returns the wait before the next attempt and advances the attempt counter. */
    fun next(): Duration {
        // the exponent saturates long before the counter could overflow
        val n = attempt.getAndUpdate { if (it < MAX_EXPONENT) it + 1 else it }
        val nominal = initial.inWholeNanoseconds * multiplier.pow(n)
        val capped = if (nominal >= max.inWholeNanoseconds) max else initial * multiplier.pow(n)
        if (jitter == 0.0) return capped
        return capped * (1.0 + jitter * (random.nextDouble() * 2.0 - 1.0))
    }

    override suspend fun delay() =
        kotlinx.coroutines.delay(next())

    override suspend fun reset() {
        attempt.value = 0
    }

    private companion object {
        const val MAX_EXPONENT = 1024
    }
}
