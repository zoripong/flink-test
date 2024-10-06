package fraud

import org.apache.flink.streaming.api.functions.source.FromIteratorFunction
import java.io.Serializable

class TransactionSource : FromIteratorFunction<Transaction>(
    RateLimitedIterator(TransactionIterator.unbounded()),
) {
    class RateLimitedIterator<T>(private val inner: Iterator<T>) :
        MutableIterator<T>, Serializable {
        override fun hasNext(): Boolean {
            return inner.hasNext()
        }

        override fun next(): T {
            try {
                Thread.sleep(100)
            } catch (e: InterruptedException) {
                throw RuntimeException(e)
            }
            return inner.next()
        }

        override fun remove() {
        }

        companion object {
            private const val serialVersionUID = 1L
        }
    }
}
