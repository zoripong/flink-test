package dedup

import dedup.mock.ViewEventIterator
import dedup.type.ViewEvent
import org.apache.flink.streaming.api.functions.source.FromIteratorFunction
import java.io.Serializable

class ViewEventSource : FromIteratorFunction<ViewEvent>(
    RateLimitedIterator(ViewEventIterator.bounded()),
) {
    class RateLimitedIterator(private val inner: ViewEventIterator) :
        MutableIterator<ViewEvent>, Serializable {
        override fun hasNext(): Boolean {
            return inner.hasNext()
        }

        override fun next(): ViewEvent {
            val now = System.currentTimeMillis()
            val viewEvent = inner.next()

            // 현재시각보다 이후에 발생한 이벤트만 읽어올 수 있도록 제한
            if (viewEvent.timestamp > now) {

                try {
                    Thread.sleep(viewEvent.timestamp - now)
                } catch (e: InterruptedException) {
                    throw RuntimeException(e)
                }
            }
            return viewEvent
        }

        override fun remove() {
        }
    }
}
