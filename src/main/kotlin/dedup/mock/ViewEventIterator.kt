package dedup.mock

import dedup.type.ViewEvent
import java.io.Serializable

class ViewEventIterator private constructor(private val bounded: Boolean) : MutableIterator<ViewEvent?>, Serializable {
    private var index = 0

    override fun hasNext(): Boolean {
        return if (index < data.size) {
            true
        } else if (!bounded) {
            index = 0
            true
        } else {
            false
        }
    }

    override fun next(): ViewEvent = data[index++]

    override fun remove() {
        TODO("Not yet implemented")
    }

    companion object {
        private fun generateRandomViewEvent(): List<ViewEvent> {
            val users = (1..2).map { "user$it" }

            val viewEvents = mutableListOf<ViewEvent>()

            users.forEach { userId ->
                (1..3).map { index ->
                    val contentId = "content${index % 2}"
                    val timestamp = System.currentTimeMillis()
                    viewEvents.add(ViewEvent(userId, contentId, timestamp))
                }
            }
            println("[ViewEventIterator::generateRandomViewEvent]")
            val sortedViewEvents = viewEvents.sortedBy { it.timestamp }

            return (sortedViewEvents + sortedViewEvents.map { it.afterMinutes(1) })
                .also {
                    val json =  "[" + it.map { it.toJson() }.joinToString(", ") + "]"
                    println(json)
                }
        }

        fun unbounded(): ViewEventIterator {
            return ViewEventIterator(false)
        }

        fun bounded(): ViewEventIterator {
            return ViewEventIterator(true)
        }

        private val data = generateRandomViewEvent()
    }
}
