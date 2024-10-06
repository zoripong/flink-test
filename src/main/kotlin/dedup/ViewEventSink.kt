package dedup

import dedup.type.ViewEvent
import org.apache.flink.streaming.api.functions.sink.SinkFunction

class ViewEventSink : SinkFunction<ViewEvent> {
    override fun invoke(value: ViewEvent, context: SinkFunction.Context) {
        println("[ViewEventSink::invoke] ${value.toJson()}")
    }
}
