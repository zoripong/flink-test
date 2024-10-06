package dedup

import dedup.type.ViewEvent
import dedup.type.ViewEventDedupKey
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

class ContentViewIncreaseJob {
    fun run() {
        val env = StreamExecutionEnvironment.getExecutionEnvironment()

        val viewEvents: DataStream<ViewEvent> = env
            .addSource(ViewEventSource())
            .name("viewEvents")

        val filtertedViewEvents: DataStream<ViewEvent> = viewEvents
            .keyBy { ViewEventDedupKey(it.contentId, it.userId) }
            .process(ContentViewCountProcessFunction())
            .name("ContentViewCountProcessFunction")

        filtertedViewEvents
            .addSink(ViewEventSink())
            .name("sinkViewEvents")

        env.execute("ContentViewIncreaseJob")
    }
}
