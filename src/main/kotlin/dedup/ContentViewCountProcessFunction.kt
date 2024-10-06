package dedup

import dedup.type.ViewEvent
import dedup.type.ViewEventDedupKey
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

//<K> – Type of the key. <I> – Type of the input elements. <O> – Type of the output elements.
// Key: User 식별자 (userId or sessionId) + Content 식별자 (contentId)
// Input: 전체 조회 이벤트 (ViewEvent)
// Output: 증가가 필요한 조회 이벤트(ViewEvent)
class ContentViewCountProcessFunction : KeyedProcessFunction<ViewEventDedupKey, ViewEvent, ViewEvent>() {

    // 마지막 조회 시간을 저장하는 상태
    @Transient
    private lateinit var lastViewTimeState: ValueState<Long>

    override fun open(parameters: Configuration) {
        lastViewTimeState = runtimeContext.getState(
            ValueStateDescriptor("lastViewTime", Long::class.java)
        )
    }

    override fun processElement(
        event: ViewEvent,
        context: KeyedProcessFunction<ViewEventDedupKey, ViewEvent, ViewEvent>.Context,
        collector: Collector<ViewEvent>
    ) {
        val currentTime = event.timestamp
        val lastViewTime = lastViewTimeState.value()

        // 이전 조회 시간이 없거나, DEDUP_TIME 시간이 지난 경우 조회수 증가
        if (lastViewTime == null || currentTime - lastViewTime > DEDUP_TIME) {
            println("collect: ${event.toJson()}")

            // 조회수 증가 로직 (예: 외부 시스템에 조회수 업데이트)
            collector.collect(event)

            // 상태 업데이트: 마지막 조회 시간을 현재 시간으로 설정
            lastViewTimeState.update(currentTime)

            // DEDUP_TIME 시간 후에 상태를 제거하는 타이머 설정
            context.timerService().registerEventTimeTimer(currentTime + DEDUP_TIME)
        } else {
            println("skip: ${event.toJson()}")
        }
    }

    override fun onTimer(
        timestamp: Long,
        ctx: KeyedProcessFunction<ViewEventDedupKey, ViewEvent, ViewEvent>.OnTimerContext,
        out: Collector<ViewEvent>
    ) {
        // 타이머가 만료되면 상태를 제거하여 메모리 관리
        lastViewTimeState.clear()
    }

    companion object {
//        private val DEDUP_TIME = 24 * 60 * 60 * 1000 // 24h
        private val DEDUP_TIME = 30 * 1000 // 30 seconds
    }
}
