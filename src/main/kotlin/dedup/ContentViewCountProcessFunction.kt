package dedup

import dedup.type.ViewEvent
import dedup.type.ViewEventDedupKey
import org.apache.flink.api.common.state.StateTtlConfig
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import java.time.Duration

//<K> – Type of the key. <I> – Type of the input elements. <O> – Type of the output elements.
// Key: User 식별자 (userId or sessionId) + Content 식별자 (contentId)
// Input: 전체 조회 이벤트 (ViewEvent)
// Output: 증가가 필요한 조회 이벤트(ViewEvent)
class ContentViewCountProcessFunction : KeyedProcessFunction<ViewEventDedupKey, ViewEvent, ViewEvent>() {

    // 마지막 조회 시간을 저장하는 상태
    @Transient
    private lateinit var lastViewTimeState: ValueState<Long>

    override fun open(parameters: Configuration) {
        // Set up the ValueState with a TTL
        val stateTtlConfig = StateTtlConfig
            .newBuilder(DEDUP_TIME)
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build()

        lastViewTimeState = runtimeContext.getState(
            ValueStateDescriptor("lastViewTime", Long::class.java).apply {
                this.enableTimeToLive(stateTtlConfig)
            }
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
        if (lastViewTime == null || currentTime - lastViewTime > DEDUP_TIME.toMillis()) {
            println("collect: ${event.toJson()}")

            // 조회수 증가 로직 (예: 외부 시스템에 조회수 업데이트)
            collector.collect(event)

            // 상태 업데이트: 마지막 조회 시간을 현재 시간으로 설정
            lastViewTimeState.update(currentTime)
        } else {
            println("skip: ${event.toJson()}")
        }
    }

    companion object {
//         private val DEDUP_TIME = Duration.ofHours(24) // 24h
        private val DEDUP_TIME = Duration.ofSeconds(30) // 30 seconds
    }
}
