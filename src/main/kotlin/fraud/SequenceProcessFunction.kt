package fraud

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import java.time.ZonedDateTime

class SequenceProcessFunction : ProcessFunction<Long, Transaction>() {
    override fun processElement(value: Long, ctx: Context, out: Collector<Transaction>) {
        val result = Transaction(
            accountId = value,
            timestamp = ZonedDateTime.now().toEpochSecond(),
            value = value.toDouble(),
        )
        println(result)
        out.collect(result)
    }
}
