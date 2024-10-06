package fraud

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.slf4j.LoggerFactory

class AlertSink : SinkFunction<Alert> {
    override fun invoke(value: Alert, context: SinkFunction.Context) {
        LOG.info(value.toString())
        println("[AlertSink::invoke] $value")
    }

    companion object {
        private const val serialVersionUID = 1L
        private val LOG = LoggerFactory.getLogger(AlertSink::class.java)
    }
}
