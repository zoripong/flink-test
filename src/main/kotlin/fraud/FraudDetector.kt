package fraud

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

class FraudDetector : KeyedProcessFunction<Long, Transaction, Alert>() {
    override fun processElement(transaction: Transaction, ctx: Context, collector: Collector<Alert>) {
        val alert = Alert(transaction.accountId)
        collector.collect(alert)
        println("[fraud.FraudDetector::processElement] $transaction, $alert")
    }
}
