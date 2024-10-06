package fraud

import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

class FraudDetectionJob {
    fun run() {
        val env = StreamExecutionEnvironment.getExecutionEnvironment()

        val transactions: DataStream<Transaction> = env
            .addSource(TransactionSource())
            .name("transactions")

        val alerts: DataStream<Alert> = transactions
            .keyBy(Transaction::accountId)
            .process(FraudDetector())
            .name("fraud-detector")

        alerts
            .addSink(AlertSink())
            .name("send-alerts")

        env.execute("Fraud Detection")
    }
}
