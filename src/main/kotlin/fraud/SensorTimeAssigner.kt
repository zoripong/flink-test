package fraud

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time

class SensorTimeAssigner : BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(5)) {

    override fun extractTimestamp(sensorReading: SensorReading) = sensorReading.timestamp
}
