package dedup.type

import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter

data class ViewEvent(
    val userId: String,
    val contentId: String,
    val timestamp: Long,
) {
    fun toJson(): String = """{"userId": "$userId", "contentId": "$contentId", "timestamp": "${convertTimestampToFormattedString(timestamp)}"}""".trimIndent()

    fun convertTimestampToFormattedString(timestamp: Long): String {
        val zonedDateTime = Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault())
        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
        return zonedDateTime.format(formatter)
    }

    fun afterMinutes(minutes: Int): ViewEvent =
        this.copy(timestamp = timestamp + 60 * 1000 * minutes)
}
