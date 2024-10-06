package fraud

data class Transaction(
    val accountId: Long,
    val timestamp: Long,
    val value: Double,
)
