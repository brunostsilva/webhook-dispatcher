package io.github.brunostsilva.webhooks.delivery

import java.security.SecureRandom
import java.util.Base64
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

/**
 * Signs webhooks as defined by the Standard Webhooks spec (https://www.standardwebhooks.com):
 * `webhook-signature: v1,<base64(HMAC-SHA256(secret, "<webhook-id>.<webhook-timestamp>.<body>"))>`.
 * Receivers recompute it with their copy of the secret to check that the request came from us and
 * wasn't altered, and reject old timestamps to stop replays.
 */
object WebhookSigner {

    private const val SECRET_PREFIX = "whsec_"
    private const val ALGORITHM = "HmacSHA256"
    private val random = SecureRandom()

    /** A new signing secret: `whsec_` and 32 random bytes in base64. */
    fun newSecret(): String {
        val bytes = ByteArray(32).also(random::nextBytes)
        return SECRET_PREFIX + Base64.getEncoder().encodeToString(bytes)
    }

    fun sign(secret: String, messageId: String, timestamp: Long, payload: String): String {
        val key = Base64.getDecoder().decode(secret.removePrefix(SECRET_PREFIX))
        val mac = Mac.getInstance(ALGORITHM).apply { init(SecretKeySpec(key, ALGORITHM)) }
        val signature = mac.doFinal("$messageId.$timestamp.$payload".toByteArray(Charsets.UTF_8))
        return "v1," + Base64.getEncoder().encodeToString(signature)
    }
}
