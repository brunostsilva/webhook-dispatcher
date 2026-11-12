package io.github.brunostsilva.webhooks.delivery

import java.util.Base64
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class WebhookSignerTest {

    @Test
    fun `matches the Standard Webhooks reference signature`() {
        // The example from the Standard Webhooks spec.
        val signature = WebhookSigner.sign(
            secret = "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw",
            messageId = "msg_p5jXN8AQM9LWM0D4loKWxJek",
            timestamp = 1614265330,
            payload = """{"test": 2432232314}""",
        )

        assertThat(signature).isEqualTo("v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE=")
    }

    @Test
    fun `any change to the id, the timestamp or the payload changes the signature`() {
        val secret = WebhookSigner.newSecret()
        val original = WebhookSigner.sign(secret, "evt_1", 1_700_000_000, """{"amount":100}""")

        assertThat(WebhookSigner.sign(secret, "evt_2", 1_700_000_000, """{"amount":100}""")).isNotEqualTo(original)
        assertThat(WebhookSigner.sign(secret, "evt_1", 1_700_000_001, """{"amount":100}""")).isNotEqualTo(original)
        assertThat(WebhookSigner.sign(secret, "evt_1", 1_700_000_000, """{"amount":101}""")).isNotEqualTo(original)
        assertThat(WebhookSigner.sign(WebhookSigner.newSecret(), "evt_1", 1_700_000_000, """{"amount":100}"""))
            .isNotEqualTo(original)
    }

    @Test
    fun `new secrets are 32 random bytes with the whsec_ prefix`() {
        val secret = WebhookSigner.newSecret()

        assertThat(secret).startsWith("whsec_")
        assertThat(Base64.getDecoder().decode(secret.removePrefix("whsec_"))).hasSize(32)
        assertThat(WebhookSigner.newSecret()).isNotEqualTo(secret)
    }
}
