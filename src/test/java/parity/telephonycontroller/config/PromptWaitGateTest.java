package parity.telephonycontroller.config;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import parity.telephonycontroller.lifecycle.ConfigurationRefusedException;

/** MS-042: TC refuses to start on a parity-ms that waits for a prompt as long as TC waits for parity-ms, or longer. */
class PromptWaitGateTest {

    @Test
    void aPromptWaitNotShorterThanTcsTimeoutIsRefused() {
        Duration timeout = Duration.ofMillis(250);
        for (long wait : new long[] {250, 251, 2_000}) {
            assertThatThrownBy(() -> TelephonyWiring.checkPromptWait(wait, timeout)).as("%d ms", wait)
                    .isInstanceOf(ConfigurationRefusedException.class);
        }
        for (long wait : new long[] {0, 200, 249}) {
            assertThatCode(() -> TelephonyWiring.checkPromptWait(wait, timeout)).as("%d ms", wait).doesNotThrowAnyException();
        }
        // An older parity-ms (or one down at start) doesn't say: let through, checked again on its hello.
        assertThatCode(() -> TelephonyWiring.checkPromptWait(-1, timeout)).doesNotThrowAnyException();
    }
}
