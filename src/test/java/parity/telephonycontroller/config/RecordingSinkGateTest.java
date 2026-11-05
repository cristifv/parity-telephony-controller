package parity.telephonycontroller.config;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;
import parity.telephonycontroller.lifecycle.ConfigurationRefusedException;

/** The null recording sink starts only in local test setups (MS-012; the coordinator's gate). */
class RecordingSinkGateTest {

    private static MockEnvironment profiles(String... active) {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(active);
        return env;
    }

    @Test
    void theNullSinkIsRefusedOutsideLocalTestProfiles() {
        for (String[] p : new String[][] {{}, {"default"}, {"prod"}, {"staging"}, {"local", "prod"}, {"test", "staging"},
                {"fake", "production"}}) {
            assertThatThrownBy(() -> TelephonyWiring.checkRecordingSink(true, profiles(p)))
                    .as(String.join(",", p)).isInstanceOf(ConfigurationRefusedException.class);
        }
        for (String[] p : new String[][] {{"local"}, {"test"}, {"fake"}}) {
            assertThatCode(() -> TelephonyWiring.checkRecordingSink(true, profiles(p))).doesNotThrowAnyException();
        }
        // No sink: nothing to check, any profile.
        assertThatCode(() -> TelephonyWiring.checkRecordingSink(false, profiles("prod"))).doesNotThrowAnyException();
    }
}
