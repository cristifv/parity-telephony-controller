package parity.telephonycontroller.api;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import parity.telephonycontroller.session.TelephonyException;

/** PCI: digit reporting always ends on its own, after 1–300 s (the contract's bound). */
class RequestChecksTest {

    private static final String LEG = "0192f6a1-8000-7000-8000-000000000001";
    private static final String COLL = "0192f6a1-a000-7000-8000-000000000001";

    @Test
    void reportingOnOrMaskedNeedsADurationOfOneToThreeHundredSeconds() {
        for (String mode : new String[] {"ON", "MASKED"}) {
            assertThatThrownBy(() -> RequestChecks.digitReporting(LEG, mode, COLL, 0)).isInstanceOf(TelephonyException.class);
            assertThatThrownBy(() -> RequestChecks.digitReporting(LEG, mode, COLL, 301)).isInstanceOf(TelephonyException.class);
            assertThatCode(() -> RequestChecks.digitReporting(LEG, mode, COLL, 1)).doesNotThrowAnyException();
            assertThatCode(() -> RequestChecks.digitReporting(LEG, mode, COLL, 300)).doesNotThrowAnyException();
        }
        assertThatCode(() -> RequestChecks.digitReporting(LEG, "OFF", "", 0)).doesNotThrowAnyException();
    }
}
