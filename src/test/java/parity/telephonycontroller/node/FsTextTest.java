package parity.telephonycontroller.node;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** The agent leg's X-Parity-* headers in the dial string (TC-031) and the encoders that guard it. */
class FsTextTest {

    private static final String INTERACTION = "0190f3a2-7c41-7d2e-9b3a-5e6f7a8b9c0d";
    private static final String LEG = "0190f3a2-7c42-7a10-8c3b-1d2e3f4a5b6c";

    @Test
    void agentDialStringCarriesBothHeadersWithTcsValues() {
        String dial = new FsText.DialString().var("origination_uuid", LEG).parityHeaders(INTERACTION, LEG)
                .build("sofia/deskphones/sip:agent1@acme.agents.example");
        assertEquals("{origination_uuid=" + LEG + ",sip_h_X-Parity-Interaction-Id=" + INTERACTION
                + ",sip_h_X-Parity-Leg-Id=" + LEG + "}sofia/deskphones/sip:agent1@acme.agents.example", dial);
    }

    /** Anything but a lowercase UUID is refused: expansion, a split, an injected header, case, empty. */
    @ParameterizedTest
    @ValueSource(strings = {"${uuid}", "$(echo x)", "0190f3a2-7c41-7d2e-9b3a-5e6f7a8b9c0d,sip_h_X-Evil=1",
            "0190f3a2-7c41-7d2e-9b3a-5e6f7a8b9c0d\r\nX-Evil: 1", "{a=b}", "0190F3A2-7C41-7D2E-9B3A-5E6F7A8B9C0D", "",
            "not-a-uuid"})
    void unsafeValuesAreRefused(String bad) {
        assertThrows(FsText.Refused.class, () -> new FsText.DialString().parityHeaders(bad, LEG));
        assertThrows(FsText.Refused.class, () -> new FsText.DialString().parityHeaders(INTERACTION, bad));
    }

    @Test
    void nullValuesAreRefused() {
        assertThrows(FsText.Refused.class, () -> new FsText.DialString().parityHeaders(null, LEG));
        assertThrows(FsText.Refused.class, () -> new FsText.DialString().parityHeaders(INTERACTION, null));
    }

    /** The general encoder the rest of the dial string uses refuses each character freeswitch acts on. */
    @Test
    void varRefusesExpansionAndSplitCharacters() {
        for (String c : List.of("$", "(", "{", "}", "[", "]", "'", "^", ",", "\r", "\n", "\0")) {
            assertThrows(FsText.Refused.class, () -> new FsText.DialString().var("cc_x", "a" + c + "b"), c);
        }
    }
}
