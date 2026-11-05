package parity.telephonycontroller.scripts;

import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

/**
 * Every {@code telephony.v1} script against TC with real freeswitch (TC ADR 0002 §10): ESL, the
 * dialplan, the loopback carrier, (c+), the safety hangup, tokens and hooks, with a SIP agent at the
 * far ends and media simulated.
 */
class TelephonyScriptsRealNodeIT {

    private static RealNodeTarget target;

    @BeforeAll
    static void start() throws Exception {
        target = new RealNodeTarget();
    }

    @AfterAll
    static void stop() {
        if (target != null) {
            target.close();
        }
    }

    @TestFactory
    Stream<DynamicTest> scripts() throws Exception {
        return TelephonyScripts.against(target);
    }
}
