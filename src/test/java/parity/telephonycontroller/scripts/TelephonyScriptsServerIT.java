package parity.telephonycontroller.scripts;

import java.io.IOException;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

/**
 * Every {@code telephony.v1} script against TC's own server: real gRPC, etcd v3.7.2 in a container
 * through the JSON gateway, the file journal, the simulated node until steps 3 and 4.
 */
class TelephonyScriptsServerIT {

    private static ServerTarget target;

    @BeforeAll
    static void start() throws IOException {
        target = new ServerTarget();
    }

    @AfterAll
    static void stop() {
        if (target != null) {
            target.close();
        }
    }

    @TestFactory
    Stream<DynamicTest> scripts() throws IOException {
        return TelephonyScripts.against(target);
    }
}
