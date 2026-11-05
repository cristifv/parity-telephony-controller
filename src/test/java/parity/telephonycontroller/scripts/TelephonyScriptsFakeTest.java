package parity.telephonycontroller.scripts;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import parity.telephonycontroller.config.TelephonyServerLifecycle;
import parity.telephonycontroller.config.TelephonyWiring;

/**
 * Every {@code telephony.v1} script against fake-telephony-controller: the application with the
 * {@code fake} profile, driven through its control endpoint as interaction-engine's tests will.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("fake")
class TelephonyScriptsFakeTest {

    private static final Path DIR = tempDir();
    private static FakeTarget target;

    @Autowired
    TelephonyServerLifecycle server;

    @Autowired
    TelephonyWiring.FakeControlLifecycle control;

    private static Path tempDir() {
        try {
            return parity.telephonycontroller.realnode.TempDirs.deleteOnExit(Files.createTempDirectory("tc-fake"));
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry r) {
        r.add("parity.telephony-controller.lock-dir", () -> DIR.resolve("lock").toString());
        r.add("parity.telephony-controller.lifecycle.healthy-file", () -> DIR.resolve("healthy").toString());
        r.add("parity.telephony-controller.lifecycle.drained-file", () -> DIR.resolve("drained").toString());
        r.add("parity.telephony-controller.grpc.port", () -> "0");
        r.add("parity.telephony-controller.fake.control-port", () -> "0");
    }

    @TestFactory
    Stream<DynamicTest> scripts() throws IOException {
        target = new FakeTarget(server.port(), control.port());
        return TelephonyScripts.against(target);
    }

    @AfterAll
    static void close() {
        if (target != null) {
            target.close();
        }
    }
}
