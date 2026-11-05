package parity.telephonycontroller.scripts;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;

/** parity-contracts' {@code fixtures/telephony.v1} scripts, as dynamic tests against a target. */
final class TelephonyScripts {

    private TelephonyScripts() {}

    static Path dir() {
        return Path.of(System.getProperty("parity.contracts.dir", "../parity-contracts"), "fixtures", "telephony.v1");
    }

    static List<Path> all() throws IOException {
        try (Stream<Path> files = Files.walk(dir())) {
            return files.filter(p -> p.toString().endsWith(".json") && !p.getFileName().toString().equals("deferred.json"))
                    .sorted().toList();
        }
    }

    /** parity-contracts 0.2.2's telephony.v1 scripts: 44 valid, 6 writer-invalid. Exact, so none goes missing. */
    static final int EXPECTED = 64;

    static Stream<DynamicTest> against(ScriptTarget target) throws IOException {
        List<Path> scripts = all();
        if (scripts.size() != EXPECTED) {
            throw new IllegalStateException("expected " + EXPECTED + " telephony.v1 scripts, found " + scripts.size()
                    + " in " + dir());
        }
        // -Dtc.script=<name fragment>: only the matching scripts (debugging one against a target).
        String only = System.getProperty("tc.script", "");
        return scripts.stream().filter(p -> p.toString().contains(only))
                .map(p -> DynamicTest.dynamicTest(target.name() + ": " + dir().relativize(p),
                () -> new ScriptRunner(target).run(p)));
    }
}
