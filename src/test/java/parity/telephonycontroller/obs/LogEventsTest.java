package parity.telephonycontroller.obs;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Every log call in {@code src/main/java} names its event, at a level its catalogue entry allows
 * ({@code logging/events.parity-telephony-controller.v1.json}, or parity-contracts'
 * {@code logging/events.common.v1.json}), and every catalogued event is still written. The call
 * sites the tests never reach are checked here; what the tests capture, contract-check checks
 * (scripts/ci.sh).
 */
class LogEventsTest {

    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final Path CATALOGUE = Path.of("logging", "events.parity-telephony-controller.v1.json");
    /** SLF4J's fluent API, from the level to the {@code .log(}: the event is one of its pairs. */
    private static final Pattern CHAIN = Pattern.compile("\\.at(Trace|Debug|Info|Warn|Error)\\(\\)");
    private static final Pattern EVENT = Pattern.compile("addKeyValue\\(\"event\",\\s*\"([^\"]*)\"\\)");
    /** A line written directly ({@link JsonLogFormatter#direct}): its level, then its event. */
    private static final Pattern DIRECT = Pattern.compile("Level\\.(TRACE|DEBUG|INFO|WARN|ERROR),\\s*\"([a-z0-9_.]+)\"");
    /** The plain API can't carry an event. */
    private static final Pattern PLAIN = Pattern.compile("\\b(LOG|log)\\.(trace|debug|info|warn|error)\\(");

    private static Map<String, Set<String>> levels(Path file) throws IOException {
        Map<String, Set<String>> out = new TreeMap<>();
        JsonNode events = JSON.readTree(Files.readString(file)).get("events");
        for (Map.Entry<String, JsonNode> e : events.properties()) {
            Set<String> l = new TreeSet<>();
            e.getValue().get("levels").forEach(x -> l.add(x.asString()));
            out.put(e.getKey(), l);
        }
        return out;
    }

    @Test
    void everyLogCallNamesACataloguedEventAtAnAllowedLevel() throws IOException {
        Map<String, Set<String>> own = levels(CATALOGUE);
        Map<String, Set<String>> common = levels(Path.of(System.getProperty("parity.contracts.dir", "../parity-contracts"),
                "logging", "events.common.v1.json"));
        assertThat(own.keySet()).as("a service never redefines a common event").doesNotContainAnyElementsOf(common.keySet());

        List<String> problems = new ArrayList<>();
        Set<String> used = new TreeSet<>();
        try (Stream<Path> files = Files.walk(Path.of("src", "main", "java"))) {
            for (Path f : files.filter(p -> p.toString().endsWith(".java")).sorted().toList()) {
                String src = Files.readString(f);
                Matcher plain = PLAIN.matcher(src);
                while (plain.find()) {
                    problems.add(f + ":" + line(src, plain.start()) + ": a plain logger call (no event): use the fluent API");
                }
                Matcher m = CHAIN.matcher(src);
                while (m.find()) {
                    int end = src.indexOf(".log(", m.end());
                    Matcher e = EVENT.matcher(end < 0 ? "" : src.substring(m.end(), end));
                    String where = f + ":" + line(src, m.start());
                    if (!e.find()) {
                        problems.add(where + ": no addKeyValue(\"event\", \"<name>\") before .log(");
                        continue;
                    }
                    check(where, e.group(1), m.group(1).toLowerCase(Locale.ROOT), own, common, used, problems);
                }
                Matcher d = DIRECT.matcher(src);
                while (d.find()) {
                    check(f + ":" + line(src, d.start()), d.group(2), d.group(1).toLowerCase(Locale.ROOT), own, common,
                            used, problems);
                }
            }
        }
        assertThat(problems).isEmpty();
        Set<String> unused = new TreeSet<>(own.keySet());
        unused.removeAll(used);
        unused.remove(JsonLogFormatter.EXTERNAL);
        assertThat(unused).as("catalogued events no call site writes").isEmpty();
    }

    private static void check(String where, String event, String level, Map<String, Set<String>> own,
            Map<String, Set<String>> common, Set<String> used, List<String> problems) {
        Set<String> allowed = own.containsKey(event) ? own.get(event) : common.get(event);
        if (allowed == null) {
            problems.add(where + ": event " + event + " is in no catalogue");
        } else if (!allowed.contains(level)) {
            problems.add(where + ": " + event + " at " + level + "; its catalogue allows " + allowed);
        }
        used.add(event);
    }

    private static int line(String src, int at) {
        int n = 1;
        for (int i = 0; i < at; i++) {
            if (src.charAt(i) == '\n') {
                n++;
            }
        }
        return n;
    }
}
