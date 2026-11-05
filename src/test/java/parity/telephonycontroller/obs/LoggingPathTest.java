package parity.telephonycontroller.obs;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * TC-S0-AC3 through the logging path: the application context, the async appender and the
 * log-record.v1 encoder (src/test/resources/logback-test.xml: production's appender and encoder;
 * logback-spring.xml itself runs in ProcessLifecycleIT), and stdout captured. The same shapes as
 * {@code ie-obs}'s {@code masking.rs}. These records (numbers and digits on purpose, no event) go
 * to stdout only, never into the capture CI checks.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ExtendWith(OutputCaptureExtension.class)
class LoggingPathTest {

    /** Pieces of +447700900123 that must never appear in a line. */
    private static final List<String> CANARIES = List.of("7700900123", "447700900", "0900123", "900123", "4111111111111111");
    private static final Logger log = LoggerFactory.getLogger(LoggingPathTest.class);
    private static final JsonMapper JSON = JsonMapper.builder().build();

    private static final Path dir = tempDir();

    private static Path tempDir() {
        try {
            return parity.telephonycontroller.realnode.TempDirs.deleteOnExit(
                    java.nio.file.Files.createTempDirectory("tc-logging"));
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry r) {
        r.add("parity.telephony-controller.lock-dir", () -> dir.resolve("lock").toString());
        r.add("parity.telephony-controller.lifecycle.healthy-file", () -> dir.resolve("healthy").toString());
        r.add("parity.telephony-controller.lifecycle.drained-file", () -> dir.resolve("drained").toString());
        r.add("parity.telephony-controller.readiness.endpoints.freeswitch-esl", () -> "");
        r.add("parity.telephony-controller.readiness.endpoints.parity-ms", () -> "");
        r.add("parity.telephony-controller.grpc.port", () -> "0");
        r.add("parity.telephony-controller.node.mode", () -> "simulated");
        r.add("parity.telephony-controller.node.media", () -> "simulated");
        r.add("parity.telephony-controller.journal.dir", () -> dir.resolve("journal").toString());
    }

    @Test
    void numbersDigitsAndEslNamesAreMaskedInEveryShape(CapturedOutput output) throws Exception {
        Phone phone = Phone.parseE164("+447700900123");
        Map<String, Object> esl = new TreeMap<>();
        esl.put("Caller-Caller-ID-Number", "+447700900123");
        esl.put("variable_sip_from_user", "447700900123");
        esl.put("Channel-Name", "sofia/internal/+447700900123@carrier.example");
        esl.put("DTMF-Digit", "5");
        esl.put("variable_dtmf_digits", "4111111111111111");
        esl.put("Unique-ID", "6f1c2a9e-0000-4000-8000-000000000001");
        Map<String, Object> nested = Map.of("headers", Map.of("From", "<sip:+447700900123@carrier.example>",
                "list", List.of(Map.of("to", 447700900123L))));

        log.atInfo().addKeyValue("to", "+447700900123").log("t1 a string field");
        log.atInfo().addKeyValue("to", 447700900123L).log("t2 an integer field");
        log.atInfo().addKeyValue("caller", phone).log("t3 a Phone as a field");
        log.info("t4 a Phone in the message: {}", phone);
        log.atWarn().addKeyValue("esl", esl).log("t5 an ESL event map");
        log.atWarn().addKeyValue("payload", nested).log("t6 a nested value");
        log.atInfo().addKeyValue("digits", "1234").addKeyValue("reply_to", "replies.parity-voice-api")
                .addKeyValue("num_digits", 4).log("t7 secrets and exempt names");
        log.atInfo().addKeyValue("dtmf_digit", "5").addKeyValue("digit", "7").log("t8 dtmf tokens");
        MDC.put("from", "+447700900123");
        MDC.put("tenant_id", "acme");
        try {
            log.atInfo().addKeyValue("Caller_ANI", "+447700900123").log("t9 MDC and an ESL-style field name");
        } finally {
            MDC.clear();
        }
        log.atError().addKeyValue("level", "spoofed").addKeyValue("service", "x").log("t10 envelope");

        Map<String, JsonNode> lines = awaitLines(output, 10);
        String all = String.join("\n", lines.values().stream().map(JsonNode::toString).toList());
        for (String canary : CANARIES) {
            assertThat(all).as("canary " + canary).doesNotContain(canary);
        }
        assertThat(attrs(lines, "t1").get("to").asString()).isEqualTo("***123");
        assertThat(attrs(lines, "t2").get("to").asString()).isEqualTo("***123");
        assertThat(attrs(lines, "t3").get("caller").asString()).isEqualTo("***123");
        assertThat(lines.get("t4").get("msg").asString()).isEqualTo("t4 a Phone in the message: ***123");
        JsonNode e = attrs(lines, "t5").get("esl");
        assertThat(e.get("Caller-Caller-ID-Number").asString()).isEqualTo("***123");
        assertThat(e.get("variable_sip_from_user").asString()).isEqualTo("***123");
        assertThat(e.get("Channel-Name").asString()).isEqualTo("***123");
        assertThat(e.get("DTMF-Digit").asString()).isEqualTo("[redacted]");
        assertThat(e.get("variable_dtmf_digits").asString()).isEqualTo("[redacted]");
        assertThat(e.get("Unique-ID").asString()).isEqualTo("6f1c2a9e-0000-4000-8000-000000000001");
        assertThat(attrs(lines, "t6").get("payload").get("headers").get("From").asString()).isEqualTo("***123");
        assertThat(attrs(lines, "t6").get("payload").get("headers").get("list").get(0).get("to").asString()).isEqualTo("***123");
        assertThat(attrs(lines, "t7").get("digits").asString()).isEqualTo("[redacted]");
        assertThat(attrs(lines, "t7").get("reply_to").asString()).isEqualTo("replies.parity-voice-api");
        assertThat(attrs(lines, "t7").get("num_digits").asInt()).isEqualTo(4);
        assertThat(attrs(lines, "t8").get("dtmf_digit").asString()).isEqualTo("[redacted]");
        assertThat(attrs(lines, "t8").get("digit").asString()).isEqualTo("[redacted]");
        assertThat(attrs(lines, "t9").get("from").asString()).isEqualTo("***123");
        assertThat(lines.get("t9").get("tenant_id").asString()).as("a correlation field, from the MDC").isEqualTo("acme");
        assertThat(attrs(lines, "t9").get("caller_ani").asString()).isEqualTo("***123");
        JsonNode env = lines.get("t10");
        assertThat(env.get("level").asString()).isEqualTo("error");
        assertThat(env.get("attrs").get("level").asString()).isEqualTo("spoofed");
        assertThat(env.get("service").asString()).isEqualTo("parity-telephony-controller");
        assertThat(env.get("attrs").get("service").asString()).isEqualTo("x");
        assertThat(env.get("ts").asString()).endsWith("Z");
        assertThat(env.get("event").asString()).as("no event named: unclassified").isEqualTo("log.unclassified");
        for (String key : List.of("component", "version", "instance", "incarnation", "seq", "msg", "thread", "node", "zone")) {
            assertThat(env.has(key)).as(key).isTrue();
        }
    }

    private static JsonNode attrs(Map<String, JsonNode> lines, String tag) {
        return lines.get(tag).get("attrs");
    }

    @Test
    void anUnlistedNameIsNotMaskedSoTheCanaryCheckWorks(CapturedOutput output) throws Exception {
        log.atInfo().addKeyValue("note", "447700900123").addKeyValue("remark", "+447700900123").log("u1 an unlisted field name");
        Map<String, JsonNode> lines = awaitLines(output, 1, "u");
        assertThat(attrs(lines, "u1").get("note").asString()).as("names outside the rules are written as is").contains("7700900123");
        assertThat(attrs(lines, "u1").get("remark").asString()).as("but an E.164 number in free text is not").isEqualTo("***123");
    }

    private static Map<String, JsonNode> awaitLines(CapturedOutput output, int expected) throws InterruptedException {
        return awaitLines(output, expected, "t");
    }

    /** The async appender writes on its own thread: wait for the lines, by their message's tag. */
    private static Map<String, JsonNode> awaitLines(CapturedOutput output, int expected, String prefix)
            throws InterruptedException {
        Instant until = Instant.now().plus(Duration.ofSeconds(5));
        Map<String, JsonNode> found = new TreeMap<>();
        while (Instant.now().isBefore(until)) {
            found.clear();
            for (String line : output.getOut().lines().toList()) {
                if (!line.startsWith("{")) {
                    continue;
                }
                JsonNode node = JSON.readTree(line);
                String message = node.path("msg").asString("");
                if (node.path("component").asString("").equals(LoggingPathTest.class.getName())
                        && message.matches("^" + prefix + "\\d+ .*")) {
                    found.put(message.substring(0, message.indexOf(' ')), node);
                }
            }
            if (found.size() >= expected) {
                return found;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("expected " + expected + " lines, found " + found.keySet());
    }
}
