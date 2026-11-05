package parity.telephonycontroller.obs;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.LoggingEvent;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.slf4j.event.KeyValuePair;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * log-record.v1 as TC writes it (parity-contracts CONVENTIONS "Logging"): the envelope, the event,
 * the xsid rule, masking, the error and its stack, and the line's size. scripts/ci.sh checks what
 * the tests capture with contract-check; these pin the formatter's own rules.
 */
class JsonLogFormatterTest {

    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final LoggerContext CONTEXT = context();
    private static final Logger OWN = CONTEXT.getLogger("parity.telephonycontroller.session.TelephonyNode");
    private static final Logger LIBRARY = CONTEXT.getLogger("org.springframework.boot.SpringApplication");
    private static final JsonLogFormatter FORMATTER = new JsonLogFormatter(
            new JsonLogFormatter.Identity("parity-telephony-controller", "0.1.0", "tc-1", "zone-1", "media-1"));
    private static final String V7 = "0192f6a1-7c3e-7b21-9a4d-5e6f70819203";
    private static final String LEG = "0192f6a1-8000-7000-8000-0000000000a1";

    private static LoggerContext context() {
        LoggerContext c = new LoggerContext();
        c.setMDCAdapter(new ch.qos.logback.classic.util.LogbackMDCAdapter());
        return c;
    }

    private static LoggingEvent event(Logger logger, Level level, String msg, Throwable t, Object... kv) {
        LoggingEvent e = new LoggingEvent(JsonLogFormatterTest.class.getName(), logger, level, msg, t, null);
        for (int i = 0; i < kv.length; i += 2) {
            e.addKeyValuePair(new KeyValuePair((String) kv[i], kv[i + 1]));
        }
        return e;
    }

    private static JsonNode line(LoggingEvent e) {
        String text = FORMATTER.format(e);
        assertThat(text).endsWith("\n").doesNotContain("\n{");
        assertThat(text.substring(0, text.length() - 1)).doesNotContain("\n");
        return JSON.readTree(text);
    }

    @Test
    void theEnvelopeTheEventAndTheCorrelationFields() {
        JsonNode l = line(event(OWN, Level.INFO, "a call arrived", null, "event", "session.arrived",
                "tenant_id", "acme", "interaction_id", V7, "leg_id", LEG, "bin", "bin-a", "owner_epoch", 7L,
                "callerNumber", "+447700900123", "endpoint", "SIP"));
        assertThat(l.get("ts").asString()).matches("^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z$");
        assertThat(l.get("level").asString()).isEqualTo("info");
        assertThat(l.get("service").asString()).isEqualTo("parity-telephony-controller");
        assertThat(l.get("version").asString()).isEqualTo("0.1.0");
        assertThat(l.get("instance").asString()).isEqualTo("tc-1");
        assertThat(l.get("incarnation").asString()).isEqualTo(LogProcess.incarnation());
        assertThat(l.get("seq").asLong()).isPositive();
        assertThat(l.get("event").asString()).isEqualTo("session.arrived");
        assertThat(l.get("msg").asString()).isEqualTo("a call arrived");
        assertThat(l.get("component").asString()).isEqualTo("parity.telephonycontroller.session.TelephonyNode");
        assertThat(l.get("node").asString()).isEqualTo("media-1");
        assertThat(l.get("zone").asString()).isEqualTo("zone-1");
        assertThat(l.get("tenant_id").asString()).isEqualTo("acme");
        assertThat(l.get("xsid").asString()).as("a v7 interaction id is the xsid").isEqualTo(V7);
        assertThat(l.has("interaction_id")).isFalse();
        assertThat(l.get("leg_id").asString()).isEqualTo(LEG);
        assertThat(l.get("bin").asString()).isEqualTo("bin-a");
        assertThat(l.get("owner_epoch").asLong()).isEqualTo(7);
        assertThat(l.get("attrs").get("caller_number").asString()).as("snake_case, masked by name").isEqualTo("***123");
        assertThat(l.get("attrs").get("endpoint").asString()).isEqualTo("SIP");
        assertThat(l.has("code")).as("no caller below warn").isFalse();
        assertThat(l.has("error")).isFalse();
    }

    @Test
    void theXsidIsOnlyALowercaseUuidV7() {
        JsonNode v4 = line(event(OWN, Level.INFO, "x", null, "event", "a.b", "interaction_id",
                "6f1c2a9e-0000-4000-8000-000000000001"));
        assertThat(v4.has("xsid")).isFalse();
        assertThat(v4.get("interaction_id").asString()).isEqualTo("6f1c2a9e-0000-4000-8000-000000000001");
        JsonNode upper = line(event(OWN, Level.INFO, "x", null, "event", "a.b", "interaction_id", V7.toUpperCase(java.util.Locale.ROOT)));
        assertThat(upper.has("xsid") || upper.has("interaction_id")).isFalse();
        assertThat(upper.get("attrs").get("interaction_id").asString()).isEqualTo(V7.toUpperCase(java.util.Locale.ROOT));
        JsonNode notUuid = line(event(OWN, Level.INFO, "x", null, "event", "a.b", "interaction_id", "i-1",
                "leg_id", "leg-1", "tenant_id", "Not A Tenant", "span_id", "00f067aa0ba902b7"));
        assertThat(notUuid.get("attrs").get("interaction_id").asString()).isEqualTo("i-1");
        assertThat(notUuid.get("attrs").get("leg_id").asString()).as("a field that doesn't fit is an attribute").isEqualTo("leg-1");
        assertThat(notUuid.get("attrs").get("tenant_id").asString()).isEqualTo("Not A Tenant");
        assertThat(notUuid.has("span_id")).as("span_id needs trace_id").isFalse();
        JsonNode parent = line(event(OWN, Level.INFO, "x", null, "event", "a.b", "interaction_id", V7,
                "parent_interaction_id", "0192f6a1-7c3e-7b21-9a4d-5e6f70819204"));
        assertThat(parent.get("parent_xsid").asString()).isEqualTo("0192f6a1-7c3e-7b21-9a4d-5e6f70819204");
    }

    @Test
    void eventsAndTheEnvelopeAreNeverTakenFromTheCaller() {
        JsonNode none = line(event(OWN, Level.WARN, "no event", null, "leg_id", LEG));
        assertThat(none.get("event").asString()).as("TC's own code without an event: CI fails it").isEqualTo("log.unclassified");
        JsonNode spring = line(event(CONTEXT.getLogger("parity.telephonycontroller.ParityTelephonyControllerApplication"),
                Level.INFO, "Starting ParityTelephonyControllerApplication v0.1.0 using Java 21", null));
        assertThat(spring.get("event").asString()).as("Spring Boot's start lines: no key-value pairs").isEqualTo("log.external");
        JsonNode bad = line(event(OWN, Level.INFO, "x", null, "event", "Not An Event"));
        assertThat(bad.get("event").asString()).isEqualTo("log.unclassified");
        JsonNode spoof = line(event(OWN, Level.ERROR, "x", null, "event", "a.b", "level", "spoofed", "service", "x",
                "seq", 1, "code", "busy", "cause", "IOException"));
        assertThat(spoof.get("level").asString()).isEqualTo("error");
        assertThat(spoof.get("service").asString()).isEqualTo("parity-telephony-controller");
        assertThat(spoof.get("attrs").get("level").asString()).isEqualTo("spoofed");
        assertThat(spoof.get("attrs").get("service").asString()).isEqualTo("x");
        assertThat(spoof.get("attrs").get("code").asString()).isEqualTo("busy");
        assertThat(spoof.get("attrs").get("cause").asString()).as("cause is <kind>:<ref>, never a class").isEqualTo("IOException");
    }

    @Test
    void aLibraryRecordIsExternalWithItsTextShortened() {
        String text = "Exception encountered during context initialization: " + "x".repeat(600) + " test only";
        JsonNode l = line(event(LIBRARY, Level.WARN, text, null));
        assertThat(l.get("event").asString()).isEqualTo("log.external");
        assertThat(l.get("component").asString()).isEqualTo("org.springframework.boot.SpringApplication");
        assertThat(l.get("msg").asString()).hasSize(JsonLogFormatter.MAX_MSG).endsWith("...");
        assertThat(l.get("attrs").get("text").asString()).as("the whole text, up to 2048").endsWith("test only");
    }

    @Test
    void personalDataIsMaskedInFieldsAndInText() {
        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("From", "<sip:+447700900123@carrier.example>");
        nested.put("note", "call sip:+447700900123@carrier.example or alice@example.com");
        JsonNode l = line(event(OWN, Level.WARN, "probe 447700900123 alice@example.com", null, "event", "a.b",
                "to", 447700900123L, "digits", "1234", "dtmf_digit", "5", "admin_email", "a@b.example",
                "remark", "+447700900123", "headers", nested, "sdp", "v=0", "num_digits", 4,
                "error_message", "bad input 447700900123"));
        JsonNode a = l.get("attrs");
        assertThat(a.get("to").asString()).isEqualTo("***123");
        assertThat(a.get("digits").asString()).isEqualTo("[redacted]");
        assertThat(a.get("dtmf_digit").asString()).isEqualTo("[redacted]");
        assertThat(a.get("sdp").asString()).isEqualTo("[redacted]");
        assertThat(a.get("admin_email").asString()).isEqualTo("[email]");
        assertThat(a.get("num_digits").asInt()).isEqualTo(4);
        assertThat(a.get("remark").asString()).as("free text under a plain name").isEqualTo("***123");
        assertThat(a.get("headers").get("From").asString()).isEqualTo("***123");
        assertThat(a.get("headers").get("note").asString()).isEqualTo("call sip:***@carrier.example or [email]");
        assertThat(l.get("msg").asString()).isEqualTo("probe ***123 [email]");
        assertThat(l.get("error").get("message").asString()).isEqualTo("bad input ***123");
        assertThat(l.toString()).doesNotContain("7700900", "alice", "a@b");
    }

    @Test
    void anErrorHasItsStackOnlyAtErrorInOneString() {
        IllegalStateException t = new IllegalStateException("outer", new java.io.IOException("inner 447700900123"));
        JsonNode warn = line(event(OWN, Level.WARN, "x", t, "event", "a.b"));
        assertThat(warn.get("error").get("kind").asString()).isEqualTo("java.lang.IllegalStateException");
        assertThat(warn.get("error").get("message").asString()).isEqualTo("outer");
        assertThat(warn.get("error").has("stack")).as("no stack below error").isFalse();

        JsonNode err = line(event(OWN, Level.ERROR, "x", t, "event", "a.b", "error_code", "unavailable"));
        assertThat(err.get("error").get("code").asString()).isEqualTo("unavailable");
        String stack = err.get("error").get("stack").asString();
        assertThat(stack).startsWith("at parity.telephonycontroller.obs.JsonLogFormatterTest")
                .contains("\ncaused by: java.io.IOException: inner ***123\nat ")
                .doesNotContain("7700900");
        assertThat(stack.lines().filter(s -> s.startsWith("at ")).count()).isLessThanOrEqualTo(60);
    }

    @Test
    void aDeepLongStackKeepsItsRootCauseWithinTheLimit() {
        Throwable t = deep(40, 12);
        JsonNode l = line(event(OWN, Level.ERROR, "x", t, "event", "a.b"));
        String stack = l.get("error").get("stack").asString();
        assertThat(stack.length()).isLessThanOrEqualTo(JsonLogFormatter.MAX_STACK);
        assertThat(stack).as("the root cause survives the shortening").contains("caused by: java.lang.IllegalArgumentException: root");
        for (String frames : stack.split("caused by: ")) {
            assertThat(frames.lines().filter(s -> s.startsWith("at ")).count()).isLessThanOrEqualTo(JsonLogFormatter.MAX_FRAMES);
        }
    }

    /** {@code causes} wrappers around a root cause, each thrown {@code depth} frames down. */
    private static Throwable deep(int depth, int causes) {
        if (depth > 0) {
            return deep(depth - 1, causes);
        }
        Throwable t = new IllegalArgumentException("root");
        for (int i = 0; i < causes; i++) {
            t = new RuntimeException("wrapper " + "y".repeat(300), t);
        }
        return t;
    }

    @Test
    void aLineOverTheLimitIsShortenedNeverCut() {
        Object[] kv = new Object[2 + 2 * 40];
        kv[0] = "event";
        kv[1] = "a.b";
        for (int i = 0; i < 40; i++) {
            kv[2 + 2 * i] = "field_" + i;
            kv[3 + 2 * i] = (i < 20 ? "é" : "x").repeat(1500);
        }
        String text = FORMATTER.format(event(OWN, Level.INFO, "big", null, kv));
        assertThat(text.getBytes(StandardCharsets.UTF_8).length - 1).isLessThanOrEqualTo(JsonLogFormatter.MAX_LINE);
        JsonNode l = JSON.readTree(text);
        assertThat(l.get("attrs").size()).as("at most 32 attrs").isEqualTo(JsonLogFormatter.MAX_ATTRS);
        assertThat(l.get("attrs").get("field_0").asString()).hasSize(256);

        Map<String, Object> huge = new LinkedHashMap<>();
        for (int i = 0; i < 400; i++) {
            huge.put("k" + i, "v".repeat(100));
        }
        Object[] many = new Object[2 + 2 * 32];
        many[0] = "event";
        many[1] = "a.b";
        for (int i = 0; i < 32; i++) {
            many[2 + 2 * i] = "map_" + i;
            many[3 + 2 * i] = List.of(huge);
        }
        String still = FORMATTER.format(event(OWN, Level.ERROR, "bigger", deep(5, 3), many));
        assertThat(still.getBytes(StandardCharsets.UTF_8).length - 1).isLessThanOrEqualTo(JsonLogFormatter.MAX_LINE);
        JsonNode s = JSON.readTree(still);
        assertThat(s.get("event").asString()).isEqualTo("a.b");
        assertThat(s.get("error").get("kind").asString()).isEqualTo("java.lang.RuntimeException");
    }

    @Test
    void theWarnCallerIsTheSourceFileFromTheRepositoryRoot() {
        LoggingEvent e = event(OWN, Level.WARN, "x", null, "event", "a.b");
        e.setCallerData(new StackTraceElement[] {
                new StackTraceElement("parity.telephonycontroller.node.RealNode", "ended", "RealNode.java", 697)});
        JsonNode code = line(e).get("code");
        assertThat(code.get("file").asString()).isEqualTo("src/main/java/parity/telephonycontroller/node/RealNode.java");
        assertThat(code.get("line").asInt()).isEqualTo(697);
        assertThat(code.get("func").asString()).isEqualTo("RealNode.ended");
    }

    @Test
    void theEngineTakesTheLogsIncarnationWhenItSortsAfterTheLastRun() {
        assertThat(LogProcess.engineIncarnation(null)).isEqualTo(LogProcess.incarnation());
        assertThat(LogProcess.engineIncarnation("0192f6a1-7c3e-7b21-9a4d-5e6f70819203")).isEqualTo(LogProcess.incarnation());
        String ahead = parity.telephonycontroller.model.UuidV7.create(System.currentTimeMillis() + 60_000, new java.util.Random(1));
        String next = LogProcess.engineIncarnation(ahead);
        assertThat(next).isNotEqualTo(LogProcess.incarnation()).isGreaterThan(ahead);
    }
}
