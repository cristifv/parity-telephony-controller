package parity.telephonycontroller.obs;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.classic.spi.StackTraceElementProxy;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.slf4j.event.KeyValuePair;
import org.springframework.core.env.Environment;

/**
 * TC's log lines in parity-contracts' {@code log-record.v1} (CONVENTIONS "Logging"): one JSON
 * object per line, at most {@value #MAX_LINE} bytes.
 * <ul>
 *   <li>The envelope: {@code ts}, lowercase {@code level}, {@code service}, {@code version},
 *       {@code instance}, {@code incarnation} and {@code seq} ({@link LogProcess}), {@code event},
 *       {@code msg}, {@code thread}, {@code component} (the logger), and {@code code} at
 *       {@code warn} and above when the appender took the caller ({@link CountingAsyncAppender}).
 *       {@code node} and {@code zone}: the controller's own, unless the record names others.</li>
 *   <li>{@code event}: the call site's {@code addKeyValue("event", "<noun>.<past_verb>")}, a name
 *       from {@code logging/events.parity-telephony-controller.v1.json} or the common catalogue.
 *       Without one, a record of TC's own code (a TC logger, with key-value pairs) is
 *       {@code log.unclassified} (in no catalogue: CI's contract-check fails it); anything else (a
 *       library, or Spring Boot's start lines under the application class's logger) is
 *       {@code log.external}.</li>
 *   <li>Key-value pairs and MDC entries named like a log-record.v1 correlation or flow field
 *       ({@code tenant_id}, {@code leg_id}, {@code bin}, {@code owner_epoch}, {@code op},
 *       {@code outcome}, ...) go to the top level when their value fits the field; everything else
 *       goes under {@code attrs} (snake_case names, at most {@value #MAX_ATTRS}). An
 *       {@code interaction_id} that is a lowercase UUIDv7 is written as {@code xsid} (CONVENTIONS
 *       "The xsid"); another UUID stays {@code interaction_id}; anything else is an attribute.</li>
 *   <li>{@code error}: {@code error_kind}, {@code error_code} and {@code error_message} pairs, or the
 *       record's throwable ({@code kind}, {@code message}, and the {@code stack} at {@code error}
 *       only: one string, {@value #MAX_FRAMES} frames per throwable at most, fewer when the causes
 *       wouldn't fit in {@value #MAX_STACK} characters).</li>
 *   <li>Masking: every value by its own name ({@link Pii#maskField}), every plain string by
 *       {@link Pii#maskText}, the message and the error's text by {@link Pii#maskProse}; a
 *       protobuf message never prints ({@link ProtoLogGuard}).</li>
 *   <li>A line over {@value #MAX_LINE} bytes is shortened, never cut: {@code attrs} strings first,
 *       then the stack, then {@code attrs} itself ({@code attrs.truncated}).</li>
 * </ul>
 */
public final class JsonLogFormatter {

    /** log-record.v1's line limit, bytes of UTF-8 without the newline. */
    public static final int MAX_LINE = 16_000;
    static final int MAX_MSG = 256;
    static final int MAX_TEXT = 2048;
    static final int MAX_ATTRS = 32;
    static final int MAX_STACK = 8192;
    static final int MAX_ERROR_MESSAGE = 1024;
    static final int MAX_FRAMES = 30;
    static final String UNCLASSIFIED = "log.unclassified";
    static final String EXTERNAL = "log.external";
    private static final String OWN = "parity.telephonycontroller.";
    private static final String TRUNCATED = "...[truncated]";

    private static final DateTimeFormatter TS =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);
    private static final Pattern EVENT = Pattern.compile("^[a-z][a-z0-9_]*(\\.[a-z][a-z0-9_]*)+$");
    private static final Pattern UUID = Pattern.compile("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$");
    private static final Pattern UUID7 = Pattern.compile("^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$");
    private static final Pattern TENANT = Pattern.compile("^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$");
    private static final Pattern OPAQUE = Pattern.compile("^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$");
    private static final Pattern COMMAND = Pattern.compile("^[A-Za-z0-9._:-]{1,200}$");
    private static final Pattern PLACE = Pattern.compile("^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$");
    private static final Pattern QUEUE = Pattern.compile("^[A-Za-z0-9_.-]{1,64}$");
    private static final Pattern STATE = Pattern.compile("^[A-Za-z][A-Za-z0-9_]{0,63}$");
    private static final Pattern OP = Pattern.compile("^[A-Za-z0-9_./:{}* -]{1,128}$");
    private static final Pattern CAUSE = Pattern.compile(
            "^(event|rpc|cmd|timer|sip|http|kafka|ws|signal|operator|config|startup|internal):[A-Za-z0-9._:/@{}* -]{1,190}$");
    private static final Pattern TRACE = Pattern.compile("^(?!0{32}$)[0-9a-f]{32}$");
    private static final Pattern SPAN = Pattern.compile("^(?!0{16}$)[0-9a-f]{16}$");
    private static final Pattern KIND = Pattern.compile("^[A-Za-z_][A-Za-z0-9_.$:<>]{0,127}$");
    private static final Pattern CODE = Pattern.compile("^[a-z][a-z0-9_]{0,63}$");
    private static final Pattern VERSION = Pattern.compile("^[0-9A-Za-z][0-9A-Za-z.+_-]{0,63}$");
    private static final Pattern SERVICE = Pattern.compile("^[a-z][a-z0-9-]{1,62}$");
    private static final Set<String> OUTCOMES = Set.of("ok", "refused", "error", "timeout", "retry");
    /** The correlation fields, in the order they are written. */
    private static final List<String> TOP = List.of("trace_id", "span_id", "tenant_id", "xsid", "parent_xsid",
            "interaction_id", "call_id", "session_id", "leg_id", "agent_id", "queue_id", "entry_id",
            "reservation_id", "request_id", "command_id", "event_id", "bin", "zone", "cell", "node", "leader_epoch",
            "owner_epoch", "op", "from_state", "to_state", "cause", "duration_ms", "attempt", "outcome");

    /** Who is logging, on every line. */
    public record Identity(String service, String version, String instance, String zone, String node) {

        public Identity {
            service = service != null && SERVICE.matcher(service).matches() ? service : "parity-telephony-controller";
            version = version != null && VERSION.matcher(version).matches() ? version : "unknown";
            instance = instance != null && OPAQUE.matcher(instance).matches() ? instance : "unknown";
            zone = zone != null && PLACE.matcher(zone).matches() ? zone : null;
            node = node != null && OPAQUE.matcher(node).matches() ? node : null;
        }

        /** From the application's settings (application.yml). */
        public static Identity of(Environment env) {
            return new Identity(env.getProperty("spring.application.name"),
                    env.getProperty("parity.telephony-controller.version"),
                    env.getProperty("parity.telephony-controller.instance"),
                    env.getProperty("parity.telephony-controller.zone"),
                    env.getProperty("parity.telephony-controller.node-id"));
        }

        /** Before (or without) the application: system properties, the environment, the jar's version. */
        public static Identity fromSystem() {
            String version = System.getProperty("parity.telephony-controller.version",
                    JsonLogFormatter.class.getPackage().getImplementationVersion());
            return new Identity("parity-telephony-controller", version,
                    System.getProperty("parity.telephony-controller.instance", System.getenv("HOSTNAME")),
                    System.getProperty("parity.telephony-controller.zone", System.getenv("ZONE")),
                    System.getProperty("parity.telephony-controller.node-id", System.getenv("MEDIA_NODE_ID")));
        }
    }

    private final Identity id;

    public JsonLogFormatter(Identity id) {
        this.id = id;
    }

    /**
     * A line written without logback (a signal handler's, when the logging system may be down or
     * about to stop): the same format, the same {@code seq}, on stdout at once.
     */
    public static void direct(Class<?> source, Level level, String event, String msg, Map<String, ?> fields) {
        LoggingEvent e = new LoggingEvent();
        e.setLevel(level);
        e.setLoggerName(source.getName());
        e.setMessage(msg);
        e.setThreadName(Thread.currentThread().getName());
        e.setInstant(Instant.now());
        e.setMDCPropertyMap(Map.of());
        e.setSequenceNumber(LogProcess.nextSeq());
        e.addKeyValuePair(new KeyValuePair("event", event));
        fields.forEach((k, v) -> e.addKeyValuePair(new KeyValuePair(k, v)));
        String line = new JsonLogFormatter(LogProcess.identity()).format(e);
        System.out.print(line);
        System.out.flush();
    }

    /** One record as one line, with its newline. */
    public String format(ILoggingEvent e) {
        Fields r = new Fields();
        Map<String, String> mdc = e.getMDCPropertyMap();
        if (mdc != null) {
            mdc.forEach(r::put);
        }
        List<KeyValuePair> pairs = e.getKeyValuePairs();
        if (pairs != null) {
            for (KeyValuePair p : pairs) {
                r.put(p.key, p.value);
            }
        }
        r.settle();
        String logger = e.getLoggerName() == null ? "" : e.getLoggerName();
        // TC's own calls use the fluent API with key-value pairs; a TC logger's record without any
        // is Spring Boot's (its start lines go through the application class's logger).
        boolean own = logger.startsWith(OWN) && pairs != null && !pairs.isEmpty();
        Level level = e.getLevel();
        String event = r.event != null ? r.event : own ? UNCLASSIFIED : EXTERNAL;
        String text = Pii.maskProse(message(e));
        String msg = text == null || text.isEmpty() ? event : cut(text, MAX_MSG);
        if (text != null && text.length() > MAX_MSG && !r.attrs.containsKey("text")) {
            r.attrs.put("text", cut(text, MAX_TEXT));
        }
        IThrowableProxy t = e.getThrowableProxy();
        if (t != null) {
            r.error.putIfAbsent("kind", kind(t.getClassName()));
            if (t.getMessage() != null) {
                r.error.putIfAbsent("message", cut(Pii.maskProse(t.getMessage()), MAX_ERROR_MESSAGE));
            }
            if (level.isGreaterOrEqual(Level.ERROR)) {
                r.error.put("stack", stack(t));
            }
        }
        r.error.values().removeIf(java.util.Objects::isNull);
        if (id.node() != null) {
            r.top.putIfAbsent("node", id.node());
        }
        if (id.zone() != null) {
            r.top.putIfAbsent("zone", id.zone());
        }

        Map<String, Object> line = new LinkedHashMap<>();
        line.put("ts", TS.format(e.getInstant() != null ? e.getInstant() : Instant.ofEpochMilli(e.getTimeStamp())));
        line.put("level", level.toString().toLowerCase(Locale.ROOT));
        line.put("service", id.service());
        line.put("version", id.version());
        line.put("instance", id.instance());
        line.put("incarnation", LogProcess.incarnation());
        line.put("seq", LogProcess.seq(e));
        line.put("event", event);
        line.put("msg", msg);
        String thread = e.getThreadName();
        if (thread != null && !thread.isEmpty()) {
            line.put("thread", cut(thread, 64));
        }
        line.put("component", component(logger));
        if (level.isGreaterOrEqual(Level.WARN) && e.hasCallerData()) {
            Map<String, Object> code = code(e.getCallerData());
            if (code != null) {
                line.put("code", code);
            }
        }
        for (String k : TOP) {
            Object v = r.top.get(k);
            if (v != null) {
                line.put(k, v);
            }
        }
        if (!r.error.isEmpty()) {
            line.put("error", r.error);
        }
        if (!r.attrs.isEmpty()) {
            line.put("attrs", r.attrs);
        }
        return fit(line) + "\n";
    }

    /** The record's text: TC's are constants; a protobuf argument is replaced by its masked fields (R1-5). */
    private static String message(ILoggingEvent e) {
        Object[] args = e.getArgumentArray();
        if (args == null || Arrays.stream(args).noneMatch(a -> a instanceof com.google.protobuf.MessageOrBuilder)) {
            return e.getFormattedMessage();
        }
        Object[] safe = Arrays.stream(args).map(ProtoLogGuard::safe).toArray();
        return org.slf4j.helpers.MessageFormatter.arrayFormat(e.getMessage(), safe).getMessage();
    }

    // ---- fields ----------------------------------------------------------------------------------

    /** One record's fields while it is being sorted into the top level, error and attrs. */
    private static final class Fields {
        String event;
        final Map<String, Object> top = new LinkedHashMap<>();
        final Map<String, Object> error = new LinkedHashMap<>();
        final Map<String, Object> attrs = new LinkedHashMap<>();

        void put(String key, Object raw) {
            if (key == null || raw == null) {
                return;
            }
            Object v = ProtoLogGuard.safe(raw);
            String s = v instanceof String || v instanceof java.util.UUID || v instanceof Enum<?> ? v.toString() : null;
            switch (key) {
                case "event" -> {
                    if (s != null && s.length() <= 64 && EVENT.matcher(s).matches()) {
                        event = s;
                    } else {
                        attr("event_name", v);
                    }
                    return;
                }
                case "error_kind" -> {
                    if (s != null && KIND.matcher(s).matches()) {
                        error.put("kind", s);
                        return;
                    }
                }
                case "error_code" -> {
                    if (s != null && CODE.matcher(s).matches()) {
                        error.put("code", s);
                        return;
                    }
                }
                case "error_message" -> {
                    if (s != null) {
                        error.put("message", cut(Pii.maskProse(s), MAX_ERROR_MESSAGE));
                        return;
                    }
                }
                case "interaction_id", "xsid" -> {
                    if (s != null && UUID7.matcher(s).matches()) {
                        top.put("xsid", s);
                        top.remove("interaction_id");
                        return;
                    }
                    if (s != null && UUID.matcher(s).matches() && !top.containsKey("xsid")) {
                        top.put("interaction_id", s);
                        return;
                    }
                    attr("interaction_id", v);
                    return;
                }
                case "parent_interaction_id", "parent_xsid" -> {
                    if (s != null && UUID7.matcher(s).matches()) {
                        top.put("parent_xsid", s);
                        return;
                    }
                }
                default -> {
                    if (fits(key, v, s)) {
                        top.put(key, v instanceof Number ? v : s);
                        return;
                    }
                }
            }
            attr(key, v);
        }

        /** The schema's pairs that need each other: a field without its partner is an attribute. */
        void settle() {
            if (top.containsKey("span_id") && !top.containsKey("trace_id")) {
                attr("span_id", top.remove("span_id"));
            }
            if (top.containsKey("from_state") && !top.containsKey("to_state")) {
                attr("from_state", top.remove("from_state"));
            }
            Object parent = top.get("parent_xsid");
            if (parent != null && (!top.containsKey("xsid") || parent.equals(top.get("xsid")))) {
                attr("parent_xsid", top.remove("parent_xsid"));
            }
        }

        void attr(String key, Object value) {
            String k = attrName(key);
            if (attrs.size() < MAX_ATTRS || attrs.containsKey(k)) {
                attrs.put(k, clean(Pii.maskField(key, value)));
            }
        }
    }

    /** A value that fits its log-record.v1 field (a correlation or flow field), by name. */
    private static boolean fits(String key, Object v, String s) {
        return switch (key) {
            case "tenant_id" -> s != null && TENANT.matcher(s).matches();
            case "call_id", "leg_id", "entry_id", "reservation_id", "event_id" -> s != null && UUID.matcher(s).matches();
            case "session_id", "agent_id", "node" -> s != null && OPAQUE.matcher(s).matches();
            case "request_id", "command_id" -> s != null && COMMAND.matcher(s).matches();
            case "queue_id" -> s != null && QUEUE.matcher(s).matches();
            case "bin", "zone", "cell" -> s != null && PLACE.matcher(s).matches();
            case "leader_epoch", "owner_epoch" -> integral(v) && ((Number) v).longValue() >= 0
                    && ((Number) v).longValue() <= 9_007_199_254_740_991L;
            case "op" -> s != null && OP.matcher(s).matches();
            case "from_state", "to_state" -> s != null && STATE.matcher(s).matches();
            case "cause" -> s != null && CAUSE.matcher(s).matches();
            case "trace_id" -> s != null && TRACE.matcher(s).matches();
            case "span_id" -> s != null && SPAN.matcher(s).matches();
            case "duration_ms" -> v instanceof Number n && Double.isFinite(n.doubleValue()) && n.doubleValue() >= 0;
            case "attempt" -> integral(v) && ((Number) v).longValue() >= 1 && ((Number) v).longValue() <= 1000;
            case "outcome" -> s != null && OUTCOMES.contains(s);
            default -> false;
        };
    }

    private static boolean integral(Object v) {
        return v instanceof Long || v instanceof Integer || v instanceof Short || v instanceof Byte;
    }

    /** An attrs name: snake_case ({@link Pii#normalize}), other characters as {@code _}, at most 64. */
    static String attrName(String key) {
        String n = Pii.normalize(key);
        StringBuilder b = new StringBuilder(n.length());
        for (int i = 0; i < n.length(); i++) {
            char c = n.charAt(i);
            b.append((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' ? c : '_');
        }
        if (b.isEmpty() || b.charAt(0) < 'a' || b.charAt(0) > 'z') {
            b.insert(0, "f_");
        }
        String out = b.toString();
        return out.length() > 64 ? out.substring(0, 64) : out;
    }

    /** Every string at any depth through {@link Pii#maskText}; numbers stay numbers. */
    private static Object clean(Object v) {
        if (v instanceof String s) {
            return Pii.maskText(s);
        }
        if (v instanceof Map<?, ?> map) {
            Map<String, Object> out = new LinkedHashMap<>();
            map.forEach((k, x) -> out.put(String.valueOf(k), clean(x)));
            return out;
        }
        if (v instanceof Collection<?> items) {
            List<Object> out = new ArrayList<>(items.size());
            items.forEach(x -> out.add(clean(x)));
            return out;
        }
        if (v == null || v instanceof Boolean || v instanceof Number) {
            return v;
        }
        return Pii.maskText(String.valueOf(v));
    }

    // ---- envelope parts --------------------------------------------------------------------------

    private static String component(String logger) {
        if (logger.isEmpty()) {
            return "unknown";
        }
        StringBuilder b = new StringBuilder(Math.min(logger.length(), 128));
        for (int i = 0; i < logger.length() && i < 128; i++) {
            char c = logger.charAt(i);
            boolean ok = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_'
                    || (i > 0 && ((c >= '0' && c <= '9') || c == '.' || c == ':' || c == '/' || c == '$' || c == '-'));
            b.append(ok ? c : '_');
        }
        return b.toString();
    }

    /** Where the record was made, for TC's own frames: the source file from the repository root. */
    private static Map<String, Object> code(StackTraceElement[] frames) {
        if (frames == null || frames.length == 0) {
            return null;
        }
        StackTraceElement f = frames[0];
        String cls = f.getClassName();
        if (!cls.startsWith(OWN)) {
            return null;
        }
        Map<String, Object> code = new LinkedHashMap<>();
        int dot = cls.lastIndexOf('.');
        if (f.getFileName() != null) {
            code.put("file", "src/main/java/" + cls.substring(0, dot).replace('.', '/') + "/" + f.getFileName());
        }
        if (f.getLineNumber() >= 1) {
            code.put("line", f.getLineNumber());
        }
        code.put("func", cut(cls.substring(dot + 1) + "." + f.getMethodName(), 256));
        return code;
    }

    private static String kind(String className) {
        return className != null && KIND.matcher(className).matches() ? className : "Throwable";
    }

    /**
     * The stack in one string: each throwable's frames ({@value #MAX_FRAMES} at most), its causes as
     * {@code caused by: <kind>: <message>} lines. When the causes don't fit in {@value #MAX_STACK}
     * characters, fewer frames per throwable, so the root cause stays; then a cut.
     */
    static String stack(IThrowableProxy t) {
        String s = "";
        for (int frames : new int[] {MAX_FRAMES, 12, 5, 2, 0}) {
            s = stack(t, frames);
            if (s.length() <= MAX_STACK) {
                return s;
            }
        }
        return s.substring(0, MAX_STACK - TRUNCATED.length()) + TRUNCATED;
    }

    private static String stack(IThrowableProxy top, int maxFrames) {
        StringBuilder b = new StringBuilder();
        IdentityHashMap<IThrowableProxy, Boolean> seen = new IdentityHashMap<>();
        boolean first = true;
        for (IThrowableProxy p = top; p != null && seen.put(p, Boolean.TRUE) == null && seen.size() <= 16; p = p.getCause()) {
            if (!first) {
                if (!b.isEmpty()) {
                    b.append('\n');
                }
                b.append("caused by: ").append(kind(p.getClassName()));
                if (p.getMessage() != null) {
                    b.append(": ").append(Pii.maskProse(cut(p.getMessage(), MAX_ERROR_MESSAGE)));
                }
            }
            StackTraceElementProxy[] frames = p.getStackTraceElementProxyArray();
            int n = frames == null ? 0 : Math.min(frames.length, maxFrames);
            for (int i = 0; i < n; i++) {
                if (!b.isEmpty()) {
                    b.append('\n');
                }
                b.append("at ").append(frames[i].getStackTraceElement());
            }
            if (frames != null && frames.length > n) {
                if (!b.isEmpty()) {
                    b.append('\n');
                }
                b.append("... ").append(frames.length - n).append(" more");
            }
            first = false;
        }
        return b.toString();
    }

    // ---- size ------------------------------------------------------------------------------------

    /**
     * The line within {@value #MAX_LINE} bytes: {@code attrs} strings and nested values cut to 256
     * characters; then the stack shortened and the error's message cut; then {@code attrs} replaced
     * by {@code {"truncated": true}}. Never the line itself.
     */
    @SuppressWarnings("unchecked")
    static String fit(Map<String, Object> line) {
        String s = json(line);
        if (bytes(s) <= MAX_LINE) {
            return s;
        }
        Object a = line.get("attrs");
        if (a instanceof Map<?, ?> attrs) {
            Map<String, Object> shorter = new LinkedHashMap<>();
            ((Map<String, Object>) attrs).forEach((k, v) -> shorter.put(k, v instanceof String str ? cut(str, 256)
                    : v instanceof Map<?, ?> || v instanceof Collection<?> ? cut(json(v), 256) : v));
            line.put("attrs", shorter);
            s = json(line);
            if (bytes(s) <= MAX_LINE) {
                return s;
            }
        }
        if (line.get("error") instanceof Map<?, ?> err) {
            Map<String, Object> e = (Map<String, Object>) err;
            if (e.get("message") instanceof String m) {
                e.put("message", cut(m, 256));
            }
            if (e.get("stack") instanceof String st) {
                int over = bytes(s) - MAX_LINE;
                int keep = Math.max(0, st.length() - over * 2 - 64);
                e.put("stack", keep < TRUNCATED.length() ? TRUNCATED : st.substring(0, keep - TRUNCATED.length()) + TRUNCATED);
            }
            s = json(line);
            if (bytes(s) <= MAX_LINE) {
                return s;
            }
            e.remove("stack");
        }
        line.put("attrs", Map.of("truncated", true));
        return json(line);
    }

    private static String json(Object v) {
        StringBuilder out = new StringBuilder(512);
        Json.value(out, v);
        return out.toString();
    }

    /** UTF-8 bytes of {@code s} (the JSON writer escapes control characters, so no surrogate is lone). */
    static int bytes(String s) {
        if (s.length() * 3 <= MAX_LINE) {
            return s.length();
        }
        int n = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c < 0x80) {
                n++;
            } else if (c < 0x800) {
                n += 2;
            } else if (Character.isHighSurrogate(c)) {
                n += 4;
                i++;
            } else {
                n += 3;
            }
        }
        return n;
    }

    /** At most {@code max} characters, a cut marked with {@code ...}; never half a surrogate pair. */
    static String cut(String s, int max) {
        if (s == null || s.length() <= max) {
            return s;
        }
        int end = max - 3;
        if (Character.isHighSurrogate(s.charAt(end - 1))) {
            end--;
        }
        return s.substring(0, end) + "...";
    }
}
