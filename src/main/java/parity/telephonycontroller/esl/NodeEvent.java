package parity.telephonycontroller.esl;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;
import tools.jackson.core.ObjectReadContext;
import tools.jackson.core.json.JsonFactory;

/**
 * An ESL event as TC keeps it (TC ADR 0002 §1): only the allow-listed keys of the JSON body; every
 * other value is skipped unread. Values may be personal data (numbers) or SDPs: {@link #toString()}
 * prints the event's name and its keys, never values.
 */
public final class NodeEvent {

    /** The keys kept, besides every {@code variable_cc_*}. */
    static final Set<String> KEPT = Set.of(
            "Event-Name", "Event-Subclass", "Event-Sequence", "Core-UUID", "Unique-ID", "Job-UUID",
            "Answer-State", "Hangup-Cause", "Application", "Application-Response", "Application-UUID",
            "Caller-Destination-Number", "Caller-Caller-ID-Number", "Channel-Call-State",
            "variable_sip_term_status", "variable_switch_r_sdp", "variable_sip_call_id",
            "variable_originate_disposition", "variable_hangup_cause", "cc_leg", "parity-probe-seq", "_body");

    /** Keys whose value may be logged (TC ADR 0002 §1's allow-list). */
    public static final Set<String> LOGGABLE = Set.of("Event-Name", "Event-Subclass", "Unique-ID", "Job-UUID",
            "Hangup-Cause", "Answer-State", "Application");

    private static final JsonFactory JSON = JsonFactory.builder().build();

    private final Map<String, String> fields;

    public NodeEvent(Map<String, String> fields) {
        this.fields = Collections.unmodifiableMap(new HashMap<>(fields));
    }

    /** The allow-listed keys of a {@code text/event-json} body. */
    public static NodeEvent parse(byte[] json) throws EslProtocolException {
        Map<String, String> out = new HashMap<>();
        try (JsonParser p = JSON.createParser(ObjectReadContext.empty(), json)) {
            if (p.nextToken() != JsonToken.START_OBJECT) {
                throw new EslProtocolException(EslProtocolException.Reason.BAD_HEADER_LINE);
            }
            while (p.nextToken() == JsonToken.PROPERTY_NAME) {
                String key = p.currentName();
                JsonToken v = p.nextToken();
                if (v == JsonToken.START_OBJECT || v == JsonToken.START_ARRAY) {
                    p.skipChildren();
                } else if (v != null && v.isScalarValue() && (KEPT.contains(key) || key.startsWith("variable_cc_"))) {
                    out.put(key, p.getString());
                }
            }
        } catch (tools.jackson.core.JacksonException e) {
            throw new EslProtocolException(EslProtocolException.Reason.BAD_HEADER_LINE);
        }
        return new NodeEvent(out);
    }

    public String get(String key) {
        return fields.get(key);
    }

    /** A {@code variable_cc_*} value: {@code cc("leg_id")} reads {@code variable_cc_leg_id}. */
    public String cc(String name) {
        return fields.get("variable_cc_" + name);
    }

    public String name() {
        String n = fields.get("Event-Name");
        return n == null ? "" : n;
    }

    public String subclass() {
        String s = fields.get("Event-Subclass");
        return s == null ? "" : s;
    }

    public String uuid() {
        return fields.get("Unique-ID");
    }

    public String coreUuid() {
        return fields.get("Core-UUID");
    }

    public long sequence() {
        String s = fields.get("Event-Sequence");
        try {
            return s == null ? 0 : Long.parseLong(s.trim());
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    public Map<String, String> fields() {
        return fields;
    }

    @Override
    public String toString() {
        return "NodeEvent[" + name() + " " + subclass() + ", keys " + fields.keySet() + "]";
    }
}
