package parity.telephonycontroller.node;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parity.telephonycontroller.journal.Journal;
import parity.telephonycontroller.model.Kinds.EndedBy;
import parity.telephonycontroller.model.Kinds.Endpoint;
import parity.telephonycontroller.model.Kinds.Role;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * freeswitch's per-leg CDR files ({@code mod_json_cdr}, {@code <uuid>.cdr.json}; R1-3): how a leg
 * that ended while TC was down ended. Only TC's managed legs ({@code cc_managed}), only files
 * written since a given time (the journal's newest record, TC ADR 0002 §2.4). Read-only.
 */
final class CdrFiles {

    private static final Logger LOG = LoggerFactory.getLogger(CdrFiles.class);
    private static final JsonMapper JSON = JsonMapper.builder().build();

    private CdrFiles() {}

    static List<Journal.LegEnded> read(Path dir, long sinceMs) {
        List<Journal.LegEnded> out = new ArrayList<>();
        if (dir == null || !Files.isDirectory(dir)) {
            return out;
        }
        try (Stream<Path> files = Files.walk(dir, 2)) {
            for (Path p : files.filter(f -> f.getFileName().toString().endsWith(".cdr.json")).toList()) {
                try {
                    if (Files.getLastModifiedTime(p).toMillis() < sinceMs - 5_000) {
                        continue;
                    }
                    Journal.LegEnded e = parse(JSON.readTree(Files.readString(p)));
                    if (e != null) {
                        out.add(e);
                    }
                } catch (IOException | RuntimeException e) {
                    LOG.atWarn().addKeyValue("event", "cdr_file.read_failed").addKeyValue("error_kind", e.getClass().getSimpleName())
                            .log("a CDR file couldn't be read");
                }
            }
        } catch (IOException e) {
            LOG.atWarn().addKeyValue("event", "cdr_file.list_failed").addKeyValue("error_kind", e.getClass().getSimpleName())
                    .log("the CDR directory couldn't be listed");
        }
        return out;
    }

    static Journal.LegEnded parse(JsonNode cdr) {
        JsonNode v = cdr.path("variables");
        if (!"true".equals(v.path("cc_managed").asString(""))) {
            return null;
        }
        String role = v.path("cc_role").asString("");
        if (role.isEmpty() || role.startsWith("loopback")) {
            return null;
        }
        String legId = v.path("cc_leg_id").asString("");
        String interaction = v.path("cc_interaction_id").asString("");
        if (legId.isEmpty() || interaction.isEmpty()) {
            return null;
        }
        boolean inbound = role.equals("arrival");
        Role r = inbound ? Role.CUSTOMER : switch (role) {
            case "agent" -> Role.AGENT;
            case "supervisor" -> Role.SUPERVISOR;
            case "external" -> Role.EXTERNAL;
            default -> Role.CUSTOMER;
        };
        Endpoint endpoint = "WEBRTC".equals(v.path("cc_endpoint").asString("")) ? Endpoint.WEBRTC : Endpoint.SIP;
        long answered = RealNode.parseLong(v.path("answer_uepoch").asString("0")) / 1000;
        long end = RealNode.parseLong(v.path("end_uepoch").asString("0")) / 1000;
        int sip = (int) RealNode.parseLong(v.path("sip_term_status").asString("0"));
        var cause = RealNode.cause(v.path("hangup_cause").asString(null));
        String fingerprint = v.path("cc_fingerprint").asString("");
        return new Journal.LegEnded(v.path("cc_tenant_id").asString(""), interaction, legId, r, endpoint,
                inbound ? v.path("sip_call_id").asString(legId) : legId, cause,
                cause == parity.telephonycontroller.model.Kinds.EndCause.NORMAL ? 0 : sip, EndedBy.REMOTE, answered, end,
                fingerprint.isEmpty() ? null : fingerprint);
    }
}
