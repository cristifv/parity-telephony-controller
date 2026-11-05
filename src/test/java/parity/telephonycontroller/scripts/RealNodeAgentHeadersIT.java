package parity.telephonycontroller.scripts;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import parity.contracts.telephony.v1.Endpoint;
import parity.telephonycontroller.model.UuidV7;
import parity.telephonycontroller.realnode.TestSipAgent;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * TC-031 on real freeswitch (the dialplan, the loopback carrier, sofia), media simulated: an agent's
 * INVITE carries {@code X-Parity-Interaction-Id} and {@code X-Parity-Leg-Id} once each, with TC's
 * values (the session's interaction id, TC's leg id), and a carrier's forged {@code X-Parity-*}
 * headers never reach it. The carrier's copies of the two are removed from the arrival. The results
 * go to {@code target/tc-evidence/agent-headers.txt}.
 */
class RealNodeAgentHeadersIT {

    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static RealNodeTarget target;
    private static LiveCalls calls;
    private static final Map<String, String> REPORT = new LinkedHashMap<>();

    @BeforeAll
    static void start() throws Exception {
        target = new RealNodeTarget();
        calls = new LiveCalls(target);
    }

    @AfterAll
    static void stop() throws IOException {
        Path out = Path.of("target/tc-evidence");
        Files.createDirectories(out);
        StringBuilder b = new StringBuilder("# TC-031: X-Parity-* on agent legs, forged ones stripped (one run)\n");
        REPORT.forEach((k, v) -> b.append(k).append(": ").append(v).append('\n'));
        Files.writeString(out.resolve("agent-headers.txt"), b.toString(), StandardCharsets.UTF_8);
        System.out.print(b);
        if (calls != null) {
            calls.close();
        }
        if (target != null) {
            target.close();
        }
    }

    @Test
    void agentInviteCarriesTcsHeadersAndNoForgedOne() throws Exception {
        calls.begin();
        String forged = UuidV7.create();
        Map<String, String> forgedHeaders = new LinkedHashMap<>();
        forgedHeaders.put("X-Parity-Interaction-Id", forged);
        forgedHeaders.put("x-parity-interaction-id", forged); // a repeat: sofia keeps it as sip_h_<name>-1
        forgedHeaders.put("X-Parity-Leg-Id", forged);
        forgedHeaders.put("X-Parity-Agent", "forged"); // a name TC doesn't use
        LiveCalls.Inbound c = calls.arrive(TestSipAgent.sdp(41_900, 1, "0"), forgedHeaders);

        // The arrival: the carrier's copies of TC's two headers are gone, its other headers kept.
        String arrival = channel(r -> "carrier".equals(r.path("context").asString("")));
        assertEquals("_undef_", var(arrival, "sip_h_X-Parity-Interaction-Id"));
        assertEquals("_undef_", var(arrival, "sip_h_X-Parity-Leg-Id"));
        assertEquals("false", var(arrival, "sip_copy_custom_headers"));
        assertEquals(c.interactionId, var(arrival, "sip_h_X-Interaction-ID"));
        REPORT.put("arrival: sip_h_X-Parity-Interaction-Id / sip_h_X-Parity-Leg-Id", "_undef_ / _undef_ (unset by the dialplan)");
        REPORT.put("arrival: the repeat (sip_h_x-parity-interaction-id-1) / another name (sip_h_X-Parity-Agent)",
                (var(arrival, "sip_h_x-parity-interaction-id-1").equals(forged) ? "kept" : "gone") + " / "
                        + (var(arrival, "sip_h_X-Parity-Agent").equals("forged") ? "kept" : "gone")
                        + " (inert: never read or copied; TC-032)");
        REPORT.put("arrival: sip_copy_custom_headers", "false");

        // The agent's leg: TC's two headers, once each, nothing forged.
        calls.answer(c);
        String agentLeg = calls.addAgent(c, "sip:agent1@acme.agents.example", Endpoint.ENDPOINT_SIP);
        TestSipAgent.Call agent = target.agent().awaitIncoming(m -> agentLeg.equals(m.h("x-test-leg-id")),
                Duration.ofSeconds(10));
        assertNotNull(agent, "no INVITE reached the agent");
        List<String> interaction = agent.invite.all("x-parity-interaction-id");
        List<String> leg = agent.invite.all("x-parity-leg-id");
        assertEquals(List.of(c.interactionId), interaction);
        assertEquals(List.of(agentLeg), leg);
        List<String> parityNames = agent.invite.headers().keySet().stream().filter(n -> n.startsWith("x-parity-")).toList();
        boolean forgedSeen = agent.invite.headers().values().stream().flatMap(List::stream).anyMatch(v -> v.contains(forged));
        assertEquals(List.of("x-parity-interaction-id", "x-parity-leg-id"), parityNames.stream().sorted().toList());
        assertTrue(!forgedSeen, "a forged X-Parity-* value reached the agent");
        REPORT.put("agent INVITE: X-Parity-Interaction-Id", interaction + " (the session's: " + c.interactionId + ")");
        REPORT.put("agent INVITE: X-Parity-Leg-Id", leg + " (TC's leg: " + agentLeg + ")");
        REPORT.put("agent INVITE: X-Parity-* names", parityNames.toString());
        REPORT.put("agent INVITE: the forged value " + forged + " anywhere", String.valueOf(forgedSeen));

        // loopback-b, which bridges to the agent, copies no custom header.
        String loopbackB = channel(r -> r.path("name").asString("").contains(agentLeg)
                && r.path("name").asString("").endsWith("-b"));
        assertEquals("false", var(loopbackB, "sip_copy_custom_headers"));
        REPORT.put("loopback-b: sip_copy_custom_headers", "false");
        agent.fail(486);
    }

    /** The uuid of the one channel matching {@code p}, from {@code show channels} (it lags: awaited). */
    private static String channel(Predicate<JsonNode> p) throws Exception {
        String[] found = new String[1];
        LiveCalls.await(() -> {
            try {
                for (JsonNode r : JSON.readTree(target.node().esl().api("show channels as json")).path("rows")) {
                    if (p.test(r)) {
                        found[0] = r.path("uuid").asString();
                        return true;
                    }
                }
                return false;
            } catch (Exception e) {
                return false;
            }
        }, 5_000, "the channel isn't listed");
        return found[0];
    }

    private static String var(String uuid, String name) throws Exception {
        return target.node().esl().api("uuid_getvar " + uuid + " " + name).trim();
    }
}
