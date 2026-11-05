package parity.telephonycontroller.session;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static parity.telephonycontroller.session.EngineFixture.BIN;
import static parity.telephonycontroller.session.EngineFixture.caller;

import java.util.List;
import org.junit.jupiter.api.Test;
import parity.telephonycontroller.model.Kinds.Endpoint;
import parity.telephonycontroller.model.Kinds.LegState;
import parity.telephonycontroller.model.Kinds.Role;
import parity.telephonycontroller.node.AgentRegistrar;
import parity.telephonycontroller.node.NodePort;
import parity.telephonycontroller.owner.OwnerRecord;

/**
 * TC-056 at the engine: with a per-tenant registrar, an agent leg outside the session tenant's own
 * domain is refused with a code IE doesn't retry (tenant_mismatch, invalid_request), before
 * anything is originated, and the leg id is free again; each tenant's own agents are dialled.
 */
class EngineAgentRegistrarTest {

    private static final String I = "0192f6a1-7c3e-7b21-9a4d-5e6f70819301";
    private static final String I2 = "0192f6a1-7c3e-7b21-9a4d-5e6f70819302";
    private static final String C = "0192f6a1-8000-7000-8000-000000000301";
    private static final String C2 = "0192f6a1-8000-7000-8000-000000000302";
    private static final String A = "0192f6a1-8000-7000-8000-0000000003a1";
    private static final String B = "0192f6a1-8000-7000-8000-0000000003b1";

    private static Requests.AddLeg agent(String leg, String contact) {
        return new Requests.AddLeg(leg, Role.AGENT, Endpoint.WEBRTC, 30, "fp-" + leg + contact, contact, null, null,
                null);
    }

    private static Requests.Owner owner(String tenant, String interactionId) {
        return new Requests.Owner(tenant, interactionId, 1042, "r1", 0, "");
    }

    /** An answered inbound call of {@code tenant}. */
    private static void answered(EngineFixture f, String tenant, String interactionId, String legId) throws Exception {
        f.node.park(new NodePort.Channel(legId, tenant, interactionId, BIN, Role.CUSTOMER, Endpoint.SIP,
                legId + "@carrier", true, f.clock.nowMs(), "+447700900100", "+447700900123", "carrier-a", List.of(),
                null, LegState.PARKED));
        f.owners.claim(interactionId, 1042, new OwnerRecord(1042, tenant, BIN, "r1", "media-1"));
        f.engine().answer(caller(), owner(tenant, interactionId), EngineFixture.key("answer-" + interactionId), legId);
    }

    private static Code refused(EngineFixture f, String tenant, String interactionId, String cmd, Requests.AddLeg a) {
        try {
            f.engine().addLeg(caller(), owner(tenant, interactionId), EngineFixture.key(cmd), a);
        } catch (TelephonyException e) {
            return e.code();
        }
        throw new AssertionError("not refused");
    }

    @Test
    void eachTenantsAgentsAreDialledAndAnotherTenantsOrAnyOtherHostIsRefused() throws Exception {
        EngineFixture f = new EngineFixture(TelephonyNode.Limits.DEFAULT);
        f.node.setAgentRegistrar(AgentRegistrar.parse("{tenant}.agents.localhost"));
        answered(f, "acme", I, C);
        answered(f, "initech-de", I2, C2);
        long mismatches = TelephonyNode.agentLegsRefused(true);
        long targets = TelephonyNode.agentLegsRefused(false);

        // acme's call can't reach initech-de's agent, nor an arbitrary host.
        assertThat(refused(f, "acme", I, "x1", agent(A, "sip:agent-b1@initech-de.agents.localhost")))
                .isEqualTo(Code.TENANT_MISMATCH);
        assertThat(refused(f, "acme", I, "x2", agent(A, "sip:agent-b1@evil.example.com")))
                .isEqualTo(Code.INVALID_REQUEST);
        assertThat(refused(f, "acme", I, "x3", agent(A, "not a uri"))).isEqualTo(Code.INVALID_REQUEST);
        assertThat(f.node.channel(A)).as("never originated").isEmpty();
        assertThat(TelephonyNode.agentLegsRefused(true) - mismatches).isEqualTo(1);
        assertThat(TelephonyNode.agentLegsRefused(false) - targets).isEqualTo(2);

        // The refused leg id isn't held: acme's own agent is dialled with it.
        assertThat(f.engine().addLeg(caller(), owner("acme", I), EngineFixture.key("ok-a"),
                agent(A, "sip:agent-a1@acme.agents.localhost")).state()).isEqualTo(LegState.ORIGINATING);
        assertThat(f.node.channel(A)).isPresent();

        // initech-de's call reaches its own agent, never acme's.
        assertThat(refused(f, "initech-de", I2, "y1", agent(B, "sip:agent-a1@acme.agents.localhost")))
                .isEqualTo(Code.TENANT_MISMATCH);
        assertThat(f.engine().addLeg(caller(), owner("initech-de", I2), EngineFixture.key("ok-b"),
                agent(B, "sip:agent-b1@initech-de.agents.localhost")).state()).isEqualTo(LegState.ORIGINATING);
        assertThat(f.node.channel(B)).isPresent();
    }

    @Test
    void theSingleDomainSettingRefusesNothingNew() throws Exception {
        EngineFixture f = new EngineFixture(TelephonyNode.Limits.DEFAULT);
        f.node.setAgentRegistrar(AgentRegistrar.parse("acme.agents.localhost"));
        answered(f, "acme", I, C);
        assertThat(f.engine().addLeg(caller(), owner("acme", I), EngineFixture.key("a"),
                agent(A, "sip:agent-b1@elsewhere.example.net")).state()).isEqualTo(LegState.ORIGINATING);
        assertThatThrownBy(() -> f.engine().addLeg(caller(), owner("acme", I), EngineFixture.key("b"),
                agent(B, "tel:+447700900123")))
                .isInstanceOfSatisfying(TelephonyException.class, e -> assertThat(e.code()).isEqualTo(Code.INVALID_REQUEST));
    }
}
