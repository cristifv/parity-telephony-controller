package parity.telephonycontroller.node;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import parity.telephonycontroller.model.Kinds.Endpoint;

/**
 * TC-056: agent legs for every tenant whose agents registered with freeswitch, each dialled only
 * in its own tenant's domain; the single-domain setting from before unchanged.
 */
class AgentRegistrarTest {

    private static final String TEMPLATE = "{tenant}.agents.localhost";

    private static RealNode.Config config(String registrar, String proxy) {
        return new RealNode.Config(10, Duration.ofSeconds(2), 60, proxy, AgentRegistrar.parse(registrar), Map.of(),
                Path.of("/nonexistent"), 61_000, 61_099, false);
    }

    private static String target(String registrar, String tenant, String contact) throws Exception {
        return RealNode.agentTarget(config(registrar, null), tenant, contact, Endpoint.WEBRTC);
    }

    private static NodePort.Refused.Why refusal(String registrar, String tenant, String contact) {
        try {
            target(registrar, tenant, contact);
        } catch (NodePort.Refused e) {
            return e.why();
        } catch (Exception e) {
            throw new AssertionError("not a refusal", e);
        }
        throw new AssertionError("not refused: " + tenant + " " + contact);
    }

    @Test
    void twoTenantsAgentsAreDialledFromTheirOwnDomains() throws Exception {
        assertThat(target(TEMPLATE, "acme", "sip:agent-a1@acme.agents.localhost"))
                .isEqualTo("user/agent-a1@acme.agents.localhost");
        assertThat(target(TEMPLATE, "initech-de", "sip:agent-b1@initech-de.agents.localhost"))
                .isEqualTo("user/agent-b1@initech-de.agents.localhost");
        // The '*' spelling is the same template; the domain dialled is the tenant's, lowercase.
        assertThat(target("*.agents.localhost", "initech-de", "sips:agent-b1@INITECH-DE.Agents.Localhost:5061"))
                .isEqualTo("user/agent-b1@initech-de.agents.localhost");
    }

    @Test
    void aContactInAnotherTenantsDomainIsRefusedAsATenantMismatch() {
        assertThat(refusal(TEMPLATE, "acme", "sip:agent-b1@initech-de.agents.localhost"))
                .isEqualTo(NodePort.Refused.Why.TENANT_MISMATCH);
        assertThat(refusal(TEMPLATE, "initech-de", "sip:agent-a1@acme.agents.localhost"))
                .isEqualTo(NodePort.Refused.Why.TENANT_MISMATCH);
        // A name below the tenant's domain isn't the tenant's domain.
        assertThat(refusal(TEMPLATE, "acme", "sip:agent-a1@x.acme.agents.localhost"))
                .isEqualTo(NodePort.Refused.Why.TENANT_MISMATCH);
    }

    @ParameterizedTest
    @ValueSource(strings = {"sip:agent-a1@evil.example.com", "sip:agent-a1@acme.agents.localhost.evil.example.com",
        "sip:agent-a1@agents.localhost", "sip:agent-a1@127.0.0.1", "sip:agent-a1@acme", "sip:agent-a1@acme.agents.localhos"})
    void aContactOutsideThePatternIsRefused(String contact) {
        assertThat(refusal(TEMPLATE, "acme", contact)).isEqualTo(NodePort.Refused.Why.INVALID_TARGET);
    }

    @ParameterizedTest
    @ValueSource(strings = {"Acme", "-acme", "acme-", "a.b", "acme_x", "", "acme agents",
        "a123456789012345678901234567890123456789012345678901234567890123"})
    void aTenantIdThatIsntADnsLabelIsRefused(String tenant) {
        // Even when the contact matches what the bad tenant id would build.
        assertThat(refusal(TEMPLATE, tenant, "sip:agent-a1@" + tenant + ".agents.localhost"))
                .isEqualTo(NodePort.Refused.Why.INVALID_TARGET);
        assertThat(refusal(TEMPLATE, null, "sip:agent-a1@acme.agents.localhost"))
                .isEqualTo(NodePort.Refused.Why.INVALID_TARGET);
    }

    @Test
    void aContactThatIsntASipUriIsRefusedNotRetried() {
        assertThat(refusal(TEMPLATE, "acme", "sip:agent-a1@acme.agents.localhost;transport=ws"))
                .isEqualTo(NodePort.Refused.Why.INVALID_TARGET);
        assertThat(refusal(null, "acme", "tel:+447700900123")).isEqualTo(NodePort.Refused.Why.INVALID_TARGET);
    }

    @Test
    void theSingleDomainSettingIsUnchanged() throws Exception {
        String acme = "acme.agents.localhost";
        assertThat(target(acme, "acme", "sip:agent-a1@acme.agents.localhost"))
                .isEqualTo("user/agent-a1@acme.agents.localhost");
        // Any other domain goes to sofia, as before TC-056 (directly, or through the proxy).
        assertThat(target(acme, "initech-de", "sip:agent-b1@initech-de.agents.localhost"))
                .isEqualTo("sofia/agents/sip:agent-b1@initech-de.agents.localhost");
        assertThat(RealNode.agentTarget(config(acme, "sip:10.0.0.5:5060"), "acme", "sip:a1@example.net:5070",
                Endpoint.SIP)).isEqualTo("sofia/deskphones/sip:a1@example.net:5070;fs_path=<sip:10.0.0.5:5060>");
        // Empty (production): every agent leg goes to sofia.
        assertThat(target("", "acme", "sip:agent-a1@acme.agents.localhost"))
                .isEqualTo("sofia/agents/sip:agent-a1@acme.agents.localhost");
        assertThat(AgentRegistrar.parse(acme).perTenant()).isFalse();
        assertThat(AgentRegistrar.parse("  ")).isNull();
        assertThat(AgentRegistrar.parse(null)).isNull();
    }

    @ParameterizedTest
    @ValueSource(strings = {"{tenant}", "{tenant}.", "*.", "{tenant}.Agents.localhost", "a.{tenant}.localhost",
        "{tenant}.{tenant}.localhost", "*.*.localhost", "{tenant}.-x.localhost", "agents localhost", "{tenant}agents"})
    void aMalformedSettingIsRefusedAtStart(String setting) {
        assertThatThrownBy(() -> AgentRegistrar.parse(setting)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void theRefusalNamesTheRuleOnly() {
        assertThatThrownBy(() -> target(TEMPLATE, "acme", "sip:agent-b1@initech-de.agents.localhost"))
                .isInstanceOf(NodePort.Refused.class)
                .hasMessageNotContaining("initech").hasMessageNotContaining("agent-b1").hasMessageNotContaining("acme");
    }
}
