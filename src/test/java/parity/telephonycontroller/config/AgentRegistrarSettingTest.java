package parity.telephonycontroller.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import parity.telephonycontroller.lifecycle.ConfigurationRefusedException;

/** TC-056: node.agent-registrar-domain is empty, a domain, or {tenant}.<domain>; anything else stops the start. */
class AgentRegistrarSettingTest {

    @Test
    void theThreeFormsStartAndAMalformedTemplateDoesNot() {
        assertThat(TelephonyWiring.agentRegistrar("")).isNull();
        assertThat(TelephonyWiring.agentRegistrar("acme.agents.localhost").perTenant()).isFalse();
        assertThat(TelephonyWiring.agentRegistrar("{tenant}.agents.localhost").perTenant()).isTrue();
        assertThat(TelephonyWiring.agentRegistrar("*.agents.localhost").toString()).isEqualTo("{tenant}.agents.localhost");
        assertThatThrownBy(() -> TelephonyWiring.agentRegistrar("{tenant}.Agents.localhost"))
                .isInstanceOf(ConfigurationRefusedException.class)
                .hasMessageContaining("node.agent-registrar-domain");
    }
}
