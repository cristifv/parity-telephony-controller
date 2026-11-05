package parity.telephonycontroller.node;

import java.util.Locale;
import java.util.regex.Pattern;

/**
 * The agents freeswitch dials from its own directory ({@code user/<agent>@<domain>}) because they
 * registered with freeswitch itself, not with opensips (TC-056). {@code node.agent-registrar-domain}
 * is one of:
 * <ul>
 *   <li><b>A domain</b> ({@code acme.agents.localhost}): the single-domain setting from before
 *       TC-056, unchanged. A contact in exactly that domain is dialled from the directory; any other
 *       goes to sofia (through {@code agent-proxy} when set).
 *   <li><b>A template</b> ({@code {tenant}.agents.localhost}, or {@code *.agents.localhost}): every
 *       tenant's agents registered with freeswitch, each tenant in its own domain whose first label
 *       is the tenant id. The domain dialled is built from the <i>leg's tenant</i> (the session's,
 *       from IE's verified owner), never taken from the contact: a contact in any other domain,
 *       another tenant's included, is refused before anything is dialled.
 * </ul>
 * Empty (production): agents register with opensips and every agent leg goes to sofia.
 */
public final class AgentRegistrar {

    /** parity-contracts CONVENTIONS: a tenant id is a DNS label. */
    private static final Pattern TENANT_ID = Pattern.compile("^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$");
    /** A template's fixed part: one or more lowercase DNS labels. */
    private static final Pattern SUFFIX = Pattern.compile(
            "^(?=.{1,189}$)[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)*$");
    private static final Pattern LITERAL = Pattern.compile("^[A-Za-z0-9.-]{1,253}$");
    private static final String TENANT = "{tenant}.";
    private static final String ANY = "*.";

    /** The single domain, or null for a template. */
    private final String literal;
    /** A template's {@code .<suffix>}, lowercase, or null for a single domain. */
    private final String dotSuffix;

    private AgentRegistrar(String literal, String dotSuffix) {
        this.literal = literal;
        this.dotSuffix = dotSuffix;
    }

    /**
     * The setting, or null when it is empty (production).
     *
     * @throws IllegalArgumentException a template that isn't {@code {tenant}.<labels>} or
     *     {@code *.<labels>}, or a domain with characters no agent contact can have
     */
    public static AgentRegistrar parse(String setting) {
        if (setting == null || setting.isBlank()) {
            return null;
        }
        String v = setting.strip();
        String rest = v.startsWith(TENANT) ? v.substring(TENANT.length())
                : v.startsWith(ANY) ? v.substring(ANY.length()) : null;
        if (rest != null) {
            if (!SUFFIX.matcher(rest).matches()) {
                throw new IllegalArgumentException("a template is {tenant}.<lowercase DNS labels>");
            }
            return new AgentRegistrar(null, "." + rest);
        }
        if (!LITERAL.matcher(v).matches()) {
            throw new IllegalArgumentException("neither a domain nor {tenant}.<domain>");
        }
        return new AgentRegistrar(v, null);
    }

    /** A per-tenant template (not the single domain). */
    public boolean perTenant() {
        return dotSuffix != null;
    }

    /**
     * The directory domain to dial an agent of {@code tenantId} at, given its contact's host; null
     * when freeswitch isn't that agent's registrar (single-domain mode only: the leg goes to sofia).
     *
     * @throws NodePort.Refused per-tenant mode: the tenant id isn't a DNS label, or the host isn't
     *     the tenant's own domain ({@code TENANT_MISMATCH} when it is another name in the template's
     *     domain, {@code INVALID_TARGET} when it is outside it)
     */
    String directoryDomain(String tenantId, String host) {
        if (dotSuffix == null) {
            // Before TC-056, unchanged: exact match.
            return literal.equals(host) ? literal : null;
        }
        if (tenantId == null || !TENANT_ID.matcher(tenantId).matches()) {
            throw new NodePort.Refused(NodePort.Refused.Why.INVALID_TARGET, "the leg's tenant id isn't a DNS label");
        }
        String own = tenantId + dotSuffix;
        String h = host == null ? "" : host.toLowerCase(Locale.ROOT);
        if (h.equals(own)) {
            return own;
        }
        if (h.endsWith(dotSuffix)) {
            throw new NodePort.Refused(NodePort.Refused.Why.TENANT_MISMATCH,
                    "an agent contact in a registrar domain that isn't the leg's tenant's");
        }
        throw new NodePort.Refused(NodePort.Refused.Why.INVALID_TARGET,
                "an agent contact outside the agents' registrar domains");
    }

    @Override
    public String toString() {
        return dotSuffix == null ? literal : "{tenant}" + dotSuffix;
    }
}
