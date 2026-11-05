package parity.telephonycontroller.session;

import java.util.List;
import parity.telephonycontroller.model.Kinds.EndCause;
import parity.telephonycontroller.model.Kinds.Endpoint;
import parity.telephonycontroller.model.Kinds.Role;

/** What the {@code api} package hands the engine, already validated. */
public final class Requests {

    private Requests() {}

    /** The caller's identity: the bin of its mTLS certificate (or, in tests, the test header). */
    public record Caller(String bin) {}

    /** {@code SessionOwner}. */
    public record Owner(String tenantId, String interactionId, long epoch, String replicaId, long ackedSeq,
            String ackedIncarnation) {}

    /**
     * A command's de-duplication key (its id, its RPC, and a fingerprint of the request without the
     * acks) and this attempt's expiry.
     */
    public record CommandKey(String commandId, String rpc, String fingerprint, Attempt attempt) {

        public CommandKey(String commandId, String rpc, String fingerprint) {
            this(commandId, rpc, fingerprint, Attempt.NONE);
        }
    }

    /**
     * When this attempt of a command stops being worth acting on (TC ADR 0002 §2.5, R1-8):
     * {@code expiresAtMs} from {@code parity-attempt-expires-ms} (0: none), on the node's clock less
     * the skew budget; {@code deadlineNanos} from gRPC's relative deadline (System.nanoTime;
     * Long.MAX_VALUE: none).
     */
    public record Attempt(long expiresAtMs, long deadlineNanos) {

        public static final Attempt NONE = new Attempt(0, Long.MAX_VALUE);

        /** TC refuses this long before the expiry: the clocks of IE's host and TC's may differ by it. */
        public static final long SKEW_BUDGET_MS = 250;

        public boolean expired(long nowMs, long nowNanos) {
            return (expiresAtMs > 0 && nowMs >= expiresAtMs - SKEW_BUDGET_MS)
                    || (deadlineNanos != Long.MAX_VALUE && nowNanos - deadlineNanos >= 0);
        }
    }

    /**
     * {@code AddLeg}. The destination (an agent's contact, or a number through a trunk) and the
     * presented number are carried to the node, which dials them; they are never logged.
     */
    public record AddLeg(String legId, Role role, Endpoint endpoint, int ringTimeoutS, String fingerprint,
            String agentContact, String pstnNumber, String trunkId, String callerId, Dtmf dtmf) {

        public AddLeg(String legId, Role role, Endpoint endpoint, int ringTimeoutS, String fingerprint) {
            this(legId, role, endpoint, ringTimeoutS, fingerprint, null, null, null, null, null);
        }

        public AddLeg(String legId, Role role, Endpoint endpoint, int ringTimeoutS, String fingerprint,
                String agentContact, String pstnNumber, String trunkId, String callerId) {
            this(legId, role, endpoint, ringTimeoutS, fingerprint, agentContact, pstnNumber, trunkId, callerId, null);
        }
    }

    /** {@code AddLeg}'s DTMF action codes (P5): the codes, the facets pinned, the inter-digit timeout (0: 2 s). */
    public record Dtmf(List<parity.telephonycontroller.node.NodePort.DtmfBinding> bindings, List<String> locks,
            int interDigitMs) {}

    /** {@code DeclineArrival} with {@code REFUSED} (a redirect comes later). */
    public record Decline(String tenantId, String interactionId, String replicaId, String legId, int sipStatus) {}

    /** {@code RemoveLeg}. */
    public record RemoveLeg(String legId, EndCause cause) {}
}
