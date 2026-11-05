package parity.telephonycontroller.api;

import java.util.Set;
import java.util.regex.Pattern;
import parity.contracts.telephony.v1.AddLegRequest;
import parity.contracts.telephony.v1.DeclineArrivalRequest;
import parity.contracts.telephony.v1.DeclineReason;
import parity.contracts.telephony.v1.EndCause;
import parity.contracts.telephony.v1.Endpoint;
import parity.contracts.telephony.v1.LegRole;
import parity.contracts.telephony.v1.SessionOwner;
import parity.telephonycontroller.session.Code;
import parity.telephonycontroller.session.TelephonyException;

/**
 * §4.2's field rules, the server side of contract-check's writer rules: a request a correct IE
 * never sends is refused with {@code tenant_mismatch} or {@code invalid_request} before anything
 * else happens.
 */
final class RequestChecks {

    private static final Pattern UUID = Pattern.compile("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$");
    private static final Pattern COMMAND_ID = Pattern.compile("^[A-Za-z0-9._:-]{1,200}$");
    private static final Pattern NAME = Pattern.compile("^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$");
    private static final Set<Integer> REJECT_STATUSES = Set.of(404, 480, 486, 503, 603);
    private static final Set<Integer> REFUSE_STATUSES = Set.of(403, 404, 480, 503, 603);

    private RequestChecks() {}

    /** The tenant-id metadata must equal the body's tenant (§4.2). */
    static void tenant(String metadataTenant, String bodyTenant) throws TelephonyException {
        if (metadataTenant == null || !metadataTenant.equals(bodyTenant)) {
            throw new TelephonyException(Code.TENANT_MISMATCH);
        }
    }

    /** A command's owner and id. */
    static void command(SessionOwner owner, boolean hasOwner, String commandId, boolean needsCommandId)
            throws TelephonyException {
        require(hasOwner && owner.getOwnerEpoch() >= 1 && uuid(owner.getInteractionId())
                && !owner.getTenantId().isEmpty() && NAME.matcher(owner.getReplicaId()).matches());
        require(!needsCommandId || COMMAND_ID.matcher(commandId).matches());
    }

    static void leg(String legId) throws TelephonyException {
        require(uuid(legId));
    }

    private static final java.util.regex.Pattern PROMPT = java.util.regex.Pattern.compile("^[A-Za-z0-9_.-]{1,128}$");
    private static final java.util.regex.Pattern DIGITS = java.util.regex.Pattern.compile("^[0-9*#A-D]{1,64}$");

    /** Play: a leg, a play id (a UUID), a tenant prompt id (never "." or ".."). */
    static void play(String legId, String playId, String promptId) throws TelephonyException {
        require(uuid(legId) && uuid(playId) && PROMPT.matcher(promptId).matches() && !promptId.equals(".")
                && !promptId.equals(".."));
    }

    /** Digits to send (RFC 4733's keys), at most 64. */
    static void digits(String digits) throws TelephonyException {
        require(DIGITS.matcher(digits).matches());
    }

    /**
     * Digit reporting: a mode; unless OFF, a collection id (a UUID) and a duration of 1–300 s
     * (PCI: reporting always ends on its own; the contract's bound).
     */
    static void digitReporting(String legId, String mode, String collectionId, int maxDurationS) throws TelephonyException {
        require(uuid(legId) && !mode.equals("UNSPECIFIED")
                && (mode.equals("OFF") || (uuid(collectionId) && maxDurationS >= 1 && maxDurationS <= 300)));
    }

    private static final java.util.regex.Pattern ACTION_CODE = java.util.regex.Pattern.compile("^\\*[0-9#A-D]{1,7}$");

    /** DTMF actions: at most 8 codes of {@code *} and 1–7 keys, each with an action; a timeout of 200 ms–10 s (0: 2 s). */
    static void dtmfActions(String legId, parity.contracts.telephony.v1.DtmfActions a) throws TelephonyException {
        require(uuid(legId) && a.getBindingsCount() <= 8 && a.getInterDigitTimeoutMs() <= 10_000
                && (a.getInterDigitTimeoutMs() == 0 || a.getInterDigitTimeoutMs() >= 200));
        for (parity.contracts.telephony.v1.DtmfActionBinding b : a.getBindingsList()) {
            require(ACTION_CODE.matcher(b.getDtmfSequence()).matches()
                    && b.getAction() != parity.contracts.telephony.v1.DtmfActionKind.DTMF_ACTION_KIND_UNSPECIFIED
                    && b.getAction() != parity.contracts.telephony.v1.DtmfActionKind.UNRECOGNIZED);
        }
        for (parity.contracts.telephony.v1.Facet f : a.getLocksList()) {
            require(f != parity.contracts.telephony.v1.Facet.FACET_UNSPECIFIED
                    && f != parity.contracts.telephony.v1.Facet.UNRECOGNIZED);
        }
    }

    /** A hold's music: empty, or a tenant prompt id. */
    static void prompt(String promptId) throws TelephonyException {
        require(promptId.isEmpty() || (PROMPT.matcher(promptId).matches() && !promptId.equals(".") && !promptId.equals("..")));
    }

    static void rejectStatus(int status) throws TelephonyException {
        require(REJECT_STATUSES.contains(status));
    }

    static void addLeg(AddLegRequest r) throws TelephonyException {
        require(uuid(r.getLegId()));
        require(r.getRole() != LegRole.LEG_ROLE_UNSPECIFIED && r.getRole() != LegRole.UNRECOGNIZED);
        require(r.getEndpoint() != Endpoint.ENDPOINT_UNSPECIFIED && r.getEndpoint() != Endpoint.UNRECOGNIZED);
        require(r.getRingTimeoutS() >= 5 && r.getRingTimeoutS() <= 120);
        switch (r.getDestinationCase()) {
            case AGENT_CONTACT -> require(!r.getAgentContact().isEmpty());
            case PSTN -> require(r.getPstn().getNumber().matches("^\\+[1-9][0-9]{6,14}$")
                    && !r.getPstn().getTrunkId().isEmpty());
            default -> require(false);
        }
        require(r.getCallerId().isEmpty() || r.getCallerId().matches("^\\+[1-9][0-9]{6,14}$"));
        // P5: codes (and locks) for an agent's or supervisor's leg; the engine checks them against its role.
        if (r.getDtmfActions().getBindingsCount() > 0 || r.getDtmfActions().getLocksCount() > 0) {
            dtmfActions(r.getLegId(), r.getDtmfActions());
        }
    }

    /**
     * StartRecording: a recording id, and a key whose DEK is 32 bytes with a key reference. The
     * DEK is checked and dropped here: never logged, never passed on (the null sink writes nothing).
     */
    static void startRecording(parity.contracts.telephony.v1.StartRecordingRequest r) throws TelephonyException {
        require(uuid(r.getLegId()) && uuid(r.getRecordingId()));
        require(r.hasKey() && r.getKey().getDek().size() == 32 && NAME.matcher(r.getKey().getKekId()).matches()
                && r.getKey().getWrappedDek().size() <= 4096);
    }

    static void decline(DeclineArrivalRequest r) throws TelephonyException {
        require(uuid(r.getInteractionId()) && uuid(r.getLegId()) && NAME.matcher(r.getReplicaId()).matches());
        require(r.getReason() == DeclineReason.DECLINE_REASON_REFUSED
                || r.getReason() == DeclineReason.DECLINE_REASON_WRONG_BIN);
        if (r.getReason() == DeclineReason.DECLINE_REASON_REFUSED) {
            require(REFUSE_STATUSES.contains(r.getSipStatus()));
        }
    }

    static void localCause(EndCause cause) throws TelephonyException {
        require(cause == EndCause.END_CAUSE_NORMAL || cause == EndCause.END_CAUSE_SYSTEM);
    }

    static void interaction(String interactionId) throws TelephonyException {
        require(uuid(interactionId));
    }

    static boolean uuid(String s) {
        return UUID.matcher(s).matches();
    }

    private static void require(boolean ok) throws TelephonyException {
        if (!ok) {
            throw new TelephonyException(Code.INVALID_REQUEST);
        }
    }
}
