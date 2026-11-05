package parity.telephonycontroller.journal;

import java.util.List;
import parity.telephonycontroller.model.Kinds.EndCause;
import parity.telephonycontroller.model.Kinds.EndedBy;
import parity.telephonycontroller.model.Kinds.Endpoint;
import parity.telephonycontroller.model.Kinds.Role;
import parity.telephonycontroller.model.Kinds.SessionEndReason;

/**
 * The hangup journal (TC PLAN R1-3, ADR 0001 §4.4): every leg's end and every session's end, kept
 * across restarts so an adopter or a retry learns the real outcome. It has no field for a digit:
 * DTMF never reaches it (R1-4).
 */
public interface Journal {

    /** A leg ended. */
    record LegEnded(String tenantId, String interactionId, String legId, Role role, Endpoint endpoint,
            String sipCallId, EndCause cause, int sipStatus, EndedBy endedBy, long answeredAtMs, long endedAtMs,
            String addLegFingerprint) {}

    /**
     * A session ended: its tombstone, owned by the epoch and replica that held it. {@code ownerless}:
     * it ended before any owner (TC-036: a short-lived tombstone, kept so across a restart even if a
     * claim raised its fence later); false when unknown (older records, a rebuild from CDR files).
     */
    record SessionEnded(String tenantId, String interactionId, SessionEndReason reason, int sipStatus,
            long endedAtMs, long ownerEpoch, String ownerReplicaId, String ownerBin, boolean ownerless) {

        public SessionEnded(String tenantId, String interactionId, SessionEndReason reason, int sipStatus,
                long endedAtMs, long ownerEpoch, String ownerReplicaId, String ownerBin) {
            this(tenantId, interactionId, reason, sipStatus, endedAtMs, ownerEpoch, ownerReplicaId, ownerBin, false);
        }
    }

    /** The owner acknowledged a session's SessionEnded; its tombstone goes 10 minutes later. */
    record Acked(String tenantId, String interactionId, long atMs) {}

    /** A tombstone was dropped. */
    record Released(String tenantId, String interactionId) {}

    /** Everything the journal holds, in the order written. */
    record Contents(List<LegEnded> legs, List<SessionEnded> sessions, List<Acked> acks, List<Released> released) {}

    void legEnded(LegEnded e);

    void sessionEnded(SessionEnded e);

    void acked(Acked e);

    void released(Released e);

    Contents load();

    /** This run's incarnation, kept so the next one sorts higher whatever the clock did (TC ADR 0002 §2.6). */
    default void incarnation(String incarnation) {
    }

    /** The last run's incarnation, if the journal kept one. */
    default java.util.Optional<String> lastIncarnation() {
        return java.util.Optional.empty();
    }
}
