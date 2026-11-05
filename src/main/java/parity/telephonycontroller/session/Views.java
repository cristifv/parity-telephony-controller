package parity.telephonycontroller.session;

import java.util.List;
import parity.telephonycontroller.model.Kinds.EndCause;
import parity.telephonycontroller.model.Kinds.EndedBy;
import parity.telephonycontroller.model.Kinds.Endpoint;
import parity.telephonycontroller.model.Kinds.LegState;
import parity.telephonycontroller.model.Kinds.MediaKind;
import parity.telephonycontroller.model.Kinds.Role;
import parity.telephonycontroller.model.Kinds.SessionEndReason;
import parity.telephonycontroller.model.Kinds.SessionState;

/** Snapshots for {@code ClaimSession} and {@code GetSession}. */
public final class Views {

    private Views() {}

    /** A leg. Media state, digit reporting and plays come with steps 3 and 4. */
    public record LegView(String legId, Role role, LegState state, Endpoint endpoint, String sipCallId,
            EndCause cause, int sipStatus, EndedBy endedBy, long answeredAtMs, long endedAtMs) {}

    /** A session, or an ended session's tombstone. */
    public record SessionView(String tenantId, String interactionId, long ownerEpoch, String ownerReplicaId,
            SessionState state, MediaKind media, boolean onHold, List<LegView> legs, long lastDeliverySeq,
            SessionEndReason endReason, long endedAtMs, long arrivalDeadlineMs) {}

    /** A claim's answer. */
    public record Claimed(SessionView session, boolean existing) {}

    /** {@code existing} and the leg's state now ({@code AddLeg}). */
    public record LegResult(boolean existing, LegState state, EndCause cause) {}

    /** {@code existing} and {@code already_gone} ({@code RemoveLeg}, {@code EndSession}). */
    public record Gone(boolean existing, boolean alreadyGone) {}

    /** {@code DeclineArrival}'s answer. */
    public record Declined(boolean existing, boolean rejected) {}
}
