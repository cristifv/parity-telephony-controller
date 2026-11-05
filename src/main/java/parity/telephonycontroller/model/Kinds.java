package parity.telephonycontroller.model;

/**
 * The controller's own vocabulary, mirroring {@code telephony.v1}'s enums without depending on
 * the generated classes: only the {@code api} package maps between the two (TC ADR 0001 §4.7).
 */
public final class Kinds {

    private Kinds() {}

    /** A leg's role, set once; it scopes DTMF actions. */
    public enum Role { CUSTOMER, AGENT, SUPERVISOR, EXTERNAL }

    /** A leg's state. PARKED: an unanswered arrival. */
    public enum LegState { PARKED, ORIGINATING, RINGING, ANSWERED, ENDED }

    /** Why a leg ended. */
    public enum EndCause { NORMAL, NO_ANSWER, BUSY, DECLINED, UNREACHABLE, CANCELED, MEDIA_FAILED, SYSTEM }

    /** Who ended a leg: the far end, IE's command, or TC or freeswitch on their own. */
    public enum EndedBy { REMOTE, COMMAND, SYSTEM }

    /** A session's state; ENDED is a tombstone. */
    public enum SessionState { ARRIVING, ACTIVE, ENDED }

    /** The session's media. */
    public enum MediaKind { NONE, CALL, CONFERENCE }

    /** Why a session ended. */
    public enum SessionEndReason {
        ALL_LEGS_GONE, ENDED_BY_OWNER, ARRIVAL_UNCLAIMED, ARRIVAL_DECLINED, FREESWITCH_LOST,
        PARITY_MS_LOST, DRAIN_TIMEOUT
    }

    /** What TC did with a far end's re-INVITE. */
    public enum RemoteMediaKind { ADDRESS, UNANSWERABLE, UNREAD }

    /** The far end's media stack. */
    public enum Endpoint { WEBRTC, SIP }
}
