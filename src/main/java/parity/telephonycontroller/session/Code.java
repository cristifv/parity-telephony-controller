package parity.telephonycontroller.session;

/**
 * The controller's error codes: {@code telephony.v1}'s {@code ErrorCode} without its prefix. The
 * {@code api} package maps each to its gRPC status and the {@code parity-error} trailer.
 */
public enum Code {
    STALE_OWNER, OWNER_UNVERIFIED, OWNER_CHECK_UNAVAILABLE, TENANT_MISMATCH, INVALID_REQUEST,
    IDENTITY_MISMATCH, SESSION_UNKNOWN, LEG_UNKNOWN, LEG_CONFLICT, COMMAND_CONFLICT, WRONG_NODE,
    INVALID_STATE, SESSION_ENDED, ARRIVAL_CLOSED, PROMPT_UNKNOWN, NO_CAPACITY, DRAINING, REBUILDING,
    FREESWITCH_UNAVAILABLE, PARITY_MS_UNAVAILABLE, STREAM_SUPERSEDED, INTERNAL, ATTEMPT_EXPIRED, SESSION_BUSY;

    /** The trailer's value: the name, lowercased. */
    public String trailer() {
        return name().toLowerCase(java.util.Locale.ROOT);
    }
}
