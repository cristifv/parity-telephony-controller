package parity.telephonycontroller.esl;

/** freeswitch sent bytes that aren't a frame within the limits; the connection is closed. */
public final class EslProtocolException extends Exception {

    private static final long serialVersionUID = 1L;

    /** What was wrong: the message is this name, never the bytes. */
    public enum Reason { HEADER_BLOCK_TOO_LONG, TOO_MANY_HEADERS, BAD_HEADER_LINE, BAD_CONTENT_LENGTH, BODY_TOO_LONG }

    private final Reason reason;

    public EslProtocolException(Reason reason) {
        super(reason.name(), null, false, false);
        this.reason = reason;
    }

    public Reason reason() {
        return reason;
    }
}
