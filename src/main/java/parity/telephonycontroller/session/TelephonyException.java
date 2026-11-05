package parity.telephonycontroller.session;

/** A refused command. The message is the code only: exception messages carry no data. */
public final class TelephonyException extends Exception {

    private static final long serialVersionUID = 1L;
    private final Code code;

    public TelephonyException(Code code) {
        super(code.trailer(), null, false, false);
        this.code = code;
    }

    public Code code() {
        return code;
    }
}
