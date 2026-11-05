package parity.telephonycontroller.owner;

/** etcd didn't answer in time: the check fails closed ({@code owner_check_unavailable}). */
public final class OwnerDirectoryUnavailable extends Exception {

    private static final long serialVersionUID = 1L;

    public OwnerDirectoryUnavailable(Throwable cause) {
        super("the owner directory is unavailable", cause);
    }
}
