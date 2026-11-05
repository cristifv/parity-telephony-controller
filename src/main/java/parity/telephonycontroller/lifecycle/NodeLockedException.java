package parity.telephonycontroller.lifecycle;

import java.nio.file.Path;

/** Another controller on this host holds the node lock: this one must not start. */
public class NodeLockedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public NodeLockedException(Path lockFile) {
        super("another telephony controller holds the node lock " + lockFile + "; refusing to start");
    }
}
