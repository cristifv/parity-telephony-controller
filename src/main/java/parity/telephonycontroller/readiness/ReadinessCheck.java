package parity.telephonycontroller.readiness;

/** One thing that must hold before the controller is healthy. */
public interface ReadinessCheck {

    /** A short, stable name, for logs and {@code /readyz}. */
    String name();

    /** Null when the check passes; otherwise why not (no personal data). */
    String failure();
}
