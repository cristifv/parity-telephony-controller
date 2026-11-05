package parity.telephonycontroller.time;

/**
 * The controller's time: epoch milliseconds and timers. Production runs on the system clock;
 * the fake and the script runners on a {@link ManualClock} that moves only when told to, so a
 * script's {@code advance} steps are exact (TC ADR 0001 §6).
 */
public interface NodeClock {

    /** Now, epoch milliseconds. */
    long nowMs();

    /** Runs {@code task} once, at {@code atMs} or as soon as possible after it. */
    Timer schedule(long atMs, Runnable task);

    /** A scheduled task. */
    interface Timer {
        /** Stops it if it hasn't run; harmless otherwise. */
        void cancel();
    }
}
