package parity.telephonycontroller.obs;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import java.util.concurrent.atomic.AtomicLong;
import parity.telephonycontroller.model.UuidV7;

/**
 * This process's place in log-record.v1 (parity-contracts CONVENTIONS "Logging"): its
 * {@code incarnation}, a UUIDv7 made once per run, and {@code seq}, 1 for the process's first
 * record and +1 per record made, the ones the log queue later drops included (a gap shows where).
 * Both live here, outside logback, so a reconfiguration (Spring Boot's, or a test's) never
 * restarts them.
 */
public final class LogProcess {

    private static final String INCARNATION = UuidV7.create();
    private static final AtomicLong SEQ = new AtomicLong();
    private static volatile JsonLogFormatter.Identity identity;

    private LogProcess() {}

    /** This run's incarnation (a UUIDv7). */
    public static String incarnation() {
        return INCARNATION;
    }

    /** The next {@code seq}. */
    static long nextSeq() {
        return SEQ.incrementAndGet();
    }

    /**
     * The record's {@code seq}: the one it was given when it was made (by the appender, on the
     * logging thread, before the queue), or a new one now when nothing gave it one.
     */
    static long seq(ILoggingEvent e) {
        long s = e.getSequenceNumber();
        if (s > 0) {
            return s;
        }
        long next = nextSeq();
        if (e instanceof LoggingEvent le) {
            le.setSequenceNumber(next);
        }
        return next;
    }

    /** Who is logging: the last identity an encoder started with, or the system's. */
    static JsonLogFormatter.Identity identity() {
        JsonLogFormatter.Identity i = identity;
        return i != null ? i : JsonLogFormatter.Identity.fromSystem();
    }

    static void identity(JsonLogFormatter.Identity i) {
        identity = i;
    }

    /**
     * The engine's incarnation (telephony.v1's {@code WatchResponse.incarnation}): this run's
     * log incarnation when it sorts after {@code last} (the journal's, from the run before), so
     * the Watch stream and the logs name the run alike; otherwise (the clock went back) a UUIDv7
     * after {@code last}, as {@link UuidV7#after} makes it (TC ADR 0002 §2.6).
     */
    public static String engineIncarnation(String last) {
        if (last == null) {
            return INCARNATION;
        }
        try {
            if (UuidV7.unixMs(INCARNATION) > UuidV7.unixMs(last)) {
                return INCARNATION;
            }
        } catch (IllegalArgumentException notV7) {
            return INCARNATION;
        }
        return UuidV7.after(last);
    }
}
