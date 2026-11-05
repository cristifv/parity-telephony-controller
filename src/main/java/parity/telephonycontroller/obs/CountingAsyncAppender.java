package parity.telephonycontroller.obs;

import ch.qos.logback.classic.AsyncAppender;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.event.KeyValuePair;

/**
 * Logback's {@link AsyncAppender} with {@code neverBlock}: a full queue drops the line instead
 * of making the caller wait (a session's executor must never block on stdout). On the logging
 * thread, before the queue, each record gets its log-record.v1 {@code seq} ({@link LogProcess}:
 * a dropped record keeps its number, so the gap shows), and a {@code warn} or worse its caller
 * ({@code code}; cheap enough there, never taken below). The drops are counted here, exported as
 * {@code telephony_controller_log_lines_dropped_total}, and reported as a {@code log.dropped}
 * record ahead of the next record that fits, at most every 10 s. The count is taken when the
 * queue is seen full, so it can be off by the few lines raced in parallel.
 */
public class CountingAsyncAppender extends AsyncAppender {

    private static final AtomicLong DROPPED = new AtomicLong();
    private static final AtomicLong REPORTED = new AtomicLong();
    private static final AtomicLong NEXT_REPORT_NANOS = new AtomicLong(System.nanoTime());
    private static final long REPORT_EVERY_NANOS = TimeUnit.SECONDS.toNanos(10);

    public static long dropped() {
        return DROPPED.get();
    }

    @Override
    protected void append(ILoggingEvent event) {
        LogProcess.seq(event);
        if (isStarted() && getRemainingCapacity() == 0) {
            DROPPED.incrementAndGet();
        } else {
            reportDrops();
        }
        super.append(event);
    }

    @Override
    protected void preprocess(ILoggingEvent event) {
        super.preprocess(event);
        if (event.getLevel().isGreaterOrEqual(Level.WARN)) {
            event.getCallerData();
        }
    }

    /** A {@code log.dropped} record for the drops not reported yet, if the last report is 10 s old. */
    private void reportDrops() {
        long dropped = DROPPED.get();
        long reported = REPORTED.get();
        if (dropped == reported || !(getContext() instanceof LoggerContext context)) {
            return;
        }
        long now = System.nanoTime();
        long next = NEXT_REPORT_NANOS.get();
        if (now - next < 0 || !NEXT_REPORT_NANOS.compareAndSet(next, now + REPORT_EVERY_NANOS)
                || !REPORTED.compareAndSet(reported, dropped)) {
            return;
        }
        LoggingEvent report = new LoggingEvent(CountingAsyncAppender.class.getName(),
                context.getLogger(CountingAsyncAppender.class), Level.WARN,
                "log records were dropped: the log queue was full", null, null);
        report.addKeyValuePair(new KeyValuePair("event", "log.dropped"));
        report.addKeyValuePair(new KeyValuePair("dropped", dropped - reported));
        LogProcess.seq(report);
        super.append(report);
    }
}
