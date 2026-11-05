package parity.telephonycontroller.time;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * The system clock, with timers on one scheduler thread that hands each task to a virtual thread.
 * A cancelled timer leaves the scheduler's queue at once (TC-039): otherwise a tombstone's 24 h
 * release, cancelled by its ack, would keep its session reachable for the full 24 h.
 */
public final class SystemNodeClock implements NodeClock, AutoCloseable {

    private final ScheduledThreadPoolExecutor scheduler = scheduler();

    private static ScheduledThreadPoolExecutor scheduler() {
        ScheduledThreadPoolExecutor s = new ScheduledThreadPoolExecutor(1,
                Thread.ofPlatform().name("tc-timers").daemon(true).factory());
        s.setRemoveOnCancelPolicy(true);
        return s;
    }

    /** Timers waiting (a gauge; tests). */
    public int pending() {
        return scheduler.getQueue().size();
    }

    @Override
    public long nowMs() {
        return System.currentTimeMillis();
    }

    @Override
    public Timer schedule(long atMs, Runnable task) {
        long delay = Math.max(0, atMs - nowMs());
        ScheduledFuture<?> f = scheduler.schedule(
                () -> Thread.ofVirtual().name("tc-timer").start(task), delay, TimeUnit.MILLISECONDS);
        return () -> f.cancel(false);
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }
}
