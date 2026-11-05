package parity.telephonycontroller.time;

import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A clock that moves only through {@link #advance}: due timers run on the caller's thread, in
 * time order (ties in scheduling order), each seeing {@link #nowMs()} at its own due time.
 */
public final class ManualClock implements NodeClock {

    private record Entry(long atMs, long seq, Runnable task, AtomicBoolean cancelled) {}

    private final ReentrantLock lock = new ReentrantLock();
    private final PriorityQueue<Entry> queue = new PriorityQueue<>(
            (a, b) -> a.atMs != b.atMs ? Long.compare(a.atMs, b.atMs) : Long.compare(a.seq, b.seq));
    private volatile long now;
    private long seq;

    public ManualClock(long startMs) {
        this.now = startMs;
    }

    @Override
    public long nowMs() {
        return now;
    }

    @Override
    public Timer schedule(long atMs, Runnable task) {
        AtomicBoolean cancelled = new AtomicBoolean();
        lock.lock();
        try {
            queue.add(new Entry(atMs, seq++, task, cancelled));
        } finally {
            lock.unlock();
        }
        return () -> cancelled.set(true);
    }

    /** Moves time forward by {@code ms}, running every timer that falls due on the way. */
    public void advance(long ms) {
        long target = now + Math.max(0, ms);
        while (true) {
            Entry next;
            lock.lock();
            try {
                next = queue.peek();
                if (next == null || next.atMs > target) {
                    break;
                }
                queue.poll();
                now = Math.max(now, next.atMs);
            } finally {
                lock.unlock();
            }
            if (!next.cancelled.get()) {
                next.task.run();
            }
        }
        now = target;
    }

    /** Drops every timer and sets the time (a script's reset). */
    public void reset(long startMs) {
        lock.lock();
        try {
            queue.clear();
            now = startMs;
        } finally {
            lock.unlock();
        }
    }
}
