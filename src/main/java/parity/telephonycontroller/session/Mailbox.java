package parity.telephonycontroller.session;

import java.util.ArrayDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A session's mailbox (TC ADR 0002 §2.1): its turns run one at a time, in the order they were
 * queued, which is the single writer the fence relies on.
 *
 * <ul>
 *   <li>{@link #call}: a turn whose caller waits for its result. If the mailbox is idle the caller
 *       drains it on its own thread until its turn is done (R1-13: deterministic on a manual clock,
 *       no thread hop), then hands what is left to a virtual thread. Inside a turn, a nested call on
 *       the same mailbox runs at once (as the reentrant lock it replaces did).
 *   <li>{@link #post}: a turn nobody waits for (ESL's events): a virtual thread drains it.
 * </ul>
 *
 * <p>Commands and node facts are bounded ({@code bounded}: at most {@link #CAPACITY} queued, past
 * that {@link Busy}); the engine's own turns (timers) are not. No {@code synchronized} (R1-2).
 */
final class Mailbox {

    static final int CAPACITY = 1024;

    /** The mailbox is full: nothing was queued. */
    static final class Busy extends Exception {
        private static final long serialVersionUID = 1L;

        Busy() {
            super("the session's mailbox is full", null, false, false);
        }
    }

    /** One turn's work. */
    @FunctionalInterface
    interface Turn<R> {
        R run() throws TelephonyException;
    }

    private static final class Task {
        final Turn<?> work;
        final CompletableFuture<Object> done = new CompletableFuture<>();

        Task(Turn<?> work) {
            this.work = work;
        }

        void run() {
            try {
                done.complete(work.run());
            } catch (TelephonyException | RuntimeException | Error e) {
                done.completeExceptionally(e);
            }
        }
    }

    private final ReentrantLock lock = new ReentrantLock();
    private final ArrayDeque<Task> queue = new ArrayDeque<>();
    private Thread drainer;
    private int capacity = CAPACITY;

    /** Runs {@code turn} and returns its result; {@code bounded} turns may be refused when full. */
    @SuppressWarnings("unchecked")
    <R> R call(Turn<R> turn, boolean bounded) throws TelephonyException, Busy {
        Task t;
        boolean drainHere;
        lock.lock();
        try {
            if (isCurrent(drainer)) {
                lock.unlock();
                try {
                    return turn.run();
                } finally {
                    lock.lock();
                }
            }
            if (bounded && queue.size() >= capacity) {
                throw new Busy();
            }
            t = new Task(turn);
            queue.add(t);
            drainHere = drainer == null;
            if (drainHere) {
                drainer = Thread.currentThread();
            }
        } finally {
            lock.unlock();
        }
        if (drainHere) {
            drain(t);
        }
        try {
            return (R) t.done.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while waiting for a turn", e);
        } catch (ExecutionException e) {
            Throwable c = e.getCause();
            if (c instanceof TelephonyException te) {
                throw te;
            }
            if (c instanceof RuntimeException re) {
                throw re;
            }
            if (c instanceof Error er) {
                throw er;
            }
            throw new IllegalStateException("a turn failed", c);
        }
    }

    /** Queues {@code turn} for a virtual thread; nobody waits for it. */
    void post(Turn<?> turn, boolean bounded) throws Busy {
        Thread start = null;
        lock.lock();
        try {
            if (bounded && queue.size() >= capacity) {
                throw new Busy();
            }
            queue.add(new Task(turn));
            if (drainer == null) {
                start = Thread.ofVirtual().name("tc-session").unstarted(() -> drain(null));
                drainer = start;
            }
        } finally {
            lock.unlock();
        }
        if (start != null) {
            start.start();
        }
    }

    /** Turns queued and not yet run. */
    int queued() {
        lock.lock();
        try {
            return queue.size();
        } finally {
            lock.unlock();
        }
    }

    /** Test hook (the fake's {@code mailbox_full}): the capacity for bounded turns; 0 refuses them all. */
    void capacity(int n) {
        lock.lock();
        try {
            capacity = n;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Runs queued turns on this thread. With {@code mine}, stops once it is done and hands the rest
     * to a virtual thread, so a caller doesn't serve others' turns for long.
     */
    private void drain(Task mine) {
        while (true) {
            Task next;
            Thread handOff = null;
            lock.lock();
            try {
                if (mine != null && mine.done.isDone()) {
                    if (!queue.isEmpty()) {
                        handOff = Thread.ofVirtual().name("tc-session").unstarted(() -> drain(null));
                        drainer = handOff;
                    } else {
                        drainer = null;
                    }
                    next = null;
                } else {
                    next = queue.poll();
                    if (next == null) {
                        drainer = null;
                    }
                }
            } finally {
                lock.unlock();
            }
            if (handOff != null) {
                handOff.start();
                return;
            }
            if (next == null) {
                return;
            }
            next.run();
        }
    }

    /** Identity: is {@code t} the thread running this code. */
    @SuppressWarnings("ReferenceEquality")
    private static boolean isCurrent(Thread t) {
        return t == Thread.currentThread();
    }
}
