package parity.telephonycontroller.journal;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/** The fake's journal: in memory, surviving a simulated TC restart but not the process. */
public final class InMemoryJournal implements Journal {

    private final ReentrantLock lock = new ReentrantLock();
    private final List<LegEnded> legs = new ArrayList<>();
    private final List<SessionEnded> sessions = new ArrayList<>();
    private final List<Acked> acks = new ArrayList<>();
    private final List<Released> released = new ArrayList<>();

    @Override
    public void legEnded(LegEnded e) {
        add(legs, e);
    }

    @Override
    public void sessionEnded(SessionEnded e) {
        add(sessions, e);
    }

    @Override
    public void acked(Acked e) {
        add(acks, e);
    }

    @Override
    public void released(Released e) {
        add(released, e);
    }

    @Override
    public Contents load() {
        lock.lock();
        try {
            return new Contents(List.copyOf(legs), List.copyOf(sessions), List.copyOf(acks), List.copyOf(released));
        } finally {
            lock.unlock();
        }
    }

    private volatile String incarnation;

    @Override
    public void incarnation(String i) {
        incarnation = i;
    }

    @Override
    public java.util.Optional<String> lastIncarnation() {
        return java.util.Optional.ofNullable(incarnation);
    }

    /** Forgets everything (a script's reset). */
    public void clear() {
        lock.lock();
        try {
            legs.clear();
            sessions.clear();
            acks.clear();
            released.clear();
        } finally {
            lock.unlock();
        }
    }

    private <T> void add(List<T> list, T e) {
        lock.lock();
        try {
            list.add(e);
        } finally {
            lock.unlock();
        }
    }
}
