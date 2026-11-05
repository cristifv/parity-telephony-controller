package parity.telephonycontroller.session;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import parity.telephonycontroller.journal.Journal;
import parity.telephonycontroller.node.NodePort;
import parity.telephonycontroller.owner.OwnerDirectory;
import parity.telephonycontroller.time.NodeClock;

/**
 * The current run. The gRPC service always asks it for the engine, so a restart (a new incarnation
 * over the same node, journal and etcd) or a script's reset swaps the engine underneath it.
 */
public final class EngineHolder {

    private final ReentrantLock lock = new ReentrantLock();
    private final AtomicReference<TelephonyNode> current = new AtomicReference<>();
    private TelephonyNode.Config config;
    private NodeClock clock;
    private OwnerDirectory owners;
    private Journal journal;
    private NodePort node;
    private volatile TelephonyNode.CommandObserver observer;

    public EngineHolder(TelephonyNode.Config config, NodeClock clock, OwnerDirectory owners, Journal journal,
            NodePort node) {
        this.config = config;
        this.clock = clock;
        this.owners = owners;
        this.journal = journal;
        this.node = node;
    }

    /** Starts the first run. */
    public TelephonyNode start() {
        lock.lock();
        try {
            return started(TelephonyNode.start(config, clock, owners, journal, node, 0, false));
        } finally {
            lock.unlock();
        }
    }

    public TelephonyNode get() {
        return current.get();
    }

    /** Tests: every run, this one and those after a restart, reports its commands here. */
    public void observe(TelephonyNode.CommandObserver o) {
        observer = o;
        TelephonyNode t = current.get();
        if (t != null) {
            t.observe(o);
        }
    }

    private TelephonyNode started(TelephonyNode t) {
        if (observer != null) {
            t.observe(observer);
        }
        current.set(t);
        return t;
    }

    public NodePort node() {
        return node;
    }

    public NodeClock clock() {
        return clock;
    }

    /** The process died and came back: a new incarnation rebuilds from what survived. */
    public TelephonyNode restart(String incarnation, long rebuildMs) {
        lock.lock();
        try {
            TelephonyNode old = current.get();
            boolean draining = old != null && old.draining();
            if (old != null) {
                old.close();
            }
            config = config.withIncarnation(incarnation);
            return started(TelephonyNode.start(config, clock, owners, journal, node, rebuildMs, draining));
        } finally {
            lock.unlock();
        }
    }

    /** A script's reset: a new node and identity, then a fresh run. */
    public TelephonyNode reset(TelephonyNode.Config newConfig, NodePort newNode) {
        lock.lock();
        try {
            TelephonyNode old = current.get();
            if (old != null) {
                old.close();
            }
            config = newConfig;
            node = newNode;
            return started(TelephonyNode.start(config, clock, owners, journal, node, 0, false));
        } finally {
            lock.unlock();
        }
    }
}
