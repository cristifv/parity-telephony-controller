package parity.telephonycontroller.session;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parity.telephonycontroller.journal.Journal;
import parity.telephonycontroller.model.Kinds.EndCause;
import parity.telephonycontroller.model.Kinds.EndedBy;
import parity.telephonycontroller.model.Kinds.LegState;
import parity.telephonycontroller.model.Kinds.MediaKind;
import parity.telephonycontroller.model.Kinds.RemoteMediaKind;
import parity.telephonycontroller.model.Kinds.Role;
import parity.telephonycontroller.model.Kinds.SessionEndReason;
import parity.telephonycontroller.model.Kinds.SessionState;
import parity.telephonycontroller.model.UuidV7;
import parity.telephonycontroller.node.NodePort;
import parity.telephonycontroller.node.NodePort.Channel;
import parity.telephonycontroller.node.NodePort.Ended;
import parity.telephonycontroller.owner.OwnerDirectory;
import parity.telephonycontroller.owner.OwnerDirectoryUnavailable;
import parity.telephonycontroller.owner.OwnerRecord;
import parity.telephonycontroller.session.Events.Arrived;
import parity.telephonycontroller.session.Events.Delivery;
import parity.telephonycontroller.session.Events.Event;
import parity.telephonycontroller.session.Events.Heartbeat;
import parity.telephonycontroller.session.Events.LegUpdate;
import parity.telephonycontroller.session.Events.NodeStatus;
import parity.telephonycontroller.session.Events.RemoteMediaChanged;
import parity.telephonycontroller.session.Events.SessionRef;
import parity.telephonycontroller.session.Requests.AddLeg;
import parity.telephonycontroller.session.Requests.Caller;
import parity.telephonycontroller.session.Requests.CommandKey;
import parity.telephonycontroller.session.Requests.Decline;
import parity.telephonycontroller.session.Requests.Owner;
import parity.telephonycontroller.session.Requests.RemoveLeg;
import parity.telephonycontroller.session.Session.Cached;
import parity.telephonycontroller.session.Session.Leg;
import parity.telephonycontroller.session.Session.Pending;
import parity.telephonycontroller.session.Views.Claimed;
import parity.telephonycontroller.session.Views.Declined;
import parity.telephonycontroller.session.Views.Gone;
import parity.telephonycontroller.session.Views.LegResult;
import parity.telephonycontroller.session.Views.LegView;
import parity.telephonycontroller.session.Views.SessionView;
import parity.telephonycontroller.time.NodeClock;

/**
 * One controller run (an incarnation) on one media node: sessions, fencing, delivery and the
 * journal (TC ADR 0001 §4). A restart is a new instance over the same node, journal and etcd: it
 * rebuilds every session from freeswitch's channels and CDR files and from the journal.
 *
 * <p>The order of checks for a command (§4.3): well-formed (the api layer), session, epoch, then
 * de-duplication, state and effects; the fence comes before de-duplication, so a fenced-off owner's
 * retry gets {@code stale_owner}, never a cached answer.
 */
public final class TelephonyNode implements NodePort.Listener {

    /** The node's identity, this run's incarnation, and its delivery limits. */
    public record Config(String nodeId, String version, Set<String> bins, String incarnation,
            List<String> capabilities, Limits limits) {

        public Config(String nodeId, String version, Set<String> bins, String incarnation, List<String> capabilities) {
            this(nodeId, version, bins, incarnation, capabilities, Limits.DEFAULT);
        }

        public Config withIncarnation(String other) {
            return new Config(nodeId, version, bins, other, capabilities, limits);
        }
    }

    /**
     * What unacknowledged events may hold (TC ADR 0001 §4.4): per session, at most
     * {@code eventsPerSession} events and none older than {@code maxEventAgeMs}; per node, about
     * {@code nodeBytes}. Past a limit the oldest go: IE sees the gap in {@code delivery_seq} and
     * resyncs with GetSession.
     *
     * <p>TC-036: a session that ends with no owner (a declined arrival, one past its deadline, a
     * caller gone before any claim) keeps its tombstone {@code ownerlessTombstoneMs}, and the node
     * keeps at most {@code ownerlessTombstones} of them (the oldest go first). Such a tombstone
     * holds no unacknowledged event, so its size is fixed: the count bounds the bytes.
     */
    public record Limits(int eventsPerSession, long maxEventAgeMs, long nodeBytes, int ownerlessTombstones,
            long ownerlessTombstoneMs) {
        public static final Limits DEFAULT = new Limits(256, 600_000, 64L << 20);

        public Limits(int eventsPerSession, long maxEventAgeMs, long nodeBytes) {
            this(eventsPerSession, maxEventAgeMs, nodeBytes, OWNERLESS_TOMBSTONES_MAX, OWNERLESS_TOMBSTONE_MS);
        }
    }

    /** Unacknowledged events dropped by the limits, since the process started (a metric). */
    private static final java.util.concurrent.atomic.AtomicLong DROPPED = new java.util.concurrent.atomic.AtomicLong();

    static final long REOFFER_MS = 1_000;
    static final long ARRIVAL_DEADLINE_MS = 5_000;
    static final long REDELIVER_MS = 2_000;
    static final long HEARTBEAT_MS = 2_000;
    static final long NODE_STATUS_MS = 5_000;
    static final long TOMBSTONE_AFTER_ACK_MS = 600_000;
    static final long TOMBSTONE_MAX_MS = 86_400_000;
    /** TC-036: an ownerless tombstone's life (covers IE's retries and a late GetSession). */
    static final long OWNERLESS_TOMBSTONE_MS = 120_000;
    /** TC-036: at most this many ownerless tombstones per node (about 1 KB each). */
    static final int OWNERLESS_TOMBSTONES_MAX = 20_000;
    static final long COMMAND_TTL_MS = 600_000;
    /** After a restart, parked arrivals are offered again only within the dialplan's safety (R1-4). */
    static final long REOFFER_MAX_AGE_MS = 10_000;
    static final int REJECT_STATUS = 503;
    /** A session's mailbox capacity for commands and node facts (TC ADR 0002 §2.1). */
    public static final int MAILBOX_CAPACITY = Mailbox.CAPACITY;

    private static final Logger LOG = LoggerFactory.getLogger(TelephonyNode.class);

    private final Config config;
    private final NodeClock clock;
    private final OwnerDirectory owners;
    private final Journal journal;
    private final NodePort node;
    private final ConcurrentHashMap<String, Session> sessions = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Session> byLeg = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Stream> streams = new ConcurrentHashMap<>();
    /** Commands and claims in flight: closing the run waits for them (it replaced step 5's life lock). */
    private final ReentrantLock turns = new ReentrantLock();
    private final Condition idle = turns.newCondition();
    private int inflight;
    /** Node events that came while the rebuild ran (subscribed before the snapshot, §2.4). */
    private final ReentrantLock bufferLock = new ReentrantLock();
    private final ConcurrentLinkedQueue<Runnable> heldEvents = new ConcurrentLinkedQueue<>();
    private boolean buffering = true;
    private static final java.util.concurrent.atomic.AtomicLong OVERFLOWS = new java.util.concurrent.atomic.AtomicLong();
    private static final java.util.concurrent.atomic.AtomicLong DUPLICATE_ARRIVALS = new java.util.concurrent.atomic.AtomicLong();
    private static final java.util.concurrent.atomic.AtomicLong EXPIRED_ATTEMPTS = new java.util.concurrent.atomic.AtomicLong();
    /** Fence raises that couldn't turn a leg's digit reporting off in parity-ms (PCI: alert on any). */
    private static final java.util.concurrent.atomic.AtomicLong DIGITS_OFF_FAILED = new java.util.concurrent.atomic.AtomicLong();
    private static final java.util.concurrent.atomic.AtomicLong RECORDINGS_STARTED = new java.util.concurrent.atomic.AtomicLong();
    private static final java.util.concurrent.atomic.AtomicLong RECORDINGS_STOPPED = new java.util.concurrent.atomic.AtomicLong();
    private static final java.util.concurrent.atomic.AtomicLong RECORDINGS_REFUSED = new java.util.concurrent.atomic.AtomicLong();
    /** TC-056: agent legs refused before they were dialled (another tenant's domain; a bad target). */
    private static final java.util.concurrent.atomic.AtomicLong AGENT_LEGS_REFUSED_TENANT = new java.util.concurrent.atomic.AtomicLong();
    private static final java.util.concurrent.atomic.AtomicLong AGENT_LEGS_REFUSED_TARGET = new java.util.concurrent.atomic.AtomicLong();
    private volatile long rebuildUntil;
    private volatile boolean rebuilt;
    private volatile boolean closed;
    private volatile boolean draining;
    private volatile int crashAfterLegs = -1;
    private final java.util.concurrent.atomic.AtomicLong bufferedBytes = new java.util.concurrent.atomic.AtomicLong();
    /** TC-036: ownerless tombstones in the order they ended (released ones leave from the head). */
    private final ConcurrentLinkedQueue<Session> ownerless = new ConcurrentLinkedQueue<>();
    private final java.util.concurrent.atomic.AtomicInteger ownerlessQueued = new java.util.concurrent.atomic.AtomicInteger();
    private static final java.util.concurrent.atomic.AtomicLong OWNERLESS_EVICTED = new java.util.concurrent.atomic.AtomicLong();
    private volatile CommandObserver observer = (interactionId, epoch, rpc) -> { };

    /** Tests: every successful command, in each session's writer order (called under its lock). */
    @FunctionalInterface
    public interface CommandObserver {
        void committed(String interactionId, long epoch, String rpc);
    }

    private record Stream(String replicaId, String bin, StreamSink sink) {}

    /** A fault for tests: the process dies in the middle of raising a fence. */
    public static final class SimulatedCrash extends RuntimeException {
        private static final long serialVersionUID = 1L;

        SimulatedCrash() {
            super("simulated crash", null, false, false);
        }
    }

    /** A command's effects, run under the session's lock once the checks passed. */
    @FunctionalInterface
    private interface Body<R> {
        R run(Session s) throws TelephonyException;
    }

    private TelephonyNode(Config config, NodeClock clock, OwnerDirectory owners, Journal journal, NodePort node,
            boolean draining) {
        this.config = config;
        this.clock = clock;
        this.owners = owners;
        this.journal = journal;
        this.node = node;
        this.draining = draining;
    }

    /**
     * Starts a run: rebuilds from the node and the journal, then answers {@code rebuilding} for
     * {@code rebuildMs} (a real rebuild's duration; the simulated node rebuilds at once).
     */
    public static TelephonyNode start(Config config, NodeClock clock, OwnerDirectory owners, Journal journal,
            NodePort node, long rebuildMs, boolean draining) {
        TelephonyNode t = new TelephonyNode(config, clock, owners, journal, node, draining);
        // Subscribe first, then snapshot (§2.4): what the node reports meanwhile waits in the buffer.
        node.listen(t);
        t.rebuild();
        t.drainBuffer();
        t.rebuildUntil = clock.nowMs() + rebuildMs;
        t.every(HEARTBEAT_MS, t::heartbeat);
        t.every(NODE_STATUS_MS, t::broadcastNodeStatus);
        t.every(REDELIVER_MS, t::redeliver);
        if (rebuildMs > 0) {
            clock.schedule(t.rebuildUntil, t::finishRebuild);
        } else {
            t.finishRebuild();
        }
        return t;
    }

    /**
     * Ends this run, once the commands and claims in flight have finished (a restart between
     * commands): every stream ends, timers stop acting. The node and journal stay.
     */
    public void close() {
        turns.lock();
        try {
            closed = true;
            while (inflight > 0) {
                idle.awaitUninterruptibly();
            }
        } finally {
            turns.unlock();
        }
        for (Stream s : streams.values()) {
            s.sink.close(Code.REBUILDING);
        }
        streams.clear();
    }

    public String incarnation() {
        return config.incarnation();
    }

    public boolean draining() {
        return draining;
    }

    /** A node drain: new sessions are refused, existing ones served (F3). */
    public void drain() {
        draining = true;
        broadcastNodeStatus();
    }

    /** Tests: sees every successful command. */
    public void observe(CommandObserver o) {
        observer = o;
    }

    /** Test hook: the next fence raise dies after writing this many legs. */
    public void crashAfterLegs(int n) {
        crashAfterLegs = n;
    }

    // ---- Watch and Acknowledge -------------------------------------------------------------

    /** Opens a replica's stream: the newest per replica wins; {@code node_status} first, then replays. */
    public void watch(Caller caller, String replicaId, String bin, StreamSink sink) throws TelephonyException {
        if (closed) {
            throw new TelephonyException(Code.REBUILDING);
        }
        if (!config.bins().contains(bin) || !bin.equals(caller.bin())) {
            throw new TelephonyException(Code.IDENTITY_MISMATCH);
        }
        try {
            if (!owners.replicaRegistered(bin, replicaId)) {
                throw new TelephonyException(Code.IDENTITY_MISMATCH);
            }
        } catch (OwnerDirectoryUnavailable e) {
            throw new TelephonyException(Code.OWNER_CHECK_UNAVAILABLE);
        }
        Stream stream = new Stream(replicaId, bin, sink);
        sink.send(nodeStatusDelivery());
        Stream old = streams.put(replicaId, stream);
        if (old != null) {
            old.sink.close(Code.STREAM_SUPERSEDED);
        }
        if (rebuilt) {
            attach(stream);
        }
    }

    /** Test hook: the network drops a replica's stream. */
    public void dropStream(String replicaId) {
        Stream s = streams.get(replicaId);
        if (s != null) {
            s.sink.drop();
            streamClosed(replicaId, s.sink);
        }
    }

    /** The client closed its stream. */
    public void streamClosed(String replicaId, StreamSink sink) {
        Stream s = streams.get(replicaId);
        if (s != null && same(s.sink, sink) && streams.remove(replicaId, s)) {
            streamGone(replicaId);
        }
    }

    /** Batched acks; returns how many were ignored. */
    public int acknowledge(Caller caller, List<Owner> acks) throws TelephonyException {
        ready();
        zone(caller);
        int ignored = 0;
        for (Owner a : acks) {
            Session s = sessions.get(key(a.tenantId(), a.interactionId()));
            if (s == null || s.provisional || !Boolean.TRUE.equals(withLock(s, x -> ackFromAnyone(x, caller, a)))) {
                ignored++;
            }
        }
        return ignored;
    }

    private boolean ackFromAnyone(Session s, Caller caller, Owner a) {
        if (!config.incarnation().equals(a.ackedIncarnation())) {
            return false;
        }
        boolean ok = s.fence == 0
                ? a.epoch() == 0 && Objects.equals(a.replicaId(), s.offeredReplica)
                        && Objects.equals(caller.bin(), s.arrivalBin)
                : a.epoch() == s.fence && Objects.equals(a.replicaId(), s.ownerReplica)
                        && Objects.equals(caller.bin(), s.ownerBin);
        if (ok) {
            if (s.fence == 0 && s.arrivedSeq > 0 && a.ackedSeq() >= s.arrivedSeq) {
                s.arrivalAcked = true;
            }
            removeAcked(s, a.ackedSeq());
        }
        return ok;
    }

    // ---- Claims and snapshots ----------------------------------------------------------------

    /** A new owner (or the same one again): verify, raise the fences, rebind; the snapshot. */
    public Claimed claim(Caller caller, Owner o) throws TelephonyException {
        enter();
        try {
            return claimLive(caller, o);
        } finally {
            leave();
        }
    }

    private Claimed claimLive(Caller caller, Owner o) throws TelephonyException {
        ready();
        zone(caller);
        Session s = visible(key(o.tenantId(), o.interactionId()));
        return locked(s, x -> {
            if (x.retired) {
                throw new TelephonyException(Code.SESSION_UNKNOWN);
            }
            ownerBin(caller, x, o);
            boolean existing = x.fence > 0 && o.epoch() == x.fence && Objects.equals(o.replicaId(), x.ownerReplica);
            verify(caller, x, o);
            if (existing) {
                raise(x, o.epoch(), o.replicaId(), x.ownerBin);
            }
            ack(x, o);
            observer.committed(x.interactionId, o.epoch(), "ClaimSession");
            return new Claimed(view(x), existing);
        });
    }

    /** A snapshot, with no fence change; only for the session's bin. */
    public SessionView get(Caller caller, String tenantId, String interactionId) throws TelephonyException {
        ready();
        zone(caller);
        Session s = visible(key(tenantId, interactionId));
        return locked(s, x -> {
            String bin = x.ownerBin != null ? x.ownerBin : x.arrivalBin;
            if (!Objects.equals(bin, caller.bin())) {
                throw new TelephonyException(Code.SESSION_UNKNOWN);
            }
            return view(x);
        });
    }

    /** {@code DeclineArrival(REFUSED)}: only the replica the current attempt went to. */
    public Declined decline(Caller caller, Decline d) throws TelephonyException {
        ready();
        zone(caller);
        Session s = visible(key(d.tenantId(), d.interactionId()));
        return locked(s, x -> {
            if (d.replicaId().equals(x.declinedBy) && x.declinedStatus == d.sipStatus()) {
                return new Declined(true, true);
            }
            if (x.state != SessionState.ARRIVING || x.claimed || !d.replicaId().equals(x.offeredReplica)
                    || !Objects.equals(caller.bin(), x.arrivalBin)) {
                throw new TelephonyException(Code.ARRIVAL_CLOSED);
            }
            Leg c = x.customer();
            if (c == null || !c.legId.equals(d.legId())) {
                throw new TelephonyException(Code.LEG_UNKNOWN);
            }
            Ended e = nodeCall(() -> node.reject(c.legId, EndCause.DECLINED, d.sipStatus(), EndedBy.COMMAND));
            x.declinedBy = d.replicaId();
            x.declinedStatus = d.sipStatus();
            closeOffer(x);
            legsEnded(x, e == null ? List.of() : List.of(e), SessionEndReason.ARRIVAL_DECLINED, d.sipStatus());
            return new Declined(false, true);
        });
    }

    // ---- Commands ------------------------------------------------------------------------------

    public boolean ring(Caller caller, Owner o, CommandKey k, String legId) throws TelephonyException {
        return command(caller, o, k, false, s -> {
            Leg l = leg(s, legId);
            if (l.state == LegState.RINGING && l.inbound) {
                return true;
            }
            notEnded(s);
            if (l.state != LegState.PARKED) {
                throw new TelephonyException(Code.INVALID_STATE);
            }
            nodeRun(() -> node.ring(legId));
            l.state = LegState.RINGING;
            emit(s, legUpdate(l));
            return false;
        });
    }

    public boolean answer(Caller caller, Owner o, CommandKey k, String legId) throws TelephonyException {
        return command(caller, o, k, false, s -> {
            Leg l = leg(s, legId);
            if (l.state == LegState.ANSWERED && l.inbound) {
                return true;
            }
            notEnded(s);
            if (!l.inbound || (l.state != LegState.PARKED && l.state != LegState.RINGING)) {
                throw new TelephonyException(Code.INVALID_STATE);
            }
            if (node.freeLegs() < 1) {
                throw new TelephonyException(Code.NO_CAPACITY);
            }
            long at = nodeCall(() -> node.answer(legId, s.fence));
            l.state = LegState.ANSWERED;
            l.answeredAtMs = at;
            s.state = SessionState.ACTIVE;
            s.media = MediaKind.CALL;
            emit(s, legUpdate(l));
            return false;
        });
    }

    public boolean reject(Caller caller, Owner o, CommandKey k, String legId, int sipStatus)
            throws TelephonyException {
        return command(caller, o, k, false, s -> {
            Leg l = leg(s, legId);
            if (l.state == LegState.ENDED && l.endedBy == EndedBy.COMMAND && l.cause == EndCause.DECLINED
                    && l.sipStatus == sipStatus) {
                return true;
            }
            notEnded(s);
            if (!l.inbound || (l.state != LegState.PARKED && l.state != LegState.RINGING)) {
                throw new TelephonyException(Code.INVALID_STATE);
            }
            Ended e = nodeCall(() -> node.reject(legId, EndCause.DECLINED, sipStatus, EndedBy.COMMAND));
            legsEnded(s, e == null ? List.of() : List.of(e), SessionEndReason.ENDED_BY_OWNER, sipStatus);
            return false;
        });
    }

    // ---- Step 4: a leg's media (TC ADR 0003 D6) -----------------------------------------------------

    /** A live, answered leg of a session that has media. */
    private Leg mediaLeg(Session s, String legId) throws TelephonyException {
        Leg l = leg(s, legId);
        notEnded(s);
        if (l.state != LegState.ANSWERED || s.media == MediaKind.NONE) {
            throw new TelephonyException(Code.INVALID_STATE);
        }
        return l;
    }

    /** TC's id for one play sent to the media (a fresh one per send, so no two attempts share it). */
    private static String wireId() {
        return "tc-" + UuidV7.create();
    }

    /** The prompt's file under parity-ms's prompt directory: the tenant's own (PLAN: prompts per tenant). */
    private static String promptFile(Session s, String promptId) {
        return s.tenantId + "/" + promptId + ".wav";
    }

    /**
     * Plays a tenant prompt into a leg (replacing one playing). Repeat-safe by play id. Answers once
     * the play started (an IE without {@code result_events}): {@link #play(Caller, Owner, CommandKey,
     * String, String, String, boolean, boolean)} and {@link #awaitStart}.
     */
    public boolean play(Caller caller, Owner o, CommandKey k, String legId, String playId, String promptId, boolean loop)
            throws TelephonyException {
        Played p = play(caller, o, k, legId, playId, promptId, loop, false);
        if (p.started() != null) {
            awaitStart(p, o, legId, playId, LEGACY_PLAY_WAIT_MS);
        }
        return p.existing();
    }

    /**
     * What Play did: {@code existing} for a repeat; {@code started}: for an IE without
     * {@code result_events} on an asynchronous media, the start its answer waits for
     * ({@link #awaitStart}); null: answer now.
     */
    public record Played(boolean existing, java.util.concurrent.CompletableFuture<Code> started) {}

    /** How long the one-argument {@code play} (tests, an IE without a deadline) waits for a start. */
    static final long LEGACY_PLAY_WAIT_MS = 300;

    /**
     * Plays a tenant prompt into a leg. With an asynchronous media (the play saga, TC ADR 0004) the
     * play is sent under one id for its whole life and TC keeps its saga: with
     * {@code resultEvents}, IE hears PlayStarted then PlayFinished, or PlayFailed; without, the
     * caller waits for the start ({@link #awaitStart}). A media that doesn't play asynchronously
     * gets today's synchronous play.
     */
    public Played play(Caller caller, Owner o, CommandKey k, String legId, String playId, String promptId, boolean loop,
            boolean resultEvents) throws TelephonyException {
        if (!node.asyncPlays()) {
            return new Played(playSync(caller, o, k, legId, playId, promptId, loop, resultEvents), null);
        }
        return command(caller, o, k, false, s -> {
            Leg l = leg(s, legId);
            if (l.playIds.contains(playId)) {
                Session.Play p = byPlayId(l, playId);
                if (p != null && p.wanted && p.state == Session.PlayState.REQUESTED) {
                    // IE's own retry of a send whose answer TC lost: the same id again, now.
                    sendPlay(s, l, p);
                }
                return new Played(true, p == null ? null : p.started);
            }
            mediaLeg(s, legId);
            Session.Play p = new Session.Play(playId, promptFile(s, promptId), loop);
            p.wireId = wireId();
            p.resultEvents = resultEvents;
            p.started = resultEvents ? null : new java.util.concurrent.CompletableFuture<>();
            supersede(s, l);
            l.playIds.add(playId);
            l.plays.addLast(p);
            if (sendPlay(s, l, p) == NodePort.PlaySent.REFUSED) {
                endPlay(l, p);
                l.playIds.remove(playId);
                throw new TelephonyException(Code.PARITY_MS_UNAVAILABLE);
            }
            return new Played(false, p.started);
        });
    }

    /**
     * The answer of an IE without {@code result_events} (the play saga's legacy path): waits, outside
     * the session's turn, for the play to start or fail. When {@code waitMs} (the caller's deadline)
     * passes first, TC compensates (the play is called off, and stopped if it starts later) and the
     * caller hears {@code parity_ms_unavailable}; a start that won the race answers OK.
     */
    public void awaitStart(Played played, Owner o, String legId, String playId, long waitMs) throws TelephonyException {
        Code c;
        try {
            c = played.started().get(Math.max(1, waitMs), java.util.concurrent.TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.TimeoutException e) {
            c = callerGaveUp(o, legId, playId);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            c = callerGaveUp(o, legId, playId);
        } catch (java.util.concurrent.ExecutionException e) {
            c = Code.INTERNAL;
        }
        if (c != null) {
            throw new TelephonyException(c);
        }
    }

    /** The legacy caller stops waiting: in the session's turn, the play's start either won (null) or is called off. */
    private Code callerGaveUp(Owner o, String legId, String playId) {
        Session s = sessions.get(key(o.tenantId(), o.interactionId()));
        if (s == null) {
            return Code.PARITY_MS_UNAVAILABLE;
        }
        return withLock(s, x -> {
            Leg l = x.legs.get(legId);
            Session.Play p = l == null ? null : byPlayId(l, playId);
            if (p == null || p.started == null) {
                return Code.PARITY_MS_UNAVAILABLE;
            }
            if (!p.started.isDone()) {
                p.reported = true;
                p.started.complete(Code.PARITY_MS_UNAVAILABLE);
                compensate(x, l, p, "caller_gave_up");
            }
            return p.started.getNow(Code.PARITY_MS_UNAVAILABLE);
        });
    }

    /** Plays as before the saga (a media without {@code play_async}); with {@code resultEvents} its outcome is also an event. */
    private boolean playSync(Caller caller, Owner o, CommandKey k, String legId, String playId, String promptId,
            boolean loop, boolean resultEvents) throws TelephonyException {
        return command(caller, o, k, false, s -> {
            Leg l = leg(s, legId);
            if (l.playIds.contains(playId)) {
                return true;
            }
            mediaLeg(s, legId);
            Session.Play p = new Session.Play(playId, promptFile(s, promptId), loop);
            p.resultEvents = resultEvents;
            // A loop repeats in parity-ms itself, with no gap (R7), and parity-ms says so; an older
            // one plays it once and reports it completed, and it is sent again (playFinished).
            p.wireId = wireId();
            try {
                p.nodeLoops = node.play(legId, p.file, loop, p.wireId, s.fence);
            } catch (NodePort.Unavailable e) {
                if (!resultEvents || e.part() != NodePort.Part.PROMPT) {
                    throw new TelephonyException(nodeCode(e));
                }
                // With result_events the answer means "taken"; the prompt's failure is an event.
                l.playIds.add(playId);
                playFailedCount("PROMPT_UNKNOWN");
                emit(s, new Events.PlayFailed(legId, playId, "PROMPT_UNKNOWN"));
                return false;
            }
            p.state = Session.PlayState.STARTED;
            l.playIds.add(playId);
            l.plays.addLast(p);
            if (resultEvents) {
                emit(s, new Events.PlayStarted(legId, playId));
            }
            return false;
        });
    }

    /** What StopPlay found. */
    public record Stopped(boolean existing, boolean alreadyGone) {}

    /**
     * Stops a play; one that already ended is {@code already_gone}. With an asynchronous media, one
     * not started yet is called off (its loading cancelled, a later start stopped) and
     * PlayFinished(STOPPED) follows at once.
     */
    public Stopped stopPlay(Caller caller, Owner o, CommandKey k, String legId, String playId) throws TelephonyException {
        if (!node.asyncPlays()) {
            return stopPlaySync(caller, o, k, legId, playId);
        }
        return command(caller, o, k, false, s -> {
            Leg l = leg(s, legId);
            Session.Play p = byPlayId(l, playId);
            if (p == null || !p.wanted || p.reported || l.state == LegState.ENDED) {
                return new Stopped(false, true);
            }
            if (p.stopping) {
                return new Stopped(true, false);
            }
            p.stopping = true;
            if (p.state == Session.PlayState.STARTED) {
                nodeRun(() -> node.stopPlay(legId, p.wireId, s.fence));
                return new Stopped(false, false);
            }
            compensate(s, l, p, "stopped");
            reportFinished(s, l, p, "STOPPED", 0);
            return new Stopped(false, false);
        });
    }

    private Stopped stopPlaySync(Caller caller, Owner o, CommandKey k, String legId, String playId)
            throws TelephonyException {
        return command(caller, o, k, false, s -> {
            Leg l = leg(s, legId);
            Session.Play current = l.plays.peekLast();
            if (current == null || !current.playId.equals(playId) || l.state == LegState.ENDED) {
                return new Stopped(false, true);
            }
            if (current.stopping) {
                return new Stopped(true, false);
            }
            nodeCall(() -> node.stopPlay(legId, s.fence));
            current.stopping = true;
            return new Stopped(false, false);
        });
    }

    // ---- The play saga (TC ADR 0004) ----------------------------------------------------------------

    /** Backoff before each retry of a send whose answer was lost (the same id each time), with jitter. */
    private static final long[] PLAY_RETRY_MS = {100, 400, 1_600};

    /** Why TC sent {@code stop_play} for a play it no longer wants (the metric's reasons). */
    public static final List<String> COMPENSATIONS = List.of("stopped", "leg_ended", "hold_off", "superseded",
            "caller_gave_up", "gave_up", "late_start", "untracked");

    /** Why a play ended without starting, as IE hears it (and hold music, counted alike). */
    public static final List<String> PLAY_FAILURES = List.of("PROMPT_UNKNOWN", "PARITY_MS_UNAVAILABLE", "LEG_GONE",
            "REPLACED", "CANCELLED");

    private static final Map<String, java.util.concurrent.atomic.AtomicLong> PLAYS_COMPENSATED = counters(COMPENSATIONS);
    private static final Map<String, java.util.concurrent.atomic.AtomicLong> PLAYS_FAILED = counters(PLAY_FAILURES);
    private static final java.util.concurrent.atomic.AtomicLong PLAYS_RETRIED = new java.util.concurrent.atomic.AtomicLong();

    private static Map<String, java.util.concurrent.atomic.AtomicLong> counters(List<String> names) {
        Map<String, java.util.concurrent.atomic.AtomicLong> m = new java.util.LinkedHashMap<>();
        names.forEach(n -> m.put(n, new java.util.concurrent.atomic.AtomicLong()));
        return java.util.Collections.unmodifiableMap(m);
    }

    /** {@code stop_play} sent for a play TC no longer wants or that started after it was given up (a metric, by reason). */
    public static long playsCompensated(String reason) {
        return PLAYS_COMPENSATED.get(reason).get();
    }

    /** Plays that ended without starting (a metric, by IE's reason). */
    public static long playsFailed(String reason) {
        return PLAYS_FAILED.get(reason).get();
    }

    /** Sends repeated after a lost answer (a metric). */
    public static long playsRetried() {
        return PLAYS_RETRIED.get();
    }

    private static void playFailedCount(String reason) {
        PLAYS_FAILED.get(reason).incrementAndGet();
    }

    /** The leg's tracked play IE gave {@code playId} (the latest), or null. */
    private static Session.Play byPlayId(Leg l, String playId) {
        Session.Play found = null;
        for (Session.Play p : l.plays) {
            if (p.playId.equals(playId)) {
                found = p;
            }
        }
        return found;
    }

    /** The leg's tracked play sent under {@code wireId}, or null. */
    private static Session.Play byWireId(Leg l, String wireId) {
        for (Session.Play p : l.plays) {
            if (wireId.equals(p.wireId)) {
                return p;
            }
        }
        return null;
    }

    /** The play leaves the leg's list; its id is remembered a while, so a late repeat of its events is no stranger. */
    private static void endPlay(Leg l, Session.Play p) {
        l.plays.remove(p);
        cancelRetry(p);
        if (p.wireId != null) {
            if (l.endedPlays.size() >= 16) {
                l.endedPlays.pollFirst();
            }
            l.endedPlays.addLast(p.wireId);
        }
    }

    private static void cancelRetry(Session.Play p) {
        if (p.retryTimer != null) {
            p.retryTimer.cancel();
            p.retryTimer = null;
        }
    }

    /**
     * Sends the play under its id (again, for a retry). Answered: ACCEPTED (or EXISTING: the media
     * had it) and the saga waits for its start or failure; REFUSED: nothing follows. No answer: the
     * play stays REQUESTED and is sent again later (the same id), never taken for failed.
     */
    private NodePort.PlaySent sendPlay(Session s, Leg l, Session.Play p) {
        NodePort.PlaySent sent;
        try {
            sent = node.playAsync(l.legId, p.file, p.loop, p.wireId, s.fence);
        } catch (NodePort.Unavailable e) {
            retryPlayLater(s, l, p);
            return null;
        }
        if (sent != NodePort.PlaySent.REFUSED && p.state == Session.PlayState.REQUESTED) {
            p.state = Session.PlayState.ACCEPTED;
        }
        p.nodeLoops = node.loopsNatively();
        return sent;
    }

    /** A timer for the next send of a play whose answer was lost; past the last, TC gives up by compensating. */
    private void retryPlayLater(Session s, Leg l, Session.Play p) {
        if (p.retries >= PLAY_RETRY_MS.length) {
            compensate(s, l, p, "gave_up");
            reportFailed(s, l, p, "PARITY_MS_UNAVAILABLE");
            return;
        }
        long base = PLAY_RETRY_MS[p.retries++];
        long delay = base / 2 + java.util.concurrent.ThreadLocalRandom.current().nextLong(base / 2 + 1);
        PLAYS_RETRIED.incrementAndGet();
        cancelRetry(p);
        p.retryTimer = clock.schedule(clock.nowMs() + delay, () -> withLock(s, x -> {
            p.retryTimer = null;
            if (!closed && p.wanted && p.state == Session.PlayState.REQUESTED && l.state != LegState.ENDED
                    && l.plays.contains(p) && sendPlay(x, l, p) == NodePort.PlaySent.REFUSED) {
                endPlay(l, p);
                reportFailed(x, l, p, "PARITY_MS_UNAVAILABLE");
            }
            return null;
        }));
    }

    /**
     * A newer play on the leg: the ones not started yet are no longer wanted (the media replaces a
     * loading one itself; one whose send went unanswered is stopped by id, so that it never starts);
     * IE hears PlayFailed(REPLACED) for each. One playing is replaced when the newer one starts.
     */
    private void supersede(Session s, Leg l) {
        for (Session.Play q : List.copyOf(l.plays)) {
            if (q.wanted && q.state != Session.PlayState.STARTED) {
                if (q.state == Session.PlayState.REQUESTED) {
                    compensate(s, l, q, "superseded");
                } else {
                    q.wanted = false;
                    cancelRetry(q);
                }
                if (q.hold) {
                    q.reported = true; // replaced, not failed: nothing owed to anyone
                } else {
                    reportFailed(s, l, q, "REPLACED");
                }
            }
        }
    }

    /**
     * TC no longer wants {@code p} (or it started after TC gave up on it): {@code stop_play} by its id
     * now, best effort. Its tombstone stays until its end, so a start that comes later is stopped then.
     */
    private void compensate(Session s, Leg l, Session.Play p, String reason) {
        p.wanted = false;
        cancelRetry(p);
        PLAYS_COMPENSATED.get(reason).incrementAndGet();
        LOG.atInfo().addKeyValue("event", "play.compensated").addKeyValue("tenant_id", s.tenantId)
                .addKeyValue("interaction_id", s.interactionId).addKeyValue("leg_id", l.legId)
                .addKeyValue("play_wire_id", p.wireId).addKeyValue("reason", reason)
                .log("a play TC no longer wants: stopped by its id");
        try {
            node.stopPlay(l.legId, p.wireId, s.fence);
        } catch (NodePort.Unavailable e) {
            LOG.atWarn().addKeyValue("event", "play.compensation_deferred").addKeyValue("tenant_id", s.tenantId)
                    .addKeyValue("interaction_id", s.interactionId).addKeyValue("leg_id", l.legId)
                    .addKeyValue("reason", reason)
                    .log("parity-ms didn't answer the stop: the play is stopped if it starts later");
        }
    }

    /** The play's start reaches its owner: PlayStarted, or the waiting caller's answer. */
    private void reportStarted(Session s, Leg l, Session.Play p) {
        if (p.hold || p.reported) {
            return;
        }
        if (p.resultEvents) {
            emit(s, new Events.PlayStarted(l.legId, p.playId));
        } else if (p.started != null) {
            p.started.complete(null);
        }
    }

    /** A play ended without starting: once, PlayFailed (or the waiting caller's error); hold music is only counted. */
    private void reportFailed(Session s, Leg l, Session.Play p, String reason) {
        if (p.reported) {
            return;
        }
        p.reported = true;
        playFailedCount(reason);
        if (p.hold) {
            LOG.atWarn().addKeyValue("event", "hold.music_failed").addKeyValue("tenant_id", s.tenantId)
                    .addKeyValue("interaction_id", s.interactionId).addKeyValue("leg_id", l.legId)
                    .addKeyValue("reason", reason).log("the hold music didn't start: the hold stands");
            return;
        }
        if (p.resultEvents) {
            emit(s, new Events.PlayFailed(l.legId, p.playId, reason));
        } else if (p.started != null) {
            p.started.complete("PROMPT_UNKNOWN".equals(reason) ? Code.PROMPT_UNKNOWN : Code.PARITY_MS_UNAVAILABLE);
        }
    }

    /** A play ended: once, PlayFinished (hold music: nothing). */
    private void reportFinished(Session s, Leg l, Session.Play p, String reason, long playedMs) {
        if (p.reported) {
            return;
        }
        p.reported = true;
        if (p.started != null && !p.started.isDone()) {
            p.started.complete(null);
        }
        if (!p.hold) {
            emit(s, new Events.PlayFinished(l.legId, p.playId, reason, playedMs));
        }
    }

    /** parity-ms's reason for a play that never started, as IE hears it. */
    private static String failReason(String reason) {
        return switch (reason) {
            case "prompt_not_found", "bad_prompt" -> "PROMPT_UNKNOWN";
            case "leg_gone" -> "LEG_GONE";
            case "replaced" -> "REPLACED";
            case "cancelled" -> "CANCELLED";
            default -> "PARITY_MS_UNAVAILABLE";
        };
    }

    /** A leg ended: its plays end with it in the media; a caller still waiting hears so. */
    private static void legPlaysEnded(Leg l) {
        for (Session.Play p : l.plays) {
            cancelRetry(p);
            if (p.started != null && !p.started.isDone()) {
                p.started.complete(Code.PARITY_MS_UNAVAILABLE);
            }
        }
        l.plays.clear();
    }

    /** Silences what a leg sends (or not); returns whether that changed it. */
    public boolean setMute(Caller caller, Owner o, CommandKey k, String legId, boolean muted) throws TelephonyException {
        return command(caller, o, k, false, s -> {
            Leg l = mediaLeg(s, legId);
            if (l.muted == muted) {
                return false;
            }
            nodeCall(() -> node.mute(legId, muted, s.fence));
            l.muted = muted;
            return true;
        });
    }

    /** Sends digits to a leg's endpoint. PCI: the digits are never logged or kept. */
    public boolean sendDtmf(Caller caller, Owner o, CommandKey k, String legId, String digits) throws TelephonyException {
        return command(caller, o, k, false, s -> {
            mediaLeg(s, legId);
            nodeRun(() -> node.sendDtmf(legId, digits, s.fence));
            return false;
        });
    }

    /**
     * A leg's digit collection: which digits become {@code DtmfReceived} (OFF, MASKED or ON), under
     * {@code collectionId}, back to OFF after {@code maxDurationS} (PCI: ON only while collecting).
     */
    public boolean setDigitReporting(Caller caller, Owner o, CommandKey k, String legId, String mode,
            String collectionId, int maxDurationS) throws TelephonyException {
        return command(caller, o, k, false, s -> {
            Leg l = mediaLeg(s, legId);
            if (l.digitMode.equals(mode) && l.collectionId.equals(collectionId)) {
                return true;
            }
            nodeRun(() -> node.reportDigits(legId, mode.toLowerCase(java.util.Locale.ROOT), s.fence));
            l.digitMode = mode;
            l.collectionId = collectionId;
            if (l.digitTimer != null) {
                l.digitTimer.cancel();
                l.digitTimer = null;
            }
            if (!"OFF".equals(mode)) {
                l.digitTimer = clock.schedule(clock.nowMs() + maxDurationS * 1000L,
                        () -> digitsTimedOut(s, legId, collectionId));
            }
            return false;
        });
    }

    private void digitsTimedOut(Session s, String legId, String collectionId) {
        withLock(s, x -> {
            Leg l = x.legs.get(legId);
            // A collection a fence raise already ended (TC's side OFF) still turns parity-ms off here.
            if (closed || l == null || l.state == LegState.ENDED
                    || (!l.collectionId.equals(collectionId) && !l.collectionId.isEmpty())) {
                return null;
            }
            try {
                node.reportDigits(legId, "off", x.fence);
                l.digitMode = "OFF";
                l.digitTimer = null;
            } catch (NodePort.Unavailable e) {
                LOG.atWarn().addKeyValue("event", "digits.off_failed").addKeyValue("tenant_id", x.tenantId)
                        .addKeyValue("interaction_id", x.interactionId).addKeyValue("leg_id", legId)
                        .log("can't turn a leg's digit reporting off at its deadline");
            }
            return null;
        });
    }

    /**
     * Hold (ADR 0003 D6): both parties muted toward each other, the customer hearing the music
     * prompt in a loop; off unmutes and stops the music. Returns whether it changed. With an
     * asynchronous media the music is a play saga of TC's own (ADR 0004 D7): answered at once, never
     * reported to IE; off (or a newer play) calls it off, and a start after that is stopped.
     */
    public boolean setHold(Caller caller, Owner o, CommandKey k, boolean on, String musicPromptId)
            throws TelephonyException {
        return command(caller, o, k, false, s -> {
            notEnded(s);
            if (s.onHold == on) {
                return false;
            }
            Leg customer = s.customer();
            if (customer == null || customer.state != LegState.ANSWERED || s.media == MediaKind.NONE) {
                throw new TelephonyException(Code.INVALID_STATE);
            }
            for (Leg l : s.liveLegs()) {
                if (l.state == LegState.ANSWERED) {
                    nodeCall(() -> node.mute(l.legId, on, s.fence));
                    l.muted = on;
                }
            }
            if (on && musicPromptId != null && !musicPromptId.isEmpty()) {
                Session.Play p = new Session.Play("hold-" + UuidV7.create(), promptFile(s, musicPromptId), true);
                p.hold = true;
                p.wireId = wireId();
                if (node.asyncPlays()) {
                    supersede(s, customer);
                    customer.plays.addLast(p);
                    if (sendPlay(s, customer, p) == NodePort.PlaySent.REFUSED) {
                        endPlay(customer, p);
                        reportFailed(s, customer, p, "PARITY_MS_UNAVAILABLE");
                    }
                } else {
                    p.nodeLoops = nodeCall(() -> node.play(customer.legId, p.file, true, p.wireId, s.fence));
                    p.state = Session.PlayState.STARTED;
                    customer.plays.addLast(p);
                }
            } else if (!on && node.asyncPlays()) {
                Session.Play p = holdPlay(customer);
                if (p != null) {
                    p.stopping = true;
                    if (p.state == Session.PlayState.STARTED) {
                        nodeRun(() -> node.stopPlay(customer.legId, p.wireId, s.fence));
                    } else {
                        p.reported = true; // called off by TC itself: no failure to count
                        compensate(s, customer, p, "hold_off");
                    }
                }
            } else if (!on) {
                Session.Play p = customer.plays.peekLast();
                if (p != null && p.hold) {
                    nodeCall(() -> node.stopPlay(customer.legId, s.fence));
                    p.stopping = true;
                }
            }
            s.onHold = on;
            return true;
        });
    }

    /** The leg's hold music TC still wants (the latest), or null. */
    private static Session.Play holdPlay(Leg l) {
        Session.Play found = null;
        for (Session.Play p : l.plays) {
            if (p.hold && p.wanted && !p.stopping) {
                found = p;
            }
        }
        return found;
    }

    /** The actions each role may bind (the contract; parity-ms P5). */
    private static final java.util.Map<Role, java.util.Set<String>> DTMF_ROLE_ACTIONS = java.util.Map.of(
            Role.AGENT, java.util.Set.of("MUTE", "UNMUTE", "MUTE_TOGGLE", "RECORDING_PAUSE", "RECORDING_RESUME",
                    "RECORDING_TOGGLE"),
            Role.SUPERVISOR, java.util.Set.of("MUTE", "UNMUTE", "MUTE_TOGGLE", "LISTEN", "WHISPER", "BARGE"),
            Role.CUSTOMER, java.util.Set.of());

    /** Refused unless every code is allowed by the leg's role, and none is a prefix of another. */
    private static void checkDtmfBindings(Role role, List<NodePort.DtmfBinding> bindings) throws TelephonyException {
        java.util.Set<String> allowed = DTMF_ROLE_ACTIONS.getOrDefault(role, java.util.Set.of());
        for (int i = 0; i < bindings.size(); i++) {
            if (!allowed.contains(bindings.get(i).action())) {
                throw new TelephonyException(Code.INVALID_REQUEST);
            }
            for (int j = 0; j < bindings.size(); j++) {
                if (i != j && bindings.get(j).sequence().startsWith(bindings.get(i).sequence())) {
                    throw new TelephonyException(Code.INVALID_REQUEST);
                }
            }
        }
    }

    private static final List<String> FACETS = List.of("MUTE", "RECORDING", "MODE");

    /**
     * What to pin and to free in parity-ms for a leg whose locks become {@code locks}: MUTE and MODE
     * are the leg's; RECORDING is the session's (one pause, one recording state, PCI), so it stays
     * pinned while any live leg of the session asks for it (another leg's codes never free it).
     */
    private static List<List<String>> lockChanges(Session s, String legId, java.util.Set<String> locks) {
        boolean recording = locks.contains("RECORDING") || s.liveLegs().stream()
                .anyMatch(o -> !o.legId.equals(legId) && o.locks.contains("RECORDING"));
        List<String> lock = new ArrayList<>();
        List<String> unlock = new ArrayList<>();
        for (String f : FACETS) {
            boolean on = f.equals("RECORDING") ? recording : locks.contains(f);
            (on ? lock : unlock).add(f);
        }
        return List.of(List.copyOf(lock), List.copyOf(unlock));
    }

    /**
     * A leg's DTMF action codes (parity-ms P5): at most 8, each {@code *} and 1–7 keys, none a
     * prefix of another, each allowed by the leg's role; empty turns them off. Locks pin facets
     * against them: MUTE and MODE the leg's, RECORDING the session's (TC-028), sent with the codes.
     */
    public boolean setDtmfActions(Caller caller, Owner o, CommandKey k, String legId,
            List<NodePort.DtmfBinding> bindings, List<String> locks, int interDigitMs) throws TelephonyException {
        return command(caller, o, k, false, s -> {
            Leg l = mediaLeg(s, legId);
            checkDtmfBindings(l.role, bindings);
            java.util.Set<String> wanted = new java.util.HashSet<>(locks);
            List<List<String>> changes = lockChanges(s, legId, wanted);
            nodeRun(() -> node.dtmfActions(legId, l.role.name().toLowerCase(java.util.Locale.ROOT), bindings,
                    interDigitMs == 0 ? 2_000 : interDigitMs, changes.get(0), changes.get(1), s.fence));
            l.locks.clear();
            l.locks.addAll(wanted);
            return false;
        });
    }

    // ---- Recording: the null sink until R9 (MS-012: nothing is written) --------------------------

    /** The only key reference the null sink takes: a key for a real sink must never be "recorded" by it. */
    public static final String NULL_SINK_KEK = "local-null-sink";

    /**
     * Records a leg (both directions) into the node's recording sink. Until R9 the only sink is the
     * null one (nothing written), configured only in local test profiles: elsewhere, and for a key
     * meant for a real sink, the request is refused ({@code invalid_state}). Repeat-safe by
     * recording id; one open recording per leg. The key itself never reaches the engine.
     */
    public boolean startRecording(Caller caller, Owner o, CommandKey k, String legId, String recordingId,
            String wrappedDek, String kekId) throws TelephonyException {
        return command(caller, o, k, false, s -> {
            Session.Recording known = s.recordings.get(recordingId);
            if (known != null) {
                if (!known.legId.equals(legId)) {
                    throw new TelephonyException(Code.INVALID_REQUEST);
                }
                return true;
            }
            Leg l = mediaLeg(s, legId);
            if (!node.recordingNullSink() || !NULL_SINK_KEK.equals(kekId)) {
                RECORDINGS_REFUSED.incrementAndGet();
                LOG.atWarn().addKeyValue("event", "recording.refused").addKeyValue("tenant_id", s.tenantId)
                        .addKeyValue("interaction_id", s.interactionId).addKeyValue("leg_id", legId)
                        .addKeyValue("sink", node.recordingNullSink() ? "null" : "none")
                        .log("StartRecording refused: no recording sink for this request on this node (R9)");
                throw new TelephonyException(Code.INVALID_STATE);
            }
            for (Session.Recording r : s.recordings.values()) {
                if (r.open && r.legId.equals(legId)) {
                    throw new TelephonyException(Code.INVALID_STATE);
                }
            }
            nodeRun(() -> node.startRecording(legId, recordingId, s.fence));
            s.recordings.put(recordingId, new Session.Recording(recordingId, legId, clock.nowMs(), wrappedDek, kekId));
            RECORDINGS_STARTED.incrementAndGet();
            LOG.atInfo().addKeyValue("event", "recording.started").addKeyValue("tenant_id", s.tenantId)
                    .addKeyValue("interaction_id", s.interactionId).addKeyValue("leg_id", l.legId)
                    .addKeyValue("recording_id", recordingId).addKeyValue("sink", "null")
                    .log("recording started (null sink: nothing is written)");
            emit(s, new Events.RecordingStarted(recordingId, legId));
            return false;
        });
    }

    /** Stops a recording; one already finished (or never known here) is {@code already_gone}. */
    public Gone stopRecording(Caller caller, Owner o, CommandKey k, String recordingId) throws TelephonyException {
        return command(caller, o, k, false, s -> {
            Session.Recording r = s.recordings.get(recordingId);
            if (r == null || !r.open) {
                return new Gone(false, true);
            }
            NodePort.RecordingResult res = nodeCall(() -> node.stopRecording(r.legId, s.fence));
            finishRecording(s, r, res);
            return new Gone(false, false);
        });
    }

    /** Every open recording of a leg that ended: its result, waited for a little, then RecordingStopped. */
    private void legRecordingsEnded(Session s, Leg l) {
        for (Session.Recording r : s.recordings.values()) {
            if (r.open && r.legId.equals(l.legId)) {
                finishRecording(s, r, node.finishRecording(l.legId, s.fence));
            }
        }
    }

    /**
     * RecordingStopped from the media's result, or (a node that can't say: the simulated one, a lost
     * parity-ms) from TC's own clock and the session's pauses it followed.
     */
    private void finishRecording(Session s, Session.Recording r, NodePort.RecordingResult res) {
        r.open = false;
        long now = clock.nowMs();
        NodePort.RecordingResult out = res;
        if (out == null) {
            List<NodePort.RecordingPause> pauses = new ArrayList<>();
            for (NodePort.RecordingPause p : s.recordingPauses) {
                if (p.endMs() > r.startedAtMs) {
                    pauses.add(new NodePort.RecordingPause(Math.max(p.startMs(), r.startedAtMs), p.endMs(), p.role(),
                            p.byDtmf()));
                }
            }
            if (s.recordingPaused) {
                pauses.add(new NodePort.RecordingPause(Math.max(s.recordingPausedAtMs, r.startedAtMs), now,
                        s.recordingPausedBy, true));
            }
            out = new NodePort.RecordingResult(Math.max(0, now - r.startedAtMs), List.copyOf(pauses), "", 0, "");
        }
        RECORDINGS_STOPPED.incrementAndGet();
        emit(s, new Events.RecordingStopped(r.recordingId, r.legId, out.path(), out.sizeBytes(), out.durationMs(),
                out.sha256(), r.wrappedDek, r.kekId, out.pauses()));
    }

    /** Originates a leg; the first leg of an outbound session creates the session. */
    public LegResult addLeg(Caller caller, Owner o, CommandKey k, AddLeg a) throws TelephonyException {
        return command(caller, o, k, true, s -> {
            Leg known = s.legs.get(a.legId());
            if (known != null) {
                if (!Objects.equals(known.fingerprint, a.fingerprint())) {
                    throw new TelephonyException(Code.LEG_CONFLICT);
                }
                return new LegResult(true, known.state, known.state == LegState.ENDED ? known.cause : null);
            }
            notEnded(s);
            if (s.state == SessionState.ARRIVING) {
                throw new TelephonyException(Code.INVALID_STATE);
            }
            if (node.freeLegs() < 1) {
                throw new TelephonyException(Code.NO_CAPACITY);
            }
            long now = clock.nowMs();
            Channel ch = new Channel(a.legId(), s.tenantId, s.interactionId, s.ownerBin, a.role(), a.endpoint(),
                    a.legId(), false, now, null, null, null, List.of(), a.fingerprint(), LegState.ORIGINATING);
            ch.ownerEpoch = s.fence;
            ch.ownerReplicaId = s.ownerReplica;
            ch.ownerBin = s.ownerBin;
            // A leg id names one leg on the node: claimed before anything is originated, so another
            // session (another tenant's, perhaps) can't take it and receive this leg's events.
            if (byLeg.putIfAbsent(a.legId(), s) != null) {
                throw new TelephonyException(Code.LEG_CONFLICT);
            }
            NodePort.DtmfSetup dtmf = null;
            java.util.Set<String> dtmfLocks = java.util.Set.of();
            if (a.dtmf() != null && (!a.dtmf().bindings().isEmpty() || !a.dtmf().locks().isEmpty())) {
                // P5: an agent's or supervisor's codes, on its media before it rings; none for a customer.
                if (a.role() != Role.AGENT && a.role() != Role.SUPERVISOR) {
                    throw new TelephonyException(Code.INVALID_REQUEST);
                }
                checkDtmfBindings(a.role(), a.dtmf().bindings());
                dtmfLocks = java.util.Set.copyOf(a.dtmf().locks());
                List<List<String>> changes = lockChanges(s, a.legId(), dtmfLocks);
                dtmf = new NodePort.DtmfSetup(a.role().name().toLowerCase(java.util.Locale.ROOT), a.dtmf().bindings(),
                        a.dtmf().interDigitMs() == 0 ? 2_000 : a.dtmf().interDigitMs(), changes.get(0), changes.get(1));
            }
            NodePort.Dial dial = new NodePort.Dial(a.agentContact(), a.pstnNumber(), a.trunkId(), a.callerId(),
                    a.ringTimeoutS(), s.token, dtmf);
            try {
                nodeRun(() -> node.originate(ch, dial, s.fence));
            } catch (TelephonyException e) {
                byLeg.remove(a.legId(), s);
                throw e;
            } catch (NodePort.Refused e) {
                // TC-056: an agent contact this node won't dial for the session's tenant. Refused with
                // a code IE never retries (it reports the leg failed), not left to a 503 or INTERNAL.
                byLeg.remove(a.legId(), s);
                boolean mismatch = e.why() == NodePort.Refused.Why.TENANT_MISMATCH;
                (mismatch ? AGENT_LEGS_REFUSED_TENANT : AGENT_LEGS_REFUSED_TARGET).incrementAndGet();
                LOG.atWarn().addKeyValue("event", "leg.refused").addKeyValue("tenant_id", s.tenantId)
                        .addKeyValue("interaction_id", s.interactionId)
                        .addKeyValue("leg_id", a.legId()).addKeyValue("reason", e.why().name().toLowerCase(java.util.Locale.ROOT))
                        .addKeyValue("rule", e.getMessage()).log("an agent leg was refused before it was dialled");
                throw new TelephonyException(mismatch ? Code.TENANT_MISMATCH : Code.INVALID_REQUEST);
            }
            Leg l = new Leg(a.legId(), a.role(), a.endpoint(), a.legId(), false, a.fingerprint(),
                    LegState.ORIGINATING);
            l.locks.addAll(dtmfLocks);
            s.legs.put(l.legId, l);
            s.media = s.liveLegs().size() >= 3 || s.media == MediaKind.CONFERENCE ? MediaKind.CONFERENCE
                    : MediaKind.CALL;
            l.ringTimer = clock.schedule(now + a.ringTimeoutS() * 1000L, () -> ringTimeout(s, l.legId));
            return new LegResult(false, LegState.ORIGINATING, null);
        });
    }

    public Gone removeLeg(Caller caller, Owner o, CommandKey k, RemoveLeg r) throws TelephonyException {
        return command(caller, o, k, false, s -> {
            Leg l = leg(s, r.legId());
            if (l.state == LegState.ENDED) {
                return new Gone(false, true);
            }
            List<Ended> ended = nodeCall(() -> node.hangup(r.legId(), r.cause(), EndedBy.COMMAND));
            legsEnded(s, ended, SessionEndReason.ENDED_BY_OWNER, 0);
            return new Gone(false, false);
        });
    }

    public Gone endSession(Caller caller, Owner o, CommandKey k, EndCause cause) throws TelephonyException {
        return command(caller, o, k, false, s -> {
            if (s.state == SessionState.ENDED) {
                return new Gone(false, true);
            }
            List<Ended> ended = new ArrayList<>();
            List<Leg> order = new ArrayList<>(s.liveLegs());
            order.sort(Comparator.comparing((Leg l) -> l.role == Role.CUSTOMER));
            for (Leg l : order) {
                ended.addAll(nodeCall(() -> node.hangup(l.legId, cause, EndedBy.COMMAND)));
            }
            legsEnded(s, ended, SessionEndReason.ENDED_BY_OWNER, 0);
            if (s.state != SessionState.ENDED) {
                endSession(s, SessionEndReason.ENDED_BY_OWNER, 0);
            }
            return new Gone(false, false);
        });
    }

    // ---- The command pipeline -----------------------------------------------------------------

    private <R> R command(Caller caller, Owner o, CommandKey k, boolean mayCreate, Body<R> body)
            throws TelephonyException {
        enter();
        try {
            return commandLive(caller, o, k, mayCreate, body);
        } finally {
            leave();
        }
    }

    /**
     * One command (§4.3, TC ADR 0002 §2.2 and §2.5): the session is found, or created provisionally
     * by a command that may create one; the turn checks the attempt's expiry, the fence and
     * de-duplication, then runs the effects. A provisional session commits with its first command
     * or retires; a turn that finds its session retired starts again on the current one.
     */
    private <R> R commandLive(Caller caller, Owner o, CommandKey k, boolean mayCreate, Body<R> body)
            throws TelephonyException {
        ready();
        zone(caller);
        String key = key(o.tenantId(), o.interactionId());
        for (int attempt = 0; attempt < 8; attempt++) {
            Session s = sessions.get(key);
            if (s == null) {
                if (!mayCreate) {
                    throw new TelephonyException(Code.SESSION_UNKNOWN);
                }
                if (draining) {
                    throw new TelephonyException(Code.DRAINING);
                }
                Session fresh = new Session(o.tenantId(), o.interactionId(), SessionState.ACTIVE);
                fresh.provisional = true;
                fresh.token = UuidV7.create();
                s = sessions.computeIfAbsent(key, x -> fresh);
            }
            Session target = s;
            Object r = bounded(target, () -> {
                if (target.retired) {
                    return RETIRED;
                }
                if (target.provisional && !mayCreate) {
                    throw new TelephonyException(Code.SESSION_UNKNOWN);
                }
                checkAttempt(k);
                resyncIfNeeded(target);
                ownerBin(caller, target, o);
                boolean created = target.provisional;
                try {
                    verify(caller, target, o, k);
                    ack(target, o);
                    pruneCommands(target);
                    Cached c = target.commands.get(k.commandId());
                    if (c != null) {
                        if (c.rpc().equals(k.rpc()) && c.fingerprint().equals(k.fingerprint())) {
                            return c.result();
                        }
                        throw new TelephonyException(Code.COMMAND_CONFLICT);
                    }
                    checkAttempt(k);
                    R result = body.run(target);
                    target.provisional = false;
                    target.commands.put(k.commandId(), new Cached(k.rpc(), k.fingerprint(), result, clock.nowMs()));
                    observer.committed(target.interactionId, o.epoch(), k.rpc());
                    return result;
                } catch (TelephonyException | RuntimeException e) {
                    if (created && target.provisional) {
                        retire(key, target);
                    }
                    throw e;
                }
            });
            if (!same(r, RETIRED)) {
                @SuppressWarnings("unchecked")
                R result = (R) r;
                return result;
            }
        }
        throw new TelephonyException(Code.SESSION_BUSY);
    }

    /** A turn's marker: the session retired before this turn ran; look it up again. */
    private static final Object RETIRED = new Object();

    /** A provisional session whose first command failed goes, with anything it claimed. */
    private void retire(String key, Session s) {
        s.retired = true;
        sessions.remove(key, s);
        s.legs.keySet().forEach(id -> byLeg.remove(id, s));
        withdraw(s, p -> true);
    }

    /** §2.5: an attempt past its expiry does nothing, here and right before its first effect. */
    private void checkAttempt(CommandKey k) throws TelephonyException {
        if (k.attempt().expired(clock.nowMs(), System.nanoTime())) {
            EXPIRED_ATTEMPTS.incrementAndGet();
            throw new TelephonyException(Code.ATTEMPT_EXPIRED);
        }
    }

    /** Recordings started (the null sink until R9). */
    public static long recordingsStarted() {
        return RECORDINGS_STARTED.get();
    }

    /** Recordings finished (stopped, or their leg ended). */
    public static long recordingsStopped() {
        return RECORDINGS_STOPPED.get();
    }

    /** StartRecording refused for want of a sink (or with a key the null sink can't stand for). */
    public static long recordingsRefused() {
        return RECORDINGS_REFUSED.get();
    }

    /** Agent legs refused before they were dialled (TC-056): {@code tenant_mismatch} when true, else {@code invalid_target}. */
    public static long agentLegsRefused(boolean tenantMismatch) {
        return (tenantMismatch ? AGENT_LEGS_REFUSED_TENANT : AGENT_LEGS_REFUSED_TARGET).get();
    }

    /** Fence raises that couldn't turn digit reporting off in parity-ms (a metric; PCI, alert on any). */
    public static long digitsOffFailed() {
        return DIGITS_OFF_FAILED.get();
    }

    /** Commands that came after their expiry and did nothing (a metric). */
    public static long expiredAttempts() {
        return EXPIRED_ATTEMPTS.get();
    }

    /** Node facts dropped by a full mailbox (a metric; the sessions resynced). */
    public static long mailboxOverflows() {
        return OVERFLOWS.get();
    }

    /** Arrivals refused because their interaction already had a session (a metric). */
    public static long duplicateArrivals() {
        return DUPLICATE_ARRIVALS.get();
    }

    /** §4.3 step 3. A higher epoch is checked against etcd once, then the fences are raised. */
    private void verify(Caller caller, Session s, Owner o) throws TelephonyException {
        verify(caller, s, o, null);
    }

    private void verify(Caller caller, Session s, Owner o, CommandKey k) throws TelephonyException {
        if (o.epoch() < s.fence) {
            throw new TelephonyException(Code.STALE_OWNER);
        }
        if (o.epoch() == s.fence) {
            if (!Objects.equals(o.replicaId(), s.ownerReplica)) {
                throw new TelephonyException(Code.OWNER_UNVERIFIED);
            }
            return;
        }
        Optional<OwnerRecord> found;
        try {
            found = owners.owner(o.interactionId());
        } catch (OwnerDirectoryUnavailable e) {
            throw new TelephonyException(Code.OWNER_CHECK_UNAVAILABLE);
        }
        if (found.isEmpty() || found.get().createRevision() > o.epoch()) {
            throw new TelephonyException(Code.STALE_OWNER);
        }
        OwnerRecord r = found.get();
        if (r.createRevision() < o.epoch() || !Objects.equals(r.replicaId(), o.replicaId())
                || !Objects.equals(r.bin(), caller.bin()) || !Objects.equals(r.tenantId(), o.tenantId())) {
            throw new TelephonyException(Code.OWNER_UNVERIFIED);
        }
        if (!Objects.equals(r.nodeId(), config.nodeId())) {
            throw new TelephonyException(Code.WRONG_NODE);
        }
        if (k != null) {
            checkAttempt(k);
        }
        raise(s, o.epoch(), o.replicaId(), r.bin());
    }

    /**
     * Raises the fences: {@code cc_owner_epoch} (and the replica and bin) on every live leg, then
     * parity-ms's claim; then the session's events follow the owner. Repeating it is harmless, and
     * completes a raise a crash cut short.
     */
    private void raise(Session s, long epoch, String replicaId, String bin) throws TelephonyException {
        if (s.state == SessionState.ARRIVING && !s.claimed) {
            Leg c = s.customer();
            if (c != null && c.state != LegState.ENDED) {
                nodeRun(() -> node.claimArrival(c.legId));
            }
        }
        int written = 0;
        for (Leg l : s.liveLegs()) {
            nodeRun(() -> node.setOwner(l.legId, epoch, replicaId, bin));
            written++;
            if (crashAfterLegs >= 0 && written >= crashAfterLegs) {
                crashAfterLegs = -1;
                throw new SimulatedCrash();
            }
        }
        if (s.media != MediaKind.NONE) {
            nodeRun(() -> node.mediaClaim(s.interactionId, epoch));
        }
        String before = s.bound();
        boolean raised = epoch > s.fence;
        if (s.state == SessionState.ENDED && epoch > s.fence) {
            // A tombstone has no leg to carry cc_owner_epoch: the journal keeps its new fence, so a
            // restart can't hand the older owner its session back (found by the X11 stress test).
            journal.sessionEnded(new Journal.SessionEnded(s.tenantId, s.interactionId, s.endReason, s.endSipStatus,
                    s.endedAtMs, epoch, replicaId, bin, s.endedOwnerless));
        }
        s.fence = epoch;
        s.ownerReplica = replicaId;
        s.ownerBin = bin;
        if (s.state == SessionState.ARRIVING && !s.claimed) {
            closeOffer(s);
        }
        if (raised) {
            // PCI (ADR 0001 §4.4, T15): a new owner starts with every leg's digits unreported, and
            // never gets the old owner's unacknowledged digits.
            digitsOff(s, epoch);
            withdraw(s, p -> p.delivery.event() instanceof Events.DtmfReceived);
        }
        if (!replicaId.equals(before)) {
            replay(s);
        }
    }

    /**
     * Every leg's digit reporting OFF, in TC and in parity-ms. TC's side always changes; a leg
     * parity-ms couldn't be told about is retried by its collection's own deadline (≤ 300 s).
     */
    private void digitsOff(Session s, long epoch) {
        for (Leg l : s.liveLegs()) {
            if ("OFF".equals(l.digitMode)) {
                continue;
            }
            try {
                node.reportDigits(l.legId, "off", epoch);
                if (l.digitTimer != null) {
                    l.digitTimer.cancel();
                    l.digitTimer = null;
                }
            } catch (NodePort.Unavailable e) {
                DIGITS_OFF_FAILED.incrementAndGet();
                LOG.atError().addKeyValue("event", "digits.off_failed").addKeyValue("tenant_id", s.tenantId)
                        .addKeyValue("interaction_id", s.interactionId).addKeyValue("leg_id", l.legId)
                        .log("couldn't turn a leg's digit reporting off at a fence raise: its deadline will");
            }
            l.digitMode = "OFF";
            l.collectionId = "";
        }
    }

    /** §4.3 step 1, once the session is known: an owned session takes its owner's bin only. */
    private static void ownerBin(Caller caller, Session s, Owner o) throws TelephonyException {
        if (s.fence > 0 && o.epoch() <= s.fence && !Objects.equals(caller.bin(), s.ownerBin)) {
            throw new TelephonyException(Code.IDENTITY_MISMATCH);
        }
    }

    private void zone(Caller caller) throws TelephonyException {
        if (caller.bin() == null || !config.bins().contains(caller.bin())) {
            throw new TelephonyException(Code.IDENTITY_MISMATCH);
        }
    }

    private void ready() throws TelephonyException {
        if (closed || clock.nowMs() < rebuildUntil || !rebuilt) {
            throw new TelephonyException(Code.REBUILDING);
        }
    }

    private void ack(Session s, Owner o) {
        if (o.ackedSeq() > 0 && config.incarnation().equals(o.ackedIncarnation())) {
            removeAcked(s, o.ackedSeq());
        }
    }

    private void removeAcked(Session s, long seq) {
        // A SessionEnded the limits dropped counts as acknowledged once the acked seq passes it
        // (TC ADR 0002 §9): otherwise its tombstone would wait the full 24 h.
        if (s.droppedEndSeq > 0 && seq >= s.droppedEndSeq && !s.endAcked) {
            s.endAcked = true;
            long now = clock.nowMs();
            journal.acked(new Journal.Acked(s.tenantId, s.interactionId, now));
            scheduleRelease(s, now + TOMBSTONE_AFTER_ACK_MS);
        }
        Iterator<Pending> it = s.unacked.iterator();
        while (it.hasNext()) {
            Pending p = it.next();
            if (p.delivery.seq() <= seq) {
                it.remove();
                bufferedBytes.addAndGet(-p.bytes);
                if (p.delivery.event() instanceof Events.SessionEnded && !s.endAcked) {
                    s.endAcked = true;
                    long now = clock.nowMs();
                    journal.acked(new Journal.Acked(s.tenantId, s.interactionId, now));
                    scheduleRelease(s, now + TOMBSTONE_AFTER_ACK_MS);
                }
            }
        }
    }

    private void pruneCommands(Session s) {
        long cutoff = clock.nowMs() - COMMAND_TTL_MS;
        s.commands.values().removeIf(c -> c.atMs() < cutoff);
    }

    // ---- Delivery ------------------------------------------------------------------------------

    private void emit(Session s, Event e) {
        long now = clock.nowMs();
        Delivery d = new Delivery(config.incarnation(), config.nodeId(), s.nextSeq++,
                new SessionRef(s.tenantId, s.interactionId, s.fence), now, e);
        if (e instanceof Arrived) {
            s.arrivedSeq = d.seq();
        }
        Pending p = new Pending(d, d.estimatedBytes(), now);
        s.unacked.add(p);
        bufferedBytes.addAndGet(p.bytes);
        trim(s, now);
        send(s, d);
    }

    /** §4.4's limits on one session's unacknowledged events: the oldest go first. */
    private void trim(Session s, long now) {
        Limits l = config.limits();
        while (!s.unacked.isEmpty()
                && (s.unacked.size() > l.eventsPerSession()
                        || now - s.unacked.peekFirst().delivery.atMs() > l.maxEventAgeMs()
                        || (bufferedBytes.get() > l.nodeBytes() && s.unacked.size() > 1))) {
            dropOldest(s);
        }
    }

    private void dropOldest(Session s) {
        Pending p = s.unacked.pollFirst();
        if (p == null) {
            return;
        }
        if (p.delivery.event() instanceof Events.SessionEnded) {
            s.droppedEndSeq = p.delivery.seq();
        }
        bufferedBytes.addAndGet(-p.bytes);
        DROPPED.incrementAndGet();
        if (!s.dropLogged) {
            s.dropLogged = true;
            LOG.atWarn().addKeyValue("event", "delivery.dropped").addKeyValue("tenant_id", s.tenantId)
                    .addKeyValue("interaction_id", s.interactionId).addKeyValue("delivery_seq", p.delivery.seq())
                    .log("unacknowledged events dropped by the delivery limits; IE resyncs from the gap");
        }
    }

    /** Removes unacknowledged events (not a drop: an offer withdrawn or closed). */
    private void withdraw(Session s, java.util.function.Predicate<Pending> which) {
        Iterator<Pending> it = s.unacked.iterator();
        while (it.hasNext()) {
            Pending p = it.next();
            if (which.test(p)) {
                it.remove();
                bufferedBytes.addAndGet(-p.bytes);
            }
        }
    }

    /** Under the session's lock: to its stream, after replaying what that stream hasn't had. */
    private void send(Session s, Delivery d) {
        Stream st = boundStream(s);
        if (st == null) {
            return;
        }
        if (!same(s.attachedStream, st)) {
            replayTo(s, st);
            return;
        }
        deliverTo(st, d);
    }

    private Stream boundStream(Session s) {
        String replica = s.bound();
        return replica == null ? null : streams.get(replica);
    }

    /** A stream that can't take a message (its reader fell behind) is gone: forget it, as on a close. */
    private boolean deliverTo(Stream st, Delivery d) {
        if (st.sink.send(d)) {
            return true;
        }
        if (streams.remove(st.replicaId, st)) {
            clock.schedule(clock.nowMs(), () -> {
                if (!closed) {
                    streamGone(st.replicaId);
                }
            });
        }
        return false;
    }

    /** Arrivals offered to a replica whose stream is gone go to the next one. */
    private void streamGone(String replicaId) {
        for (Session session : sessions.values()) {
            if (session.provisional) {
                continue;
            }
            withLock(session, x -> {
                if (x.state == SessionState.ARRIVING && !x.claimed && replicaId.equals(x.offeredReplica)) {
                    reoffer(x);
                }
                return null;
            });
        }
    }

    /** Unacknowledged events' estimated size on this node. */
    public long bufferedBytes() {
        return bufferedBytes.get();
    }

    /** Unacknowledged events one session holds (tests). */
    public int buffered(String tenantId, String interactionId) {
        Session s = sessions.get(key(tenantId, interactionId));
        return s == null ? 0 : withLock(s, x -> x.unacked.size());
    }

    /** Events dropped by the limits since the process started. */
    public static long droppedEvents() {
        return DROPPED.get();
    }

    /** Whether a replica has a live stream here (tests). */
    public boolean hasStream(String replicaId) {
        return streams.containsKey(replicaId);
    }

    private void replay(Session s) {
        Stream st = boundStream(s);
        if (st != null) {
            replayTo(s, st);
        }
    }

    /** Every unacknowledged event, in order, to this stream; from now on the session is attached to it. */
    private void replayTo(Session s, Stream st) {
        s.attachedStream = st;
        long now = clock.nowMs();
        for (Pending p : s.unacked) {
            if (!deliverTo(st, p.delivery)) {
                return;
            }
            p.lastSentMs = now;
        }
    }

    /**
     * Every 2 s: the limits (age, and the node's budget across sessions, oldest first per session),
     * then events unacknowledged for 2 s again, but only to a stream that has drained: a slow
     * reader gets no extra copies.
     */
    private void redeliver() {
        long now = clock.nowMs();
        for (Session s : sessions.values()) {
            if (s.provisional) {
                continue;
            }
            withLock(s, x -> {
                trim(x, now);
                Stream st = boundStream(x);
                if (st == null || !st.sink.drained()) {
                    return null;
                }
                if (!same(x.attachedStream, st)) {
                    replayTo(x, st);
                    return null;
                }
                for (Pending p : x.unacked) {
                    if (now - p.lastSentMs >= REDELIVER_MS) {
                        if (!deliverTo(st, p.delivery)) {
                            break;
                        }
                        p.lastSentMs = now;
                    }
                }
                return null;
            });
        }
        while (bufferedBytes.get() > config.limits().nodeBytes()) {
            boolean any = false;
            for (Session s : sessions.values()) {
                any |= Boolean.TRUE.equals(withLock(s, x -> {
                    if (x.unacked.isEmpty()) {
                        return false;
                    }
                    dropOldest(x);
                    return true;
                }));
                if (bufferedBytes.get() <= config.limits().nodeBytes()) {
                    break;
                }
            }
            if (!any) {
                break;
            }
        }
    }

    private void attach(Stream stream) {
        for (Session s : sessions.values()) {
            if (s.provisional) {
                continue;
            }
            withLock(s, x -> {
                if (stream.replicaId.equals(x.bound())) {
                    if (!same(x.attachedStream, stream) && same(streams.get(stream.replicaId), stream)) {
                        replayTo(x, stream);
                    }
                } else if (x.state == SessionState.ARRIVING && !x.claimed && x.offeredReplica == null
                        && stream.bin.equals(x.arrivalBin) && x.declinedBy == null) {
                    offer(x);
                }
                return null;
            });
        }
    }

    private void heartbeat() {
        Delivery d = new Delivery(config.incarnation(), config.nodeId(), 0, null, clock.nowMs(), new Heartbeat());
        streams.values().forEach(s -> deliverTo(s, d));
    }

    private void broadcastNodeStatus() {
        Delivery d = nodeStatusDelivery();
        streams.values().forEach(s -> deliverTo(s, d));
    }

    private Delivery nodeStatusDelivery() {
        int live = (int) sessions.values().stream().filter(s -> !s.provisional && s.state != SessionState.ENDED).count();
        NodeStatus status = new NodeStatus(live, node.freeLegs(), draining, !rebuilt || clock.nowMs() < rebuildUntil,
                config.version(), capabilities());
        return new Delivery(config.incarnation(), config.nodeId(), 0, null, clock.nowMs(), status);
    }

    /** Capability: AddLeg's DTMF action codes are accepted (CONVENTIONS: IE sends them only when listed). */
    public static final String CAP_DTMF_ACTIONS = "dtmf_actions";
    /** Capability: StartRecording runs into the null sink (nothing written; local test setups only, until R9). */
    public static final String CAP_RECORDING_NULL_SINK = "recording_null_sink";

    /** What this node does beyond 0.2.0's base set (N/N-1): the configured ones, then this release's. */
    private List<String> capabilities() {
        List<String> caps = new ArrayList<>(config.capabilities());
        caps.add(CAP_DTMF_ACTIONS);
        if (node.recordingNullSink()) {
            caps.add(CAP_RECORDING_NULL_SINK);
        }
        return List.copyOf(caps);
    }

    private void every(long periodMs, Runnable task) {
        clock.schedule(clock.nowMs() + periodMs, () -> {
            if (closed) {
                return;
            }
            task.run();
            every(periodMs, task);
        });
    }

    // ---- Arrivals ------------------------------------------------------------------------------

    @Override
    public void parked(Channel ch) {
        nodeEvent(() -> arrive(ch));
    }

    /**
     * An arrival (TC ADR 0002 §3). Refused at once (503) when its bin isn't the zone's, when its
     * interaction already has a session on another leg (counted), or when its leg id is taken; the
     * same leg arriving again changes nothing. Accepted: TC's token and the hangup hook are written
     * (R1-1), then the arrival is offered.
     */
    private void arrive(Channel ch) {
        if (closed) {
            return;
        }
        if (!config.bins().contains(ch.bin)) {
            refuse(ch.legId);
            return;
        }
        String key = key(ch.tenantId, ch.interactionId);
        Session fresh = new Session(ch.tenantId, ch.interactionId, SessionState.ARRIVING);
        fresh.token = ch.sessionToken != null ? ch.sessionToken : UuidV7.create();
        Session s = sessions.putIfAbsent(key, fresh);
        if (s != null) {
            if (!same(byLeg.get(ch.legId), s)) {
                DUPLICATE_ARRIVALS.incrementAndGet();
                LOG.atWarn().addKeyValue("event", "arrival.duplicated").addKeyValue("tenant_id", ch.tenantId)
                        .addKeyValue("interaction_id", ch.interactionId).addKeyValue("leg_id", ch.legId)
                        .log("an arrival for an interaction that has a session: 503");
                refuse(ch.legId);
            }
            return;
        }
        if (byLeg.putIfAbsent(ch.legId, fresh) != null) {
            sessions.remove(key, fresh);
            refuse(ch.legId);
            return;
        }
        fact(fresh, x -> {
            try {
                node.accepted(ch.legId, x.token);
            } catch (NodePort.Unavailable e) {
                // ESL can't take the hook: the dialplan's safety hangup ends the call (503).
                sessions.remove(key, x);
                byLeg.remove(ch.legId, x);
                x.retired = true;
                LOG.atWarn().addKeyValue("event", "arrival.accept_failed").addKeyValue("tenant_id", ch.tenantId)
                        .addKeyValue("interaction_id", ch.interactionId).addKeyValue("leg_id", ch.legId)
                        .log("can't accept an arrival: the node is unavailable");
                return;
            }
            Leg l = new Leg(ch.legId, Role.CUSTOMER, ch.endpoint, ch.sipCallId, true, null, LegState.PARKED);
            x.legs.put(l.legId, l);
            x.arrivalBin = ch.bin;
            x.parkedAtMs = ch.createdAtMs;
            x.deadlineMs = ch.createdAtMs + ARRIVAL_DEADLINE_MS;
            x.arrivalTemplate = arrival(ch);
            LOG.atInfo().addKeyValue("event", "session.arrived").addKeyValue("tenant_id", ch.tenantId)
                    .addKeyValue("interaction_id", ch.interactionId).addKeyValue("leg_id", ch.legId)
                    .addKeyValue("bin", ch.bin).addKeyValue("endpoint", ch.endpoint).log("a call arrived");
            offer(x);
            x.deadlineTimer = clock.schedule(x.deadlineMs, () -> arrivalDeadline(x));
        });
    }

    /** A 503 for an arrival TC won't take; never touches another call (it has no hook, R1-1). */
    private void refuse(String legId) {
        Runnable r = () -> {
            try {
                node.reject(legId, EndCause.SYSTEM, REJECT_STATUS, EndedBy.SYSTEM);
            } catch (NodePort.Unavailable e) {
                LOG.atWarn().addKeyValue("event", "arrival.refuse_failed").addKeyValue("leg_id", legId)
                        .log("can't refuse an arrival: the node is unavailable");
            }
        };
        if (node.callbacksInline()) {
            r.run();
        } else {
            Thread.ofVirtual().name("tc-refuse").start(r);
        }
    }

    /**
     * The SIP headers an arrival may carry to IE: none by default (TC ADR 0001 R1-7, O7); never
     * an identity, authorization or routing-history header.
     */
    static final Set<String> ALLOWED_HEADERS = Set.of();

    private static Arrived arrival(Channel ch) {
        List<Events.Header> headers = ch.headers.stream()
                .filter(h -> ALLOWED_HEADERS.contains(h[0].toLowerCase(java.util.Locale.ROOT)))
                .map(h -> new Events.Header(h[0], h[1])).toList();
        return new Arrived(ch.bin, ch.legId, ch.sipCallId, ch.calledNumber, ch.callerNumber, headers, 0, "",
                ch.createdAtMs + ARRIVAL_DEADLINE_MS, ch.trunkId == null ? "" : ch.trunkId);
    }

    /** Offers the arrival to the first live replica of its bin, by rendezvous; none: waits for one. */
    private void offer(Session s) {
        List<String> order = candidates(s);
        if (order.isEmpty()) {
            return;
        }
        sendOffer(s, order.get(0));
    }

    private void sendOffer(Session s, String replicaId) {
        withdraw(s, p -> p.delivery.event() instanceof Arrived);
        s.offeredReplica = replicaId;
        s.attempt++;
        Arrived t = s.arrivalTemplate;
        emit(s, new Arrived(t.bin(), t.legId(), t.sipCallId(), t.calledNumber(), t.callerNumber(), t.headers(),
                s.attempt, t.redirectedFromBin(), s.deadlineMs, t.trunkId()));
        if (s.reofferTimer != null) {
            s.reofferTimer.cancel();
        }
        s.reofferTimer = clock.schedule(clock.nowMs() + REOFFER_MS, () -> withLock(s, x -> {
            if (!closed) {
                reoffer(x);
            }
            return null;
        }));
    }

    /** No ack or claim within 1 s, or the offered stream closed: the next replica, if there is one. */
    private void reoffer(Session s) {
        if (s.state != SessionState.ARRIVING || s.claimed || s.declinedBy != null
                || (s.arrivalAcked && streams.containsKey(String.valueOf(s.offeredReplica)))) {
            return;
        }
        List<String> order = candidates(s);
        int i = order.indexOf(s.offeredReplica);
        String next = i < 0 ? (order.isEmpty() ? null : order.get(0))
                : (i + 1 < order.size() ? order.get(i + 1) : null);
        if (next == null) {
            if (s.offeredReplica == null || !streams.containsKey(s.offeredReplica)) {
                s.offeredReplica = null;
            }
            return;
        }
        s.arrivalAcked = false;
        sendOffer(s, next);
    }

    private List<String> candidates(Session s) {
        Collection<String> live = streams.values().stream()
                .filter(st -> st.bin.equals(s.arrivalBin)).map(Stream::replicaId).toList();
        return Rendezvous.order(s.interactionId, live);
    }

    private void closeOffer(Session s) {
        s.claimed = true;
        if (s.reofferTimer != null) {
            s.reofferTimer.cancel();
        }
        if (s.deadlineTimer != null) {
            s.deadlineTimer.cancel();
        }
        withdraw(s, p -> p.delivery.event() instanceof Arrived);
    }

    /** 5 s without a verified command or claim: 503. */
    private Void arrivalDeadline(Session s) {
        return withLock(s, x -> {
            if (closed || x.state != SessionState.ARRIVING || x.claimed || x.declinedBy != null) {
                return null;
            }
            Leg c = x.customer();
            try {
                Ended e = node.reject(c.legId, EndCause.SYSTEM, REJECT_STATUS, EndedBy.SYSTEM);
                if (x.reofferTimer != null) {
                    x.reofferTimer.cancel();
                }
                legsEnded(x, e == null ? List.of() : List.of(e), SessionEndReason.ARRIVAL_UNCLAIMED, REJECT_STATUS);
            } catch (NodePort.Unavailable e) {
                x.deadlineTimer = clock.schedule(clock.nowMs() + REOFFER_MS, () -> arrivalDeadline(x));
            }
            return null;
        });
    }

    /**
     * A live leg the node's first snapshot missed (R1-14): added to its session as the rebuild adds
     * a channel (the session made if it has none), and reported with its state.
     */
    @Override
    public void discovered(Channel ch) {
        nodeEvent(() -> {
            if (closed) {
                return;
            }
            String k = key(ch.tenantId, ch.interactionId);
            Session fresh = new Session(ch.tenantId, ch.interactionId,
                    ch.state == LegState.ANSWERED || !ch.inbound ? SessionState.ACTIVE : SessionState.ARRIVING);
            fresh.token = ch.sessionToken != null ? ch.sessionToken : UuidV7.create();
            Session existing = sessions.putIfAbsent(k, fresh);
            Session s = existing != null ? existing : fresh;
            fact(s, x -> {
                if (x.legs.containsKey(ch.legId) || x.state == SessionState.ENDED) {
                    return;
                }
                Leg l = new Leg(ch.legId, ch.role, ch.endpoint, ch.sipCallId, ch.inbound, ch.addLegFingerprint, ch.state);
                l.answeredAtMs = ch.answeredAtMs;
                x.legs.put(l.legId, l);
                byLeg.putIfAbsent(l.legId, x);
                if (ch.ownerEpoch > x.fence) {
                    x.fence = ch.ownerEpoch;
                    x.ownerReplica = ch.ownerReplicaId;
                    x.ownerBin = ch.ownerBin;
                }
                if (ch.inbound && ch.role == Role.CUSTOMER) {
                    x.arrivalBin = ch.bin;
                    x.parkedAtMs = ch.createdAtMs;
                    x.deadlineMs = ch.createdAtMs + ARRIVAL_DEADLINE_MS;
                    x.arrivalTemplate = arrival(ch);
                }
                x.claimed = x.fence > 0;
                if (x.media == MediaKind.NONE && node.mediaEpoch(x.interactionId) >= 0) {
                    x.media = x.liveLegs().size() >= 3 ? MediaKind.CONFERENCE : MediaKind.CALL;
                }
                // A parked arrival isn't re-offered from here (its safety hangup ends it; BUGS.md TC-018).
                if (l.state != LegState.PARKED) {
                    emit(x, legUpdate(l));
                }
            });
        });
    }

    // ---- What the far ends do --------------------------------------------------------------------

    @Override
    public void ringing(String legId) {
        nodeEvent(() -> onLeg(legId, (s, l) -> {
            if (l.state == LegState.ORIGINATING) {
                l.state = LegState.RINGING;
                emit(s, legUpdate(l));
            }
        }));
    }

    @Override
    public void answered(String legId, long atMs) {
        nodeEvent(() -> onLeg(legId, (s, l) -> {
            if (l.state == LegState.ORIGINATING || l.state == LegState.RINGING) {
                l.state = LegState.ANSWERED;
                l.answeredAtMs = atMs;
                if (l.ringTimer != null) {
                    l.ringTimer.cancel();
                }
                emit(s, legUpdate(l));
            }
        }));
    }

    /** A play ended on a leg, without its id (an older media: plays end in the order they were sent). */
    @Override
    public void playFinished(String legId, String reason, long playedMs) {
        playFinished(legId, reason, playedMs, "");
    }

    /**
     * A play ended on a leg. With its id (parity-ms echoes the one TC sent), it is that play, wherever it
     * is in the leg's list; an id TC doesn't track is a play it gave up on (its send failed or timed
     * out), and is ignored rather than taken for the next one (MS-042, TC-050). Without an id, plays end
     * in order. A loop the media played once (it didn't say it loops) plays again; others reach IE.
     */
    @Override
    public void playFinished(String legId, String reason, long playedMs, String playId) {
        nodeEvent(() -> onLeg(legId, (s, l) -> {
            boolean byId = playId != null && !playId.isEmpty();
            Session.Play p = byId ? byWireId(l, playId) : l.plays.peekFirst();
            if (p == null) {
                if (byId && !l.endedPlays.contains(playId)) {
                    LOG.atWarn().addKeyValue("event", "play.untracked_finished").addKeyValue("leg_id", legId)
                            .addKeyValue("reason", reason)
                            .log("a play ended that TC doesn't track (one it gave up on): ignored");
                }
                return;
            }
            // Only a loop the media doesn't loop itself completes (TC-051: never one that does, e.g. an
            // empty prompt, which would otherwise be sent again and again).
            if (p.loop && !p.nodeLoops && p.wanted && !p.stopping && "completed".equals(reason)
                    && l.state == LegState.ANSWERED && replayLoop(s, l, p)) {
                return;
            }
            endPlay(l, p);
            String r = switch (reason) {
                case "completed" -> "COMPLETED";
                case "stopped", "replaced" -> "STOPPED";
                default -> "FAILED";
            };
            reportFinished(s, l, p, r, playedMs);
        }));
    }

    /** An older media's "loop": the play is sent again (a new id; IE hears nothing). False if it can't be. */
    private boolean replayLoop(Session s, Leg l, Session.Play p) {
        String before = p.wireId;
        p.wireId = wireId();
        try {
            if (node.asyncPlays()) {
                NodePort.PlaySent sent = node.playAsync(l.legId, p.file, true, p.wireId, s.fence);
                if (sent == NodePort.PlaySent.REFUSED) {
                    p.wireId = before;
                    return false;
                }
                p.nodeLoops = node.loopsNatively();
            } else {
                p.nodeLoops = node.play(l.legId, p.file, true, p.wireId, s.fence);
            }
            return true;
        } catch (NodePort.Unavailable e) {
            p.wireId = before;
            LOG.atWarn().addKeyValue("event", "play.loop_failed").addKeyValue("leg_id", l.legId)
                    .log("can't play a looped prompt again");
            return false;
        }
    }

    /**
     * The play saga (ADR 0004): a play started. One TC still wants reaches its owner (PlayStarted, or
     * the waiting caller's answer); one it no longer wants (or doesn't track, on a leg it owns) is
     * stopped at once: the compensation for a start that came after TC gave it up.
     */
    @Override
    public void playStarted(String legId, String wireId) {
        nodeEvent(() -> onLeg(legId, (s, l) -> {
            if (l.state == LegState.ENDED) {
                stopById(s, l, wireId, "leg_ended");
                return;
            }
            Session.Play p = byWireId(l, wireId);
            if (p == null) {
                // Not tracked: never TC's to leave running, ended (a start after its end) or unknown.
                stopById(s, l, wireId, l.endedPlays.contains(wireId) ? "late_start" : "untracked");
                return;
            }
            if (!p.wanted) {
                compensate(s, l, p, "late_start");
                return;
            }
            if (p.state == Session.PlayState.STARTED) {
                return; // an older media's loop, sent again: its start was told already
            }
            p.state = Session.PlayState.STARTED;
            cancelRetry(p);
            reportStarted(s, l, p);
        }));
    }

    /**
     * A start of a play TC doesn't track on a leg of its session, or on a leg that has ended: stopped
     * by its id (TC never leaves a play it doesn't want running).
     */
    private void stopById(Session s, Leg l, String wireId, String reason) {
        PLAYS_COMPENSATED.get(reason).incrementAndGet();
        LOG.atWarn().addKeyValue("event", "play.compensated").addKeyValue("tenant_id", s.tenantId)
                .addKeyValue("interaction_id", s.interactionId).addKeyValue("leg_id", l.legId)
                .addKeyValue("play_wire_id", wireId).addKeyValue("reason", reason)
                .log("a play TC doesn't want started: stopped by its id");
        try {
            node.stopPlay(l.legId, wireId, s.fence);
        } catch (NodePort.Unavailable e) {
            LOG.atWarn().addKeyValue("event", "play.compensation_deferred").addKeyValue("tenant_id", s.tenantId)
                    .addKeyValue("interaction_id", s.interactionId).addKeyValue("leg_id", l.legId)
                    .addKeyValue("reason", reason).log("parity-ms didn't take the stop of a play TC doesn't want");
        }
    }

    /** The play saga (ADR 0004): a play ended without starting; once, to its owner. */
    @Override
    public void playFailed(String legId, String wireId, String reason) {
        nodeEvent(() -> onLeg(legId, (s, l) -> {
            Session.Play p = byWireId(l, wireId);
            if (p == null) {
                return;
            }
            endPlay(l, p);
            if (p.wanted) {
                // An unwanted play's end was told by what called it off (a stop, a newer play, a give-up).
                reportFailed(s, l, p, failReason(reason));
            }
        }));
    }

    /** A far end's hold or resume reaches IE, once per change. */
    @Override
    public void remoteHold(String legId, boolean on) {
        nodeEvent(() -> onLeg(legId, (s, l) -> {
            if (l.remoteHeld != on && l.state != LegState.ENDED) {
                l.remoteHeld = on;
                emit(s, new Events.RemoteHold(legId, on));
            }
        }));
    }

    /** Media stopping or coming back on a leg reaches IE, once per change. */
    @Override
    public void mediaActivity(String legId, boolean active) {
        nodeEvent(() -> onLeg(legId, (s, l) -> {
            if (l.mediaInactive == active && l.state == LegState.ANSWERED) {
                l.mediaInactive = !active;
                emit(s, new Events.MediaActivity(legId, active));
            }
        }));
    }

    /**
     * A matched DTMF action code reaches IE with the leg's state after it (never the digits). The
     * session's recording state is parity-ms's when it says (the pause is the whole session's, PCI);
     * otherwise it follows the results. Unset while the session never had a recording.
     */
    @Override
    public void dtmfAction(String legId, String action, String result, boolean muted, String recording) {
        nodeEvent(() -> onLeg(legId, (s, l) -> {
            if (l.state == LegState.ENDED) {
                return;
            }
            l.muted = muted;
            boolean known = recording != null && !recording.isEmpty();
            boolean was = s.recordingPaused;
            boolean paused = was;
            if (known) {
                paused = "PAUSED".equals(recording);
            } else if ("APPLIED".equals(result) && s.recording()) {
                paused = switch (action) {
                    case "RECORDING_PAUSE" -> true;
                    case "RECORDING_RESUME" -> false;
                    case "RECORDING_TOGGLE" -> !was;
                    default -> was;
                };
            }
            long now = clock.nowMs();
            if (paused && !was) {
                s.recordingPaused = true;
                s.recordingPausedAtMs = now;
                s.recordingPausedBy = l.role;
            } else if (!paused && was) {
                s.recordingPauses.add(new NodePort.RecordingPause(s.recordingPausedAtMs, now, s.recordingPausedBy, true));
                s.recordingPaused = false;
            }
            String state;
            if (known && !(s.recordings.isEmpty() && "STOPPED".equals(recording))) {
                state = recording;
            } else if (s.recordings.isEmpty()) {
                state = "";
            } else {
                state = !s.recording() ? "STOPPED" : paused ? "PAUSED" : "ACTIVE";
            }
            emit(s, new Events.DtmfAction(legId, l.role, action, result, muted, state));
        }));
    }

    /** A digit under the leg's collection reaches IE (masked as an empty digit). PCI: never logged. */
    @Override
    public void digit(String legId, String digit, int durationMs, String source) {
        nodeEvent(() -> onLeg(legId, (s, l) -> {
            if ("OFF".equals(l.digitMode)) {
                return;
            }
            String d = "ON".equals(l.digitMode) && digit.matches("[0-9*#A-D]") ? digit : "";
            emit(s, new Events.DtmfReceived(legId, d, durationMs, "inband".equals(source) ? "INBAND" : "RFC4733",
                    l.collectionId));
        }));
    }

    @Override
    public void ended(List<Ended> legs) {
        nodeEvent(() -> {
            if (closed || legs.isEmpty()) {
                return;
            }
            Session s = byLeg.get(legs.get(0).legId());
            if (s != null) {
                fact(s, x -> {
                    legsEnded(x, legs, SessionEndReason.ALL_LEGS_GONE, 0);
                    coupleToCustomer(x);
                });
            }
        });
    }

    /**
     * The customer's leg ended: the session's other live legs end too, in order (R1-13), as
     * freeswitch's hook would; with the simulated node they ended already, and the real node's own
     * events for them merge as no-ops.
     */
    private void coupleToCustomer(Session s) {
        Leg c = s.customer();
        if (c == null || c.state != LegState.ENDED || s.state == SessionState.ENDED) {
            return;
        }
        List<Ended> others = new ArrayList<>();
        for (Leg l : s.liveLegs()) {
            EndCause cause = l.state == LegState.ANSWERED ? EndCause.NORMAL : EndCause.CANCELED;
            try {
                node.hangup(l.legId, cause, EndedBy.SYSTEM);
            } catch (NodePort.Unavailable e) {
                LOG.atWarn().addKeyValue("event", "leg.end_failed").addKeyValue("tenant_id", s.tenantId)
                        .addKeyValue("interaction_id", s.interactionId).addKeyValue("leg_id", l.legId)
                        .log("can't end a leg whose customer left; its hook will");
            }
            others.add(new Ended(l.legId, cause, 0, EndedBy.SYSTEM, clock.nowMs()));
        }
        if (!others.isEmpty()) {
            legsEnded(s, others, SessionEndReason.ALL_LEGS_GONE, 0);
        }
    }

    @Override
    public void remoteMedia(String legId, RemoteMediaKind kind) {
        nodeEvent(() -> onLeg(legId, (s, l) -> {
            if (l.state == LegState.ENDED || s.state == SessionState.ENDED) {
                // A late re-INVITE for a leg that is gone: nothing follows a SessionEnded.
                return;
            }
            RemoteMediaKind reported = kind;
            if (kind == RemoteMediaKind.ADDRESS) {
                try {
                    node.mediaUpdate(s.interactionId, s.fence);
                } catch (NodePort.Unavailable e) {
                    reported = RemoteMediaKind.UNREAD;
                }
            }
            emit(s, new RemoteMediaChanged(legId, reported));
        }));
    }

    @Override
    public void lost(NodePort.Part part) {
        nodeEvent(() -> lostNow(part));
    }

    private void lostNow(NodePort.Part part) {
        if (closed) {
            return;
        }
        SessionEndReason reason = part == NodePort.Part.FREESWITCH ? SessionEndReason.FREESWITCH_LOST
                : SessionEndReason.PARITY_MS_LOST;
        for (Session s : sessions.values()) {
            if (s.provisional) {
                continue;
            }
            fact(s, x -> {
                List<Ended> ended = new ArrayList<>();
                for (Leg l : x.liveLegs()) {
                    if (part == NodePort.Part.FREESWITCH) {
                        ended.add(new Ended(l.legId, EndCause.SYSTEM, 0, EndedBy.SYSTEM, clock.nowMs()));
                    } else {
                        try {
                            for (Ended e : node.hangup(l.legId, EndCause.SYSTEM, EndedBy.SYSTEM)) {
                                ended.add(e);
                            }
                        } catch (NodePort.Unavailable e) {
                            ended.add(new Ended(l.legId, EndCause.SYSTEM, 0, EndedBy.SYSTEM, clock.nowMs()));
                        }
                    }
                }
                if (!ended.isEmpty()) {
                    legsEnded(x, ended, reason, 0);
                }
            });
        }
    }

    private void ringTimeout(Session s, String legId) {
        withLock(s, x -> {
            Leg l = x.legs.get(legId);
            if (closed || l == null || (l.state != LegState.ORIGINATING && l.state != LegState.RINGING)) {
                return null;
            }
            try {
                legsEnded(x, node.hangup(legId, EndCause.NO_ANSWER, EndedBy.SYSTEM), SessionEndReason.ALL_LEGS_GONE, 0);
            } catch (NodePort.Unavailable e) {
                LOG.atWarn().addKeyValue("event", "leg.end_failed").addKeyValue("tenant_id", x.tenantId)
                        .addKeyValue("interaction_id", x.interactionId).addKeyValue("leg_id", legId)
                        .log("can't end a leg past its ring timeout");
            }
            return null;
        });
    }

    private interface LegAction {
        void apply(Session s, Leg l);
    }

    private void onLeg(String legId, LegAction action) {
        if (closed) {
            return;
        }
        Session s = byLeg.get(legId);
        if (s != null) {
            fact(s, x -> {
                Leg l = x.legs.get(legId);
                if (l != null) {
                    action.apply(x, l);
                }
            });
        }
    }

    // ---- Ends ------------------------------------------------------------------------------------

    /** Records ended legs (journal, events); ends the session when its last leg is gone. */
    private void legsEnded(Session s, List<Ended> ended, SessionEndReason reasonIfLast, int sipStatus) {
        for (Ended e : ended) {
            if (e == null) {
                continue;
            }
            Leg l = s.legs.get(e.legId());
            if (l == null || l.state == LegState.ENDED) {
                continue;
            }
            l.state = LegState.ENDED;
            l.cause = e.cause();
            l.sipStatus = e.sipStatus();
            l.endedBy = e.endedBy();
            l.endedAtMs = e.atMs();
            if (l.ringTimer != null) {
                l.ringTimer.cancel();
            }
            legPlaysEnded(l);
            journal.legEnded(new Journal.LegEnded(s.tenantId, s.interactionId, l.legId, l.role, l.endpoint,
                    l.sipCallId, l.cause, l.sipStatus, l.endedBy, l.answeredAtMs, l.endedAtMs, l.fingerprint));
            // Its recording's end comes first: IE's manifest is complete when the leg's end arrives.
            legRecordingsEnded(s, l);
            emit(s, legUpdate(l));
        }
        if (s.state != SessionState.ENDED && s.liveLegs().isEmpty()) {
            endSession(s, reasonIfLast, sipStatus);
        }
    }

    private void endSession(Session s, SessionEndReason reason, int sipStatus) {
        long now = clock.nowMs();
        s.state = SessionState.ENDED;
        s.endReason = reason;
        s.endSipStatus = sipStatus;
        s.endedAtMs = now;
        s.media = MediaKind.NONE;
        if (s.reofferTimer != null) {
            s.reofferTimer.cancel();
        }
        if (s.deadlineTimer != null) {
            s.deadlineTimer.cancel();
        }
        journal.sessionEnded(new Journal.SessionEnded(s.tenantId, s.interactionId, reason, sipStatus, now, s.fence,
                s.ownerReplica, s.ownerBin, s.fence == 0));
        // A recording still open (its leg never reported an end: the node lost) ends before the session.
        for (Session.Recording r : s.recordings.values()) {
            if (r.open) {
                finishRecording(s, r, null);
            }
        }
        LOG.atInfo().addKeyValue("event", "session.ended").addKeyValue("tenant_id", s.tenantId)
                .addKeyValue("interaction_id", s.interactionId).addKeyValue("owner_epoch", s.fence)
                .addKeyValue("reason", reason).addKeyValue("sip_status", sipStatus).log("a call's session ended");
        emit(s, new Events.SessionEnded(reason, sipStatus));
        if (s.fence == 0) {
            endOwnerless(s);
        } else {
            scheduleRelease(s, now + TOMBSTONE_MAX_MS);
        }
    }

    /**
     * TC-036: a session that ended with no owner. Its end went once to the replica its arrival
     * was offered to (if any); nothing of it waits for an ack or is replayed, to that replica or to
     * a restarted one (an ack at or past the end still counts, as for a dropped end). Its
     * tombstone lives {@code ownerlessTombstoneMs}, and the node keeps at most
     * {@code ownerlessTombstones} of them. A fence raised on it afterwards changes neither.
     */
    private void endOwnerless(Session s) {
        s.endedOwnerless = true;
        Pending end = s.unacked.peekLast();
        if (end != null && end.delivery.event() instanceof Events.SessionEnded) {
            s.droppedEndSeq = end.delivery.seq();
        }
        withdraw(s, p -> true);
        scheduleRelease(s, s.endedAtMs + config.limits().ownerlessTombstoneMs());
        queueOwnerless(s);
    }

    /** Lock-free: the node-wide order of ownerless tombstones; past the cap the oldest are released. */
    private void queueOwnerless(Session s) {
        ownerless.add(s);
        ownerlessQueued.incrementAndGet();
        // Released ones leave from the head (they end, and are released, in the same order).
        Session head;
        while ((head = ownerless.peek()) != null && sessions.get(key(head.tenantId, head.interactionId)) != head
                && ownerless.remove(head)) {
            ownerlessQueued.decrementAndGet();
        }
        while (ownerlessQueued.get() > config.limits().ownerlessTombstones()) {
            Session oldest = ownerless.poll();
            if (oldest == null) {
                break;
            }
            ownerlessQueued.decrementAndGet();
            if (oldest != s && sessions.get(key(oldest.tenantId, oldest.interactionId)) == oldest) {
                OWNERLESS_EVICTED.incrementAndGet();
                // Another session's turn: never nested in this one's (a timer turn runs it).
                clock.schedule(clock.nowMs(), () -> withLock(oldest, x -> {
                    release(x);
                    return null;
                }));
            }
        }
    }

    private void scheduleRelease(Session s, long atMs) {
        if (s.releaseTimer != null) {
            s.releaseTimer.cancel();
        }
        long at = s.endedOwnerless ? Math.min(atMs, s.endedAtMs + config.limits().ownerlessTombstoneMs()) : atMs;
        s.releaseTimer = clock.schedule(at, () -> withLock(s, x -> {
            release(x);
            return null;
        }));
    }

    /** Under the session's lock: its tombstone goes, here and in the journal. */
    private void release(Session x) {
        if (!closed && sessions.remove(key(x.tenantId, x.interactionId), x)) {
            if (x.releaseTimer != null) {
                x.releaseTimer.cancel();
            }
            x.legs.keySet().forEach(id -> byLeg.remove(id, x));
            withdraw(x, p -> true);
            journal.released(new Journal.Released(x.tenantId, x.interactionId));
        }
    }

    /** Ownerless tombstones released early by the node's cap, since the process started (a metric). */
    public static long ownerlessEvicted() {
        return OWNERLESS_EVICTED.get();
    }

    /** Sessions and tombstones this node holds (a gauge; tests). */
    public int sessionCount() {
        return sessions.size();
    }

    // ---- Rebuild ---------------------------------------------------------------------------------

    /** Every session from freeswitch's channels, its CDR files and the journal (TC PLAN "Recovery"). */
    private void rebuild() {
        Journal.Contents j = journal.load();
        Set<String> released = new HashSet<>();
        j.released().forEach(r -> released.add(key(r.tenantId(), r.interactionId())));
        Map<String, Journal.SessionEnded> endedSessions = new HashMap<>();
        j.sessions().forEach(e -> endedSessions.put(key(e.tenantId(), e.interactionId()), e));
        Map<String, Long> acked = new HashMap<>();
        j.acks().forEach(a -> acked.put(key(a.tenantId(), a.interactionId()), a.atMs()));
        List<Session> rebuiltOwnerless = new ArrayList<>();
        for (Channel ch : node.channels()) {
            String k = key(ch.tenantId, ch.interactionId);
            Session s = sessions.computeIfAbsent(k, x -> new Session(ch.tenantId, ch.interactionId, SessionState.ACTIVE));
            if (s.token == null && ch.sessionToken != null) {
                s.token = ch.sessionToken;
            }
            Leg l = new Leg(ch.legId, ch.role, ch.endpoint, ch.sipCallId, ch.inbound, ch.addLegFingerprint, ch.state);
            l.answeredAtMs = ch.answeredAtMs;
            s.legs.put(l.legId, l);
            byLeg.putIfAbsent(l.legId, s);
            if (ch.ownerEpoch > s.fence) {
                s.fence = ch.ownerEpoch;
                s.ownerReplica = ch.ownerReplicaId;
                s.ownerBin = ch.ownerBin;
            }
            if (ch.inbound && ch.role == Role.CUSTOMER) {
                s.arrivalBin = ch.bin;
                s.parkedAtMs = ch.createdAtMs;
                s.deadlineMs = ch.createdAtMs + ARRIVAL_DEADLINE_MS;
                s.arrivalTemplate = arrival(ch);
            }
        }
        List<Journal.LegEnded> endedLegs = new ArrayList<>(j.legs());
        endedLegs.addAll(node.cdrs(newest(j)));
        for (Journal.LegEnded e : endedLegs) {
            String k = key(e.tenantId(), e.interactionId());
            if (released.contains(k)) {
                continue;
            }
            Session s = sessions.computeIfAbsent(k, x -> new Session(e.tenantId(), e.interactionId(), SessionState.ENDED));
            if (s.legs.containsKey(e.legId())) {
                continue;
            }
            Leg l = new Leg(e.legId(), e.role(), e.endpoint(), e.sipCallId(), e.role() == Role.CUSTOMER
                    && e.addLegFingerprint() == null, e.addLegFingerprint(), LegState.ENDED);
            l.cause = e.cause();
            l.sipStatus = e.sipStatus();
            l.endedBy = e.endedBy();
            l.answeredAtMs = e.answeredAtMs();
            l.endedAtMs = e.endedAtMs();
            s.legs.put(l.legId, l);
            byLeg.putIfAbsent(l.legId, s);
        }
        long now = clock.nowMs();
        for (Session s : sessions.values()) {
            if (s.token == null) {
                s.token = UuidV7.create();
            }
            String k = key(s.tenantId, s.interactionId);
            List<Leg> live = s.liveLegs();
            if (!live.isEmpty()) {
                boolean active = live.stream().anyMatch(l -> l.state == LegState.ANSWERED || !l.inbound);
                s.state = active ? SessionState.ACTIVE : SessionState.ARRIVING;
                s.claimed = s.fence > 0;
                long mediaEpoch = node.mediaEpoch(s.interactionId);
                s.media = mediaEpoch < 0 ? MediaKind.NONE
                        : live.size() >= 3 ? MediaKind.CONFERENCE : MediaKind.CALL;
                continue;
            }
            s.state = SessionState.ENDED;
            Journal.SessionEnded e = endedSessions.get(k);
            if (e != null) {
                s.endReason = e.reason();
                s.endSipStatus = e.sipStatus();
                s.endedAtMs = e.endedAtMs();
                if (e.ownerEpoch() >= s.fence) {
                    s.fence = e.ownerEpoch();
                    s.ownerReplica = e.ownerReplicaId();
                    s.ownerBin = e.ownerBin();
                }
            } else {
                s.endReason = SessionEndReason.ALL_LEGS_GONE;
                s.endedAtMs = s.legs.values().stream().mapToLong(l -> l.endedAtMs).max().orElse(now);
                journal.sessionEnded(new Journal.SessionEnded(s.tenantId, s.interactionId, s.endReason, 0,
                        s.endedAtMs, s.fence, s.ownerReplica, s.ownerBin));
            }
            Long ackedAt = acked.get(k);
            s.endAcked = ackedAt != null;
            if (e != null && e.ownerless()) {
                // TC-036: the journal says it ended with no owner: the short life, from its end; never
                // sent again. A tombstone rebuilt from CDR files alone has an unknown owner: 24 h.
                s.endedOwnerless = true;
                rebuiltOwnerless.add(s);
            }
            scheduleRelease(s, s.endAcked ? ackedAt + TOMBSTONE_AFTER_ACK_MS : s.endedAtMs + TOMBSTONE_MAX_MS);
        }
        rebuiltOwnerless.sort(java.util.Comparator.comparingLong(x -> x.endedAtMs));
        rebuiltOwnerless.forEach(this::queueOwnerless);
    }

    /** The journal's newest record (ms); with none, ten minutes ago. CDR files from before it are known. */
    private long newest(Journal.Contents j) {
        long t = 0;
        for (Journal.LegEnded e : j.legs()) {
            t = Math.max(t, e.endedAtMs());
        }
        for (Journal.SessionEnded e : j.sessions()) {
            t = Math.max(t, e.endedAtMs());
        }
        for (Journal.Acked a : j.acks()) {
            t = Math.max(t, a.atMs());
        }
        return t > 0 ? t : clock.nowMs() - 600_000;
    }

    /** The rebuild's end: tombstones not acknowledged are sent again, recent arrivals offered again. */
    private void finishRebuild() {
        if (closed) {
            return;
        }
        rebuilt = true;
        broadcastNodeStatus();
        long now = clock.nowMs();
        for (Session s : sessions.values()) {
            withLock(s, x -> {
                // A session that ended in an earlier run gets its end again; one that ended in this
                // run while the rebuild finished (a node event, §2.4) has already sent it.
                boolean endSentThisRun = x.droppedEndSeq > 0
                        || x.unacked.stream().anyMatch(p -> p.delivery.event() instanceof Events.SessionEnded);
                if (x.state == SessionState.ENDED && !x.endAcked && !endSentThisRun && !x.endedOwnerless) {
                    emit(x, new Events.SessionEnded(x.endReason, x.endSipStatus));
                } else if (x.state == SessionState.ARRIVING && !x.claimed) {
                    if (now >= x.deadlineMs || now - x.parkedAtMs >= REOFFER_MAX_AGE_MS) {
                        arrivalDeadline(x);
                    } else {
                        offer(x);
                        x.deadlineTimer = clock.schedule(x.deadlineMs, () -> arrivalDeadline(x));
                    }
                }
                return null;
            });
        }
        streams.values().forEach(this::attach);
    }

    // ---- Views and invariants --------------------------------------------------------------------

    private SessionView view(Session s) {
        List<LegView> legs = s.legs.values().stream().map(l -> new LegView(l.legId, l.role, l.state, l.endpoint,
                l.sipCallId, l.cause, l.sipStatus, l.endedBy, l.answeredAtMs, l.endedAtMs)).toList();
        return new SessionView(s.tenantId, s.interactionId, s.fence, s.ownerReplica, s.state, s.media, s.onHold, legs,
                s.nextSeq - 1, s.endReason, s.endedAtMs,
                s.state == SessionState.ARRIVING && !s.claimed ? s.deadlineMs : 0);
    }

    /** What a script's end checks: the fence is the same on every live leg, in parity-ms and here. */
    public List<String> invariantViolations() {
        List<String> out = new ArrayList<>(node.invariantViolations());
        Map<String, Channel> channels = new HashMap<>();
        node.channels().forEach(c -> channels.put(c.legId, c));
        for (Session s : sessions.values()) {
            withLock(s, x -> {
                if (x.fence == 0) {
                    return null;
                }
                for (Leg l : x.liveLegs()) {
                    Channel c = channels.get(l.legId);
                    if (c != null && c.ownerEpoch != x.fence) {
                        out.add("leg " + l.legId + " has cc_owner_epoch " + c.ownerEpoch + ", the fence is " + x.fence);
                    }
                }
                long media = node.mediaEpoch(x.interactionId);
                if (media >= 0 && media != x.fence) {
                    out.add("session " + x.interactionId + " has media epoch " + media + ", the fence is " + x.fence);
                }
                return null;
            });
        }
        return out;
    }

    // ---- Helpers ---------------------------------------------------------------------------------

    private static LegUpdate legUpdate(Leg l) {
        return new LegUpdate(l.legId, l.role, l.state, l.state == LegState.ENDED ? l.cause : null,
                l.state == LegState.ENDED ? l.sipStatus : 0, l.state == LegState.ENDED ? l.endedBy : null,
                l.answeredAtMs);
    }

    private static Leg leg(Session s, String legId) throws TelephonyException {
        Leg l = s.legs.get(legId);
        if (l == null) {
            throw new TelephonyException(Code.LEG_UNKNOWN);
        }
        return l;
    }

    private static void notEnded(Session s) throws TelephonyException {
        if (s.state == SessionState.ENDED) {
            throw new TelephonyException(Code.SESSION_ENDED);
        }
    }

    /** Identity, on purpose: one stream object per Watch call, one sink per stream. */
    @SuppressWarnings("ReferenceEquality")
    private static boolean same(Object a, Object b) {
        return a == b;
    }

    static String key(String tenantId, String interactionId) {
        return tenantId + "/" + interactionId;
    }

    @FunctionalInterface
    private interface NodeOp<T> {
        T call() throws NodePort.Unavailable;
    }

    @FunctionalInterface
    private interface NodeVoid {
        void run() throws NodePort.Unavailable;
    }

    private static <T> T nodeCall(NodeOp<T> op) throws TelephonyException {
        try {
            return op.call();
        } catch (NodePort.Unavailable e) {
            throw new TelephonyException(nodeCode(e));
        }
    }

    /** The contract's code for a part of the node that failed. */
    private static Code nodeCode(NodePort.Unavailable e) {
        return switch (e.part()) {
            case FREESWITCH -> Code.FREESWITCH_UNAVAILABLE;
            case PARITY_MS -> Code.PARITY_MS_UNAVAILABLE;
            case PROMPT -> Code.PROMPT_UNKNOWN;
            case RECORDING -> Code.INVALID_STATE;
        };
    }

    private static void nodeRun(NodeVoid op) throws TelephonyException {
        nodeCall(() -> {
            op.run();
            return null;
        });
    }

    /** A caller's turn (a command, claim, snapshot or decline): refused with session_busy when full. */
    private static <R> R locked(Session s, Body<R> body) throws TelephonyException {
        return bounded(s, () -> body.run(s));
    }

    private static <R> R bounded(Session s, Mailbox.Turn<R> turn) throws TelephonyException {
        try {
            return s.mailbox.call(turn, true);
        } catch (Mailbox.Busy e) {
            throw new TelephonyException(Code.SESSION_BUSY);
        }
    }

    /** The engine's own turn (a timer, a stream's replay, a node fact delivered inline): never refused. */
    private static <R> R withLock(Session s, Function<Session, R> f) {
        try {
            return s.mailbox.call(() -> f.apply(s), false);
        } catch (Mailbox.Busy | TelephonyException e) {
            throw new IllegalStateException("an unbounded turn was refused", e);
        }
    }

    /**
     * A node fact for a session: inline when the node calls on a test's thread, else posted (ESL's
     * reader never waits). A full mailbox drops it and marks the session for a resync (§2.1).
     */
    private void fact(Session s, java.util.function.Consumer<Session> f) {
        if (node.callbacksInline()) {
            withLock(s, x -> {
                f.accept(x);
                return null;
            });
            return;
        }
        try {
            s.mailbox.post(() -> {
                if (!closed && !s.retired) {
                    resyncIfNeeded(s);
                    f.accept(s);
                }
                return null;
            }, true);
        } catch (Mailbox.Busy e) {
            OVERFLOWS.incrementAndGet();
            s.needsResync = true;
            LOG.atWarn().addKeyValue("event", "mailbox.overflowed").addKeyValue("tenant_id", s.tenantId)
                    .addKeyValue("interaction_id", s.interactionId)
                    .log("a session's mailbox is full: a node fact was dropped; the session resyncs");
        }
    }

    /**
     * After a dropped fact, a session's legs are read again from the node: a leg gone from it ended
     * (cause unknown: NORMAL); a leg further on than recorded moves forward (§2.3: never back).
     */
    private void resyncIfNeeded(Session s) {
        if (!s.needsResync) {
            return;
        }
        s.needsResync = false;
        List<Ended> gone = new ArrayList<>();
        for (Leg l : s.liveLegs()) {
            Optional<Channel> ch = node.channel(l.legId);
            if (ch.isEmpty()) {
                gone.add(new Ended(l.legId, EndCause.NORMAL, 0, EndedBy.REMOTE, clock.nowMs()));
            } else if (ch.get().state == LegState.ANSWERED && l.state != LegState.ANSWERED) {
                l.state = LegState.ANSWERED;
                l.answeredAtMs = ch.get().answeredAtMs;
                emit(s, legUpdate(l));
            }
        }
        if (!gone.isEmpty()) {
            legsEnded(s, gone, SessionEndReason.ALL_LEGS_GONE, 0);
        }
    }

    /** A session another command may see: committed, not provisional. */
    private Session visible(String key) throws TelephonyException {
        Session s = sessions.get(key);
        if (s == null || s.provisional) {
            throw new TelephonyException(Code.SESSION_UNKNOWN);
        }
        return s;
    }

    /** A command or claim starts: refused once the run is closing; close() waits for it to finish. */
    private void enter() throws TelephonyException {
        turns.lock();
        try {
            if (closed) {
                throw new TelephonyException(Code.REBUILDING);
            }
            inflight++;
        } finally {
            turns.unlock();
        }
    }

    private void leave() {
        turns.lock();
        try {
            inflight--;
            if (inflight == 0) {
                idle.signalAll();
            }
        } finally {
            turns.unlock();
        }
    }

    /** Runs a node event now, or keeps it until the rebuild has its snapshot (§2.4). */
    private void nodeEvent(Runnable r) {
        bufferLock.lock();
        try {
            if (buffering) {
                heldEvents.add(r);
                return;
            }
        } finally {
            bufferLock.unlock();
        }
        r.run();
    }

    /** The rebuild is done: what the node reported meanwhile is applied, in order (merges are monotonic). */
    private void drainBuffer() {
        bufferLock.lock();
        try {
            Runnable r;
            while ((r = heldEvents.poll()) != null) {
                r.run();
            }
            buffering = false;
        } finally {
            bufferLock.unlock();
        }
    }

    /** Test hook (the fake's {@code mailbox_full} / {@code mailbox_free}): a session's capacity for commands. */
    public void mailboxCapacity(String tenantId, String interactionId, int capacity) {
        Session s = sessions.get(key(tenantId, interactionId));
        if (s != null) {
            s.mailbox.capacity(capacity);
        }
    }
}
