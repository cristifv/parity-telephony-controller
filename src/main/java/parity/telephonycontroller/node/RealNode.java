package parity.telephonycontroller.node;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.spi.LoggingEventBuilder;
import parity.telephonycontroller.esl.EslClient;
import parity.telephonycontroller.esl.NodeEvent;
import parity.telephonycontroller.journal.Journal;
import parity.telephonycontroller.model.Kinds.EndCause;
import parity.telephonycontroller.model.Kinds.EndedBy;
import parity.telephonycontroller.model.Kinds.Endpoint;
import parity.telephonycontroller.model.Kinds.LegState;
import parity.telephonycontroller.model.Kinds.RemoteMediaKind;
import parity.telephonycontroller.model.Kinds.Role;
import parity.telephonycontroller.model.UuidV7;
import parity.telephonycontroller.time.NodeClock;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * The media node for real (TC ADR 0002 §3–§6): freeswitch through {@link EslClient}, the media
 * through a {@link MediaPort}.
 *
 * <ul>
 *   <li><b>Events.</b> ESL's reader calls {@link #event}, which never waits: it updates this node's
 *       view of its legs and hands the engine its facts (the engine posts them to session mailboxes);
 *       anything that needs freeswitch or parity-ms runs on a virtual thread, one leg at a time.
 *   <li><b>Subscribe, snapshot, apply</b> (§2.4). Each ESL cycle: events are buffered from the
 *       subscription on; {@link #up} snapshots the channels ({@code show channels},
 *       {@code uuid_dump}), then applies the buffer. A new {@code Core-UUID} means freeswitch
 *       restarted: every leg is gone ({@link Listener#lost}).
 *   <li><b>Arrivals</b> park in the dialplan; accepting one writes TC's session token and the
 *       hangup hook, read back (R1-1); the first verified command deletes the safety hangup (counted
 *       only with {@code Deleted: ≥1}) and arms a backstop until the answer or reject (R1-4).
 *   <li><b>Originated legs</b> use the loopback carrier (§4): {@code bgapi originate} of a loopback
 *       pair, the bridge from loopback-b with parity-ms's offer as its remote SDP, and the pair torn
 *       down once the far end answered and parity-ms has its answer.
 *   <li><b>Re-INVITEs</b> ((c+), §5): freeswitch answers them; the hook tells TC, which re-reads
 *       {@code switch_r_sdp} until its {@code o=} version advances and moves the media if needed.
 * </ul>
 *
 * Leg ids are {@code cc_leg_id} (R1-13): an arrival's uuid in production; an originated leg's uuid
 * is its leg id. Nothing here logs a number, an SDP or a variable's value.
 */
public final class RealNode implements NodePort, EslClient.Handler, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RealNode.class);
    private static final AtomicLong SEQ = new AtomicLong();
    private static final JsonMapper JSON = JsonMapper.builder().build();

    /** The real node's settings. */
    public record Config(int maxLegs, Duration answerTimeout, int claimedBackstopS, String agentProxy,
            AgentRegistrar agentRegistrar, Map<String, String> trunks, Path cdrDir, int canaryLow, int canaryHigh,
            boolean testHeaders) {}

    /** A record about leg {@code l}'s call (log-record.v1): its tenant, xsid (the interaction) and leg. */
    private static LoggingEventBuilder about(LoggingEventBuilder b, Leg l) {
        return b.addKeyValue("tenant_id", l.tenantId).addKeyValue("interaction_id", l.interactionId)
                .addKeyValue("leg_id", l.legId);
    }

    /** A leg TC manages, as this node knows it. */
    private static final class Leg {
        final String legId;
        final String interactionId;
        final String tenantId;
        final String bin;
        final Role role;
        final Endpoint endpoint;
        final boolean inbound;
        final long createdAtMs;
        final String calledNumber;
        final String callerNumber;
        final String trunkId;
        final String sipCallId;
        final String fingerprint;
        final long seq = SEQ.incrementAndGet();
        final ReentrantLock work = new ReentrantLock();
        volatile String uuid;
        volatile LegState state;
        volatile long answeredAtMs;
        volatile long ownerEpoch;
        volatile String ownerReplica;
        volatile String ownerBin;
        volatile String token;
        volatile String remoteSdp;
        volatile boolean backstop;
        volatile boolean ended;
        volatile boolean ringReported;
        volatile boolean killedByTc;
        // Originated legs: the loopback pair and what loopback-b sends.
        volatile String loopbackA;
        volatile String loopbackB;
        volatile String offer;
        volatile String target;
        volatile int ringTimeoutS;
        volatile String callerId;
        /** The epoch the far end's channel was created with (its dial string). */
        volatile long bakedEpoch;
        /** The far end answered and parity-ms has the answer; the bridge from loopback-b is up. */
        volatile boolean mediaReady;
        volatile boolean bridged;
        volatile boolean tornDown;
        /** Its media was released (detached, or the call ended with it). */
        volatile boolean mediaReleased;

        Leg(String legId, String interactionId, String tenantId, String bin, Role role, Endpoint endpoint,
                boolean inbound, long createdAtMs, String calledNumber, String callerNumber, String trunkId,
                String sipCallId, String fingerprint, LegState state) {
            this.legId = legId;
            this.interactionId = interactionId;
            this.tenantId = tenantId;
            this.bin = bin;
            this.role = role;
            this.endpoint = endpoint;
            this.inbound = inbound;
            this.createdAtMs = createdAtMs;
            this.calledNumber = calledNumber;
            this.callerNumber = callerNumber;
            this.trunkId = trunkId;
            this.sipCallId = sipCallId;
            this.fingerprint = fingerprint;
            this.state = state;
        }

        Channel channel() {
            Channel c = new Channel(legId, tenantId, interactionId, bin, role, endpoint, sipCallId, inbound,
                    createdAtMs, calledNumber, callerNumber, trunkId, List.of(), fingerprint, state);
            c.answeredAtMs = answeredAtMs;
            c.ownerEpoch = ownerEpoch;
            c.ownerReplicaId = ownerReplica;
            c.ownerBin = ownerBin;
            c.sessionToken = token;
            return c;
        }
    }

    private final Config config;
    private final EslClient esl;
    private final MediaPort media;
    private final NodeClock clock;
    private final ConcurrentHashMap<String, Leg> legs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Leg> byUuid = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Leg> byLoopback = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CompletableFuture<Void>> answers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Integer> originates = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, List<Runnable>> heldCoupled = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<NodeEvent> buffer = new ConcurrentLinkedQueue<>();
    private final ReentrantLock bufferLock = new ReentrantLock();
    private final ReentrantLock readyLock = new ReentrantLock();
    private final Condition readyChanged = readyLock.newCondition();
    private volatile boolean buffering = true;
    private volatile boolean ready;
    private volatile String coreUuid;
    private volatile Listener listener;
    private volatile int maxLegs;
    private volatile boolean partitioned;
    /** Test hook (the G-3 measurement): leave each loopback pair up instead of tearing it down. */
    private volatile boolean keepLoopback;
    /** When the node was last fully in sync (ready, late snapshots done); -1 while it isn't. */
    private volatile long syncedAtNanos = -1;
    private final AtomicInteger snapshotFailures = new AtomicInteger();
    private final AtomicLong snapshotRetries = new AtomicLong();
    private volatile boolean closed;
    // What the engine has been told, per leg (the real-node runner waits on it).
    private final ConcurrentHashMap<String, LegState> reported = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Integer> mediaReports = new ConcurrentHashMap<>();

    // Metrics.
    final AtomicLong reinvites = new AtomicLong();
    final AtomicLong reinviteUnanswerable = new AtomicLong();
    final AtomicLong reinviteUnchangedIdentical = new AtomicLong();
    final AtomicLong reinviteUnchangedChanged = new AtomicLong();
    final AtomicLong safetyHangupMissing = new AtomicLong();
    final AtomicLong hookMismatches = new AtomicLong();
    final AtomicLong canaryPorts = new AtomicLong();

    public RealNode(Config config, EslClient esl, MediaPort media, NodeClock clock) {
        this.config = config;
        this.esl = esl;
        this.media = media;
        this.clock = clock;
        this.maxLegs = config.maxLegs();
        media.subscribe(new MediaEvents());
        Thread.ofVirtual().name("tc-canary").start(this::canary);
    }

    /** Starts ESL; returns once the first snapshot is in, or after {@code timeout}. */
    public boolean start(Duration timeout) throws InterruptedException {
        esl.start(this);
        long until = System.nanoTime() + timeout.toNanos();
        readyLock.lock();
        try {
            while (!ready) {
                long left = until - System.nanoTime();
                if (left <= 0) {
                    return false;
                }
                readyChanged.awaitNanos(left);
            }
            return true;
        } finally {
            readyLock.unlock();
        }
    }

    public boolean live() {
        return ready && esl.live() && !partitioned;
    }

    /** Test hook: TC's own capacity limit (the scripts' {@code capacity}). */
    public void setMaxLegs(int n) {
        maxLegs = n;
    }

    /** Test hook: freeswitch can't be reached (a script's {@code freeswitch_unreachable}). */
    public void setPartitioned(boolean p) {
        partitioned = p;
    }

    public EslClient esl() {
        return esl;
    }

    @Override
    public void close() {
        closed = true;
        esl.close();
    }

    // ---- ESL's cycle ------------------------------------------------------------------------------

    @Override
    public void connecting() {
        // Not ready until a full snapshot of this connection succeeds (must-fix 1).
        ready = false;
        syncedAtNanos = -1;
        bufferLock.lock();
        try {
            buffering = true;
            buffer.clear();
        } finally {
            bufferLock.unlock();
        }
    }

    @Override
    public void up(String core) {
        String before = coreUuid;
        coreUuid = core;
        if (before != null && !before.equals(core)) {
            LOG.atWarn().addKeyValue("event", "freeswitch.restarted").log("freeswitch restarted (a new Core-UUID): every leg is gone");
            List<Leg> gone = new ArrayList<>(legs.values());
            legs.clear();
            byUuid.clear();
            byLoopback.clear();
            gone.forEach(l -> l.ended = true);
            Listener li = listener;
            if (li != null && !gone.isEmpty()) {
                li.lost(Part.FREESWITCH);
            }
        }
        // A full snapshot or nothing (must-fix 1): a partial one would rebuild sessions without
        // their legs, IE would end them, and the media sweep would cut live calls. Retried with
        // backoff; the node stays not ready (commands get FREESWITCH_UNAVAILABLE, the engine waits).
        long backoff = 100;
        while (true) {
            if (closed || !core.equals(coreUuid) || !esl.live()) {
                return;
            }
            try {
                if (snapshotFailures.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                    throw new EslClient.Timeout();
                }
                snapshot(before != null && before.equals(core), false);
                break;
            } catch (EslClient.Unavailable | EslClient.Timeout | RuntimeException e) {
                snapshotRetries.incrementAndGet();
                LOG.atWarn().addKeyValue("event", "channel_snapshot.failed").addKeyValue("error_kind", e.getClass().getSimpleName())
                        .addKeyValue("retry_ms", backoff).log("the channel snapshot failed: not ready; retrying");
                sleep(backoff);
                backoff = Math.min(backoff * 2, 5_000);
            }
        }
        String run = core;
        Thread.ofVirtual().name("tc-late-snapshot").start(() -> lateSnapshots(run));
        bufferLock.lock();
        try {
            NodeEvent e;
            while ((e = buffer.poll()) != null) {
                handle(e);
            }
            buffering = false;
        } finally {
            bufferLock.unlock();
        }
        readyLock.lock();
        try {
            ready = true;
            readyChanged.signalAll();
        } finally {
            readyLock.unlock();
        }
    }

    @Override
    public void event(NodeEvent e) {
        if (buffering) {
            bufferLock.lock();
            try {
                if (buffering) {
                    buffer.add(e);
                    return;
                }
            } finally {
                bufferLock.unlock();
            }
        }
        handle(e);
    }

    @Override
    public void down() {
        // Commands fail fast while ESL is down (EslClient.Unavailable); the next up() resyncs.
    }

    /**
     * The channels freeswitch has now; on a reconnect, what ended meanwhile is reported (§2.4).
     *
     * <p>{@code show channels} reads freeswitch's core database, which its SQL thread fills behind
     * the session table: a channel created in the last moments may be missing (a restart during a
     * burst lost legs that way, R1-14). So a leg missing from it is ended only once
     * {@code uuid_exists} says so, and {@link #lateSnapshots} looks again.
     *
     * @param late a later look: what it finds is reported to the engine as discovered
     */
    private void snapshot(boolean reconnect, boolean late) throws EslClient.Unavailable, EslClient.Timeout {
        JsonNode rows = JSON.readTree(esl.api("show channels as json")).path("rows");
        java.util.Set<String> seen = new java.util.HashSet<>();
        List<JsonNode> loopbacks = new ArrayList<>();
        List<Leg> found = new ArrayList<>();
        for (JsonNode row : rows) {
            String uuid = row.path("uuid").asString("");
            if (uuid.isEmpty()) {
                continue;
            }
            seen.add(uuid);
            if (byUuid.containsKey(uuid) || byLoopback.containsKey(uuid)) {
                continue;
            }
            String dump = esl.api("uuid_dump " + uuid + " json");
            if (dump.startsWith("-ERR")) {
                continue;
            }
            JsonNode v = JSON.readTree(dump);
            if (!"true".equals(text(v, "variable_cc_managed"))) {
                continue;
            }
            // Loopback legs attach to their leg, so they go second (the listing's order is arbitrary).
            String role = text(v, "variable_cc_role");
            if (role != null && role.startsWith("loopback")) {
                loopbacks.add(v);
                continue;
            }
            Leg l = adopt(uuid, v);
            if (l != null) {
                found.add(l);
            }
        }
        for (JsonNode v : loopbacks) {
            adopt(text(v, "Unique-ID"), v);
        }
        if (reconnect) {
            for (Leg l : List.copyOf(legs.values())) {
                if (l.uuid != null && !seen.contains(l.uuid) && !l.ended && !exists(l.uuid)) {
                    ended(l, EndCause.NORMAL, 0, EndedBy.REMOTE);
                }
            }
        }
        Listener li = listener;
        if (late && li != null) {
            for (Leg l : found) {
                about(LOG.atWarn(), l).addKeyValue("event", "leg.adopted_late").log("a leg freeswitch's listing showed late: adopted now");
                li.discovered(l.channel());
            }
        }
    }

    /** {@code uuid_exists} (the session table, not the database); true when it can't be asked. */
    private boolean exists(String uuid) {
        try {
            return "true".equals(esl.api("uuid_exists " + uuid).trim());
        } catch (EslClient.Unavailable | EslClient.Timeout e) {
            return true;
        }
    }

    /**
     * Looks again 1 s, 3 s and 10 s after ESL came up, for channels the first listing missed; then
     * the node counts as in sync (the media sweep's clock starts).
     */
    private void lateSnapshots(String core) {
        for (long wait : new long[] {1_000, 2_000, 7_000}) {
            sleep(wait);
            if (closed || !core.equals(coreUuid) || !ready) {
                return;
            }
            try {
                snapshot(false, true);
            } catch (EslClient.Unavailable | EslClient.Timeout | RuntimeException e) {
                LOG.atWarn().addKeyValue("event", "channel_snapshot.failed").addKeyValue("error_kind", e.getClass().getSimpleName())
                        .log("a late channel snapshot failed");
            }
        }
        if (!closed && core.equals(coreUuid) && ready) {
            syncedAtNanos = System.nanoTime();
        }
    }

    /** Test hook: the next {@code n} snapshots at a (re)connect fail as timed out. */
    public void failNextSnapshots(int n) {
        snapshotFailures.set(n);
    }

    /** Snapshots retried at a (re)connect (a metric and a test hook). */
    public long snapshotRetries() {
        return snapshotRetries.get();
    }

    /** A managed channel from a {@code uuid_dump}: an arrival, an originated leg, or a loopback leg. */
    private Leg adopt(String uuid, JsonNode v) {
        String role = text(v, "variable_cc_role");
        String legId = text(v, "variable_cc_leg_id");
        if (uuid == null || role == null || legId == null || (legs.containsKey(legId) && !role.startsWith("loopback"))) {
            return null;
        }
        if (role.startsWith("loopback")) {
            Leg owner = legs.get(legId);
            if (owner != null) {
                byLoopback.put(uuid, owner);
                if (role.equals("loopback-a")) {
                    owner.loopbackA = uuid;
                } else {
                    owner.loopbackB = uuid;
                }
            }
            return null;
        }
        boolean inbound = role.equals("arrival");
        Leg l = new Leg(legId, text(v, "variable_cc_interaction_id"), text(v, "variable_cc_tenant_id"),
                text(v, "variable_cc_bin"), inbound ? Role.CUSTOMER : role(role),
                endpoint(text(v, "variable_cc_endpoint"), inbound), inbound, clock.nowMs(),
                text(v, "Caller-Destination-Number"), text(v, "Caller-Caller-ID-Number"),
                text(v, "variable_cc_trunk_id"), text(v, "variable_sip_call_id"), text(v, "variable_cc_fingerprint"),
                state(text(v, "Answer-State"), inbound));
        l.uuid = uuid;
        l.ownerEpoch = parseLong(text(v, "variable_cc_owner_epoch"));
        l.ownerReplica = text(v, "variable_cc_owner_replica");
        l.ownerBin = text(v, "variable_cc_owner_bin");
        l.token = text(v, "variable_cc_session_token");
        l.remoteSdp = text(v, "variable_switch_r_sdp");
        legs.put(legId, l);
        byUuid.put(uuid, l);
        return l;
    }

    // ---- events ---------------------------------------------------------------------------------

    private void handle(NodeEvent e) {
        try {
            switch (e.name()) {
                case "CHANNEL_PARK" -> onPark(e);
                case "CHANNEL_PROGRESS", "CHANNEL_PROGRESS_MEDIA" -> onProgress(e);
                case "CHANNEL_ANSWER" -> onAnswer(e);
                case "CHANNEL_EXECUTE_COMPLETE" -> onExecuteComplete(e);
                case "CHANNEL_BRIDGE" -> onBridge(e);
                case "CHANNEL_HANGUP_COMPLETE" -> onHangup(e);
                case "CUSTOM" -> {
                    if ("parity::reinvite".equals(e.subclass())) {
                        onReinvite(e);
                    }
                }
                default -> { }
            }
        } catch (RuntimeException ex) {
            // The class only: a message could carry the event's data (a digit, a number).
            LOG.atError().addKeyValue("event", "esl_event.failed").addKeyValue("esl_event", e.name())
                    .addKeyValue("error_kind", ex.getClass().getSimpleName()).log("an ESL event couldn't be handled");
        }
    }

    private void onPark(NodeEvent e) {
        String uuid = e.uuid();
        String role = e.cc("role");
        if (uuid == null || role == null) {
            return;
        }
        switch (role) {
            case "arrival" -> arrival(uuid, e);
            case "loopback-b" -> loopbackParked(uuid, e);
            default -> { }
        }
    }

    private void arrival(String uuid, NodeEvent e) {
        if (byUuid.containsKey(uuid)) {
            return;
        }
        String legId = e.cc("leg_id");
        if (legId == null || !FsText.UUID.matcher(legId).matches()) {
            legId = uuid;
        }
        String interaction = e.cc("interaction_id");
        if (interaction == null || !FsText.UUID.matcher(interaction).matches()) {
            // No X-Interaction-ID: TC makes one; accepted() writes it before the offer (§3).
            interaction = UuidV7.create();
        }
        Leg l = new Leg(legId, interaction, e.cc("tenant_id"), e.cc("bin"), Role.CUSTOMER, Endpoint.SIP, true,
                clock.nowMs(), e.get("Caller-Destination-Number"), e.get("Caller-Caller-ID-Number"), e.cc("trunk_id"),
                e.get("variable_sip_call_id"), null, LegState.PARKED);
        l.uuid = uuid;
        l.remoteSdp = e.get("variable_switch_r_sdp");
        if (legs.putIfAbsent(legId, l) != null) {
            // Another live leg already has this leg id: refused (503) by the engine.
            Leg other = new Leg(UUID.randomUUID().toString(), interaction, l.tenantId, l.bin, Role.CUSTOMER,
                    Endpoint.SIP, true, l.createdAtMs, null, null, null, null, null, LegState.PARKED);
            other.uuid = uuid;
            byUuid.put(uuid, other);
            work(other, () -> respond(uuid, 503));
            return;
        }
        byUuid.put(uuid, l);
        Listener li = listener;
        if (li != null) {
            li.parked(l.channel());
        }
        reported.put(legId, LegState.PARKED);
    }

    private void loopbackParked(String uuid, NodeEvent e) {
        Leg l = legs.get(e.cc("leg_id"));
        if (l == null || l.ended) {
            return;
        }
        l.loopbackB = uuid;
        byLoopback.put(uuid, l);
        work(l, () -> {
            try {
                ok(esl.execute(uuid, "set", "switch_r_sdp=" + FsText.sdp(l.offer)));
                ok(esl.execute(uuid, "set", "bypass_media=true"));
                ok(esl.execute(uuid, "bridge", bridgeString(l)));
            } catch (EslClient.Unavailable | EslClient.Timeout | FsText.Refused e2) {
                about(LOG.atWarn(), l).addKeyValue("event", "leg.bridge_failed").log("the loopback carrier couldn't bridge: the leg fails");
                kill(l.loopbackA, "NORMAL_TEMPORARY_FAILURE");
                ended(l, EndCause.UNREACHABLE, 0, EndedBy.SYSTEM);
            }
        });
    }

    private void onProgress(NodeEvent e) {
        Leg l = byUuid.get(e.uuid());
        if (l == null || l.inbound || l.ended || l.ringReported) {
            return;
        }
        l.ringReported = true;
        catchUpFence(l);
        if (l.state == LegState.ORIGINATING) {
            l.state = LegState.RINGING;
        }
        Listener li = listener;
        if (li != null) {
            li.ringing(l.legId);
        }
        reported.put(l.legId, LegState.RINGING);
    }

    private void onAnswer(NodeEvent e) {
        String uuid = e.uuid();
        CompletableFuture<Void> waiter = answers.remove(uuid);
        if (waiter != null) {
            waiter.complete(null);
        }
        Leg l = byUuid.get(uuid);
        if (l == null || l.inbound || l.ended || l.state == LegState.ANSWERED) {
            return;
        }
        String answer = e.get("variable_switch_r_sdp");
        catchUpFence(l);
        work(l, () -> {
            try {
                if (answer != null) {
                    media.complete(l.tenantId, l.interactionId, l.legId, answer, l.ownerEpoch);
                    l.remoteSdp = answer;
                }
            } catch (Unavailable ex) {
                about(LOG.atWarn(), l).addKeyValue("event", "leg.answer_failed").log("parity-ms couldn't take an answer: the leg ends");
                kill(uuid, "MEDIA_TIMEOUT");
                ended(l, EndCause.MEDIA_FAILED, 0, EndedBy.SYSTEM);
                return;
            }
            // The loopback pair goes once parity-ms has the answer and loopback-b's bridge is up (§4):
            // the far end's leg then parks (park_after_bridge) without a re-INVITE. Killing it while
            // the bridge still originates cancels the far end (ORIGINATOR_CANCEL): the call drops.
            l.mediaReady = true;
            teardownIfReady(l);
            if (l.role == Role.CUSTOMER && l.token != null) {
                writeHook(l);
            }
            l.state = LegState.ANSWERED;
            l.answeredAtMs = clock.nowMs();
            Listener li = listener;
            if (li != null) {
                li.answered(l.legId, l.answeredAtMs);
            }
            reported.put(l.legId, LegState.ANSWERED);
        });
    }

    /** loopback-b's bridge to the far end is up: once the far end answered too, the pair can go. */
    private void onBridge(NodeEvent e) {
        Leg l = byLoopback.get(e.uuid());
        if (l == null) {
            l = byUuid.get(e.uuid());
        }
        if (l == null || l.inbound || l.ended) {
            return;
        }
        l.bridged = true;
        Leg leg = l;
        work(leg, () -> teardownIfReady(leg));
    }

    private void teardownIfReady(Leg l) {
        if (l.mediaReady && l.bridged && !l.tornDown && !keepLoopback) {
            l.tornDown = true;
            kill(l.loopbackA, "NORMAL_CLEARING");
        }
    }

    /** A bridge that failed on loopback-b: the far end never answered (busy, no answer, unreachable). */
    private void onExecuteComplete(NodeEvent e) {
        if (!"bridge".equals(e.get("Application"))) {
            return;
        }
        Leg l = byLoopback.get(e.uuid());
        if (l == null || l.ended || l.state == LegState.ANSWERED) {
            return;
        }
        String result = e.get("variable_originate_disposition");
        if (result == null) {
            result = e.get("Application-Response");
        }
        if (result == null || "SUCCESS".equals(result)) {
            return;
        }
        String cause = result;
        about(LOG.atWarn(), l).addKeyValue("event", "leg.dial_failed").addKeyValue("disposition", causeText(cause))
                .log("the bridge to the far end failed");
        // The far end's own channel, if it existed, reports with its SIP status; give it a moment.
        work(l, () -> {
            sleep(200);
            if (!l.ended) {
                kill(l.loopbackA, "NORMAL_CLEARING");
                ended(l, cause(cause), 0, EndedBy.REMOTE);
            }
        });
    }

    private void onHangup(NodeEvent e) {
        String uuid = e.uuid();
        CompletableFuture<Void> waiter = answers.remove(uuid);
        if (waiter != null) {
            waiter.completeExceptionally(new IllegalStateException("hung up"));
        }
        Leg loopOwner = byLoopback.remove(uuid);
        if (loopOwner != null) {
            return;
        }
        Leg l = byUuid.remove(uuid);
        if (l == null) {
            return;
        }
        String hangupCause = e.get("Hangup-Cause");
        int sip = (int) parseLong(e.get("variable_sip_term_status"));
        EndCause cause = cause(hangupCause);
        about(LOG.atInfo(), l).addKeyValue("event", "leg.ended").addKeyValue("role", l.role)
                .addKeyValue("hangup_cause", causeText(hangupCause))
                .addKeyValue("sip_status", sip).addKeyValue("by_tc", l.killedByTc).log("a leg hung up");
        int status = cause == EndCause.NORMAL ? 0 : sip;
        if (l.killedByTc) {
            ended(l, cause, status, EndedBy.SYSTEM);
            return;
        }
        if (FsText.HangupHook.CAUSE.equals(hangupCause) && l.role != Role.CUSTOMER && l.token != null
                && customerLive(l)) {
            // Ended by the customer's hook before the customer's own end arrived: hold it so the
            // session sees the customer leave first (R1-13); 2 s at most.
            heldCoupled.computeIfAbsent(l.token, k -> new java.util.concurrent.CopyOnWriteArrayList<>())
                    .add(() -> ended(l, l.state == LegState.ANSWERED ? EndCause.NORMAL : EndCause.CANCELED, 0,
                            EndedBy.SYSTEM));
            String token = l.token;
            Thread.ofVirtual().start(() -> {
                sleep(2_000);
                releaseHeld(token);
            });
            return;
        }
        ended(l, cause, status, EndedBy.REMOTE);
        if (l.role == Role.CUSTOMER && l.token != null) {
            releaseHeld(l.token);
        }
    }

    private boolean customerLive(Leg l) {
        for (Leg o : legs.values()) {
            if (o.role == Role.CUSTOMER && l.token.equals(o.token) && !o.ended) {
                return true;
            }
        }
        return false;
    }

    private void releaseHeld(String token) {
        List<Runnable> held = heldCoupled.remove(token);
        if (held != null) {
            held.forEach(Runnable::run);
        }
    }

    /** A leg ended: once, then the engine hears of it. */
    private void ended(Leg l, EndCause cause, int sipStatus, EndedBy by) {
        if (l.ended) {
            return;
        }
        l.ended = true;
        l.state = LegState.ENDED;
        legs.remove(l.legId, l);
        if (l.uuid != null) {
            byUuid.remove(l.uuid, l);
        }
        releaseMedia(l);
        Listener li = listener;
        if (li != null) {
            li.ended(List.of(new Ended(l.legId, cause, sipStatus, by, clock.nowMs())));
        }
        reported.put(l.legId, LegState.ENDED);
    }

    // ---- re-INVITEs ((c+), §5) -------------------------------------------------------------------

    private static final long[] REREAD_MS = {0, 25, 25, 50, 100};

    private void onReinvite(NodeEvent e) {
        String uuid = e.get("cc_leg");
        Leg l = uuid == null ? null : byUuid.get(uuid);
        if (l == null || l.ended) {
            return;
        }
        reinvites.incrementAndGet();
        work(l, () -> {
            String last = l.remoteSdp;
            long lastVersion = Sdp.version(last);
            String now = null;
            for (long wait : REREAD_MS) {
                sleep(wait);
                try {
                    now = getvar(uuid, "switch_r_sdp");
                } catch (Unavailable ex) {
                    report(l, RemoteMediaKind.UNREAD);
                    return;
                }
                if (Sdp.version(now) > lastVersion) {
                    break;
                }
            }
            if (now == null || now.isEmpty()) {
                report(l, RemoteMediaKind.UNREAD);
                return;
            }
            if (Sdp.version(now) <= lastVersion) {
                if (now.equals(last)) {
                    reinviteUnchangedIdentical.incrementAndGet();
                    return;
                }
                reinviteUnchangedChanged.incrementAndGet();
                about(LOG.atWarn(), l).addKeyValue("event", "reinvite.sdp_unversioned")
                        .log("a re-INVITE changed the SDP without raising its o= version");
            }
            Sdp before = Sdp.parse(last);
            Sdp after = Sdp.parse(now);
            l.remoteSdp = now;
            boolean heldBefore = !"sendrecv".equals(before.direction());
            boolean heldNow = !"sendrecv".equals(after.direction());
            if (heldBefore != heldNow) {
                // The far end's own hold or resume: IE decides what follows (step 4).
                remoteHolds.merge(l.legId, 1, Integer::sum);
                Listener li = listener;
                if (li != null) {
                    li.remoteHold(l.legId, heldNow);
                }
            }
            if (after.port() == 0 || (!before.codecs().isEmpty() && !after.codecs().contains(before.codecs().get(0)))) {
                reinviteUnanswerable.incrementAndGet();
                about(LOG.atError(), l).addKeyValue("event", "reinvite.unanswerable").log("a re-INVITE TC can't answer (another codec)");
                report(l, RemoteMediaKind.UNANSWERABLE);
                return;
            }
            if (!before.address().equals(after.address()) || before.port() != after.port()) {
                try {
                    media.updateRemote(l.tenantId, l.interactionId, l.inbound ? MediaPort.Side.A : MediaPort.Side.B, now,
                            l.ownerEpoch);
                    report(l, RemoteMediaKind.ADDRESS);
                } catch (Unavailable ex) {
                    report(l, RemoteMediaKind.UNREAD);
                }
            }
        });
    }

    private void report(Leg l, RemoteMediaKind kind) {
        Listener li = listener;
        if (li != null) {
            li.remoteMedia(l.legId, kind);
        }
        mediaReports.merge(l.legId, 1, Integer::sum);
    }

    // ---- NodePort -----------------------------------------------------------------------------------

    @Override
    public void listen(Listener l) {
        listener = l;
    }

    @Override
    public boolean callbacksInline() {
        return false;
    }

    @Override
    public void accepted(String legId, String sessionToken) throws Unavailable {
        Leg l = need(legId);
        FsText.HangupHook hook = new FsText.HangupHook(new FsText.SessionToken(sessionToken));
        l.token = sessionToken;
        api("uuid_setvar_multi " + l.uuid + " cc_session_token=" + sessionToken + ";cc_interaction_id="
                + FsText.uuid(l.interactionId));
        writeHook(l, hook);
    }

    /** R1-1: the hook is written as a literal and read back; a mismatch ends the leg (503) and is alerted. */
    private void writeHook(Leg l, FsText.HangupHook hook) throws Unavailable {
        api("uuid_setvar " + l.uuid + " api_hangup_hook " + hook.value());
        String back = getvar(l.uuid, "api_hangup_hook");
        if (!hook.value().equals(back)) {
            hookMismatches.incrementAndGet();
            about(LOG.atError(), l).addKeyValue("event", "leg.hook_mismatched").log("the hangup hook didn't read back as written: the leg ends");
            kill(l.uuid, "NORMAL_TEMPORARY_FAILURE");
            throw new Unavailable(Part.FREESWITCH);
        }
    }

    private void writeHook(Leg l) {
        try {
            writeHook(l, new FsText.HangupHook(new FsText.SessionToken(l.token)));
        } catch (Unavailable e) {
            about(LOG.atWarn(), l).addKeyValue("event", "leg.hook_failed").log("couldn't write an originated customer leg's hook");
        }
    }

    @Override
    public void claimArrival(String legId) throws Unavailable {
        Leg l = need(legId);
        String r = api("sched_del " + l.uuid);
        if (deleted(r) < 1) {
            // The safety hangup is gone or was never armed (R1-4): counted, alerted, and refused.
            safetyHangupMissing.incrementAndGet();
            about(LOG.atError(), l).addKeyValue("event", "arrival.backstop_missing").log("an arrival's safety hangup wasn't there to delete");
            throw new Unavailable(Part.FREESWITCH);
        }
        api("sched_hangup +" + config.claimedBackstopS() + " " + l.uuid + " NORMAL_TEMPORARY_FAILURE");
        l.backstop = true;
    }

    @Override
    public long answer(String legId, long epoch) throws Unavailable {
        Leg l = need(legId);
        if (l.remoteSdp == null) {
            throw new Unavailable(Part.FREESWITCH);
        }
        String answer = media.open(l.tenantId, l.interactionId, l.remoteSdp, epoch);
        CompletableFuture<Void> answered = new CompletableFuture<>();
        answers.put(l.uuid, answered);
        exec(l.uuid, "set", "switch_m_sdp=" + FsText.sdp(answer));
        exec(l.uuid, "answer", null);
        try {
            answered.get(config.answerTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new Unavailable(Part.FREESWITCH);
        } catch (ExecutionException | TimeoutException e) {
            answers.remove(l.uuid);
            throw new Unavailable(Part.FREESWITCH);
        }
        deleteBackstop(l);
        l.state = LegState.ANSWERED;
        l.answeredAtMs = clock.nowMs();
        l.ownerEpoch = Math.max(l.ownerEpoch, epoch);
        return l.answeredAtMs;
    }

    @Override
    public void ring(String legId) throws Unavailable {
        Leg l = need(legId);
        exec(l.uuid, "ring_ready", null);
        l.state = LegState.RINGING;
    }

    @Override
    public Ended reject(String legId, EndCause cause, int sipStatus, EndedBy by) throws Unavailable {
        Leg l = need(legId);
        deleteBackstop(l);
        l.killedByTc = true;
        exec(l.uuid, "respond", Integer.toString(sipStatus));
        markEnded(l);
        String uuid = l.uuid;
        // respond sends the final response; the channel is ended for certain a moment later.
        work(l, () -> {
            sleep(500);
            kill(uuid, "NORMAL_CLEARING");
        });
        return new Ended(legId, cause, sipStatus, by, clock.nowMs());
    }

    @Override
    public void originate(Channel spec, Dial dial, long epoch) throws Unavailable {
        if (partitioned || !esl.live()) {
            throw new Unavailable(Part.FREESWITCH);
        }
        String legId = FsText.uuid(spec.legId);
        String token = new FsText.SessionToken(dial.sessionToken()).value();
        boolean pstn = dial.pstnNumber() != null;
        String target = pstn ? pstnTarget(dial) : agentTarget(config, spec.tenantId, dial.agentContact(), spec.endpoint);
        Leg l = new Leg(legId, spec.interactionId, spec.tenantId, spec.bin, spec.role, spec.endpoint, false,
                clock.nowMs(), pstn ? dial.pstnNumber() : null, null, dial.trunkId(), legId, spec.addLegFingerprint,
                LegState.ORIGINATING);
        l.uuid = legId;
        l.token = token;
        l.ownerEpoch = spec.ownerEpoch;
        l.ownerReplica = spec.ownerReplicaId;
        l.ownerBin = spec.ownerBin;
        l.offer = media.attach(spec.tenantId, spec.interactionId, legId, spec.endpoint == Endpoint.WEBRTC, epoch);
        NodePort.DtmfSetup dtmf = dial.dtmf();
        if (dtmf != null) {
            // AddLeg's codes (P5) are on the leg's media before it rings: an agent can't key one
            // unseen. A failure gives the leg back (detach), so IE's retry starts clean.
            try {
                media.dtmfActions(spec.tenantId, spec.interactionId, MediaPort.Side.B, dtmf.role(), dtmf.bindings(),
                        dtmf.interDigitMs(), dtmf.lock(), dtmf.unlock(), epoch);
            } catch (Unavailable e) {
                try {
                    media.detach(spec.tenantId, spec.interactionId, legId, epoch);
                } catch (Unavailable again) {
                    LOG.atWarn().addKeyValue("event", "leg.detach_failed").addKeyValue("tenant_id", spec.tenantId)
                            .addKeyValue("interaction_id", spec.interactionId).addKeyValue("leg_id", legId)
                            .log("couldn't detach a leg whose DTMF codes failed");
                }
                throw e;
            }
        }
        if (spec.addLegFingerprint != null) {
            FsText.name(spec.addLegFingerprint);
        }
        if (dial.callerId() != null && !dial.callerId().isEmpty()) {
            FsText.number(dial.callerId());
        }
        FsText.name(spec.tenantId);
        FsText.name(spec.bin);
        l.target = target;
        l.ringTimeoutS = dial.ringTimeoutS();
        l.callerId = dial.callerId();
        l.ownerEpoch = Math.max(l.ownerEpoch, epoch);
        if (legs.putIfAbsent(legId, l) != null) {
            throw new Unavailable(Part.FREESWITCH);
        }
        byUuid.put(legId, l);
        originates.merge(legId, 1, Integer::sum);
        String loopA = UUID.randomUUID().toString();
        l.loopbackA = loopA;
        byLoopback.put(loopA, l);
        // sip_copy_custom_headers reaches loopback-b with the other variables (loopback.xml sets it too):
        // its bridge then copies no X- header onto the far end's leg, only the dial string's (TC-031).
        String loop = new FsText.DialString().var("origination_uuid", loopA).var("cc_managed", "true")
                .var("cc_role", "loopback-a").var("cc_leg_id", legId).var("cc_session_token", token)
                .var("sip_copy_custom_headers", "false")
                .build("loopback/tcleg-" + legId + "-" + token + "/loopback");
        try {
            // The job's outcome is handled here; nobody waits for it.
            CompletableFuture<String> unused = esl.bgapi("originate " + loop + " &park()").whenComplete((result, err) -> {
                // The job completes only when loopback-a is answered, i.e. after the far end
                // answered and TC tore the pair down: its result counts only if loopback-b never parked.
                if ((err != null || result == null || !result.startsWith("+OK")) && l.loopbackB == null) {
                    work(l, () -> ended(l, EndCause.SYSTEM, 0, EndedBy.SYSTEM));
                }
            });
        } catch (EslClient.Unavailable | EslClient.Timeout e) {
            legs.remove(legId, l);
            byUuid.remove(legId, l);
            byLoopback.remove(loopA, l);
            throw new Unavailable(Part.FREESWITCH);
        }
    }

    @Override
    public List<Ended> hangup(String legId, EndCause cause, EndedBy by) throws Unavailable {
        Leg l = legs.get(legId);
        if (l == null) {
            return List.of();
        }
        l.killedByTc = true;
        kill(l.loopbackA, "NORMAL_CLEARING");
        kill(l.loopbackB, "NORMAL_CLEARING");
        String r = api("uuid_kill " + l.uuid + " " + fsCause(cause, l));
        if (r.startsWith("-ERR") && !r.contains("No such channel")) {
            throw new Unavailable(Part.FREESWITCH);
        }
        markEnded(l);
        return List.of(new Ended(legId, cause, 0, by, clock.nowMs()));
    }

    @Override
    public boolean setOwner(String legId, long epoch, String replicaId, String bin) throws Unavailable {
        Leg l = legs.get(legId);
        if (l == null) {
            return false;
        }
        String r = api("uuid_setvar_multi " + l.uuid + " cc_owner_epoch=" + epoch + ";cc_owner_replica="
                + FsText.name(replicaId) + ";cc_owner_bin=" + FsText.name(bin));
        if (r.startsWith("-ERR") && (l.inbound || l.state == LegState.ANSWERED)) {
            return false;
        }
        // An originated leg whose far end isn't dialled yet keeps the fence for its dial string,
        // and one dialled since gets it at its first event (catchUpFence).
        l.ownerEpoch = epoch;
        l.ownerReplica = replicaId;
        l.ownerBin = bin;
        if (!r.startsWith("-ERR")) {
            l.bakedEpoch = epoch;
        }
        return true;
    }

    // ---- Step 4: a leg's media (ADR 0003 D6) -------------------------------------------------------

    private Leg mediaLeg(String legId) throws Unavailable {
        Leg l = legs.get(legId);
        if (l == null || l.ended || (!l.inbound && l.offer == null)) {
            throw new Unavailable(Part.PARITY_MS);
        }
        return l;
    }

    private static MediaPort.Side sideOf(Leg l) {
        return l.inbound ? MediaPort.Side.A : MediaPort.Side.B;
    }

    @Override
    public void play(String legId, String file, long epoch) throws Unavailable {
        play(legId, file, false, epoch);
    }

    @Override
    public boolean play(String legId, String file, boolean loop, long epoch) throws Unavailable {
        return play(legId, file, loop, null, epoch);
    }

    @Override
    public boolean play(String legId, String file, boolean loop, String playId, long epoch) throws Unavailable {
        Leg l = mediaLeg(legId);
        return media.play(l.tenantId, l.interactionId, sideOf(l), file, loop, playId, epoch);
    }

    @Override
    public boolean asyncPlays() {
        return media.asyncPlays();
    }

    @Override
    public PlaySent playAsync(String legId, String file, boolean loop, String playId, long epoch) throws Unavailable {
        Leg l = mediaLeg(legId);
        return media.playAsync(l.tenantId, l.interactionId, sideOf(l), file, loop, playId, epoch);
    }

    @Override
    public void stopPlay(String legId, String playId, long epoch) throws Unavailable {
        Leg l = mediaLeg(legId);
        media.stopPlay(l.tenantId, l.interactionId, sideOf(l), playId, epoch);
    }

    @Override
    public boolean stopPlay(String legId, long epoch) throws Unavailable {
        Leg l = mediaLeg(legId);
        return media.stopPlay(l.tenantId, l.interactionId, sideOf(l), epoch);
    }

    @Override
    public boolean mute(String legId, boolean muted, long epoch) throws Unavailable {
        Leg l = mediaLeg(legId);
        return media.mute(l.tenantId, l.interactionId, sideOf(l), muted, epoch);
    }

    @Override
    public void sendDtmf(String legId, String digits, long epoch) throws Unavailable {
        Leg l = mediaLeg(legId);
        media.sendDtmf(l.tenantId, l.interactionId, sideOf(l), digits, epoch);
    }

    @Override
    public void reportDigits(String legId, String mode, long epoch) throws Unavailable {
        Leg l = mediaLeg(legId);
        media.reportDigits(l.tenantId, l.interactionId, sideOf(l), mode, epoch);
    }

    @Override
    public void dtmfActions(String legId, String role, List<DtmfBinding> bindings, int interDigitMs, List<String> lock,
            List<String> unlock, long epoch) throws Unavailable {
        Leg l = mediaLeg(legId);
        media.dtmfActions(l.tenantId, l.interactionId, sideOf(l), role, bindings, interDigitMs, lock, unlock, epoch);
    }

    // ---- Recording on the null sink (part A of R7/P5; MS-012: nothing written until R9) -----------

    /** A leg's open recording: where its result will come from once its media lets it go. */
    private record OpenRecording(String tenantId, String interactionId, MediaPort.Side side,
            CompletableFuture<NodePort.RecordingResult> result) {}

    /** By leg id. */
    private final ConcurrentHashMap<String, OpenRecording> recordings = new ConcurrentHashMap<>();

    /** How long a leg's end waits for its recording's result (a detach or the call's delete in flight). */
    static final long RECORDING_RESULT_WAIT_MS = 2_000;

    @Override
    public boolean recordingNullSink() {
        return "null".equals(media.recordingSink());
    }

    @Override
    public void startRecording(String legId, String recordingId, long epoch) throws Unavailable {
        Leg l = mediaLeg(legId);
        MediaPort.Side side = sideOf(l);
        media.startRecording(l.tenantId, l.interactionId, side, recordingId, epoch);
        recordings.put(legId, new OpenRecording(l.tenantId, l.interactionId, side, new CompletableFuture<>()));
    }

    @Override
    public NodePort.RecordingResult stopRecording(String legId, long epoch) throws Unavailable {
        OpenRecording r = recordings.remove(legId);
        if (r == null) {
            return null;
        }
        NodePort.RecordingResult res = media.stopRecording(r.tenantId(), r.interactionId(), r.side(), epoch);
        r.result().complete(res);
        return res;
    }

    @Override
    public NodePort.RecordingResult finishRecording(String legId, long epoch) {
        OpenRecording r = recordings.remove(legId);
        if (r == null) {
            return null;
        }
        // The customer's leg A outlives its channel in parity-ms until the call's last leg goes: stop
        // it now. Otherwise its result comes with the detach or the delete already on its way.
        if (r.side() == MediaPort.Side.A && !r.result().isDone() && media.epoch(r.interactionId()) >= 0) {
            try {
                NodePort.RecordingResult res = media.stopRecording(r.tenantId(), r.interactionId(), r.side(), epoch);
                if (res != null) {
                    r.result().complete(res);
                }
            } catch (Unavailable e) {
                // the delete's answer may still carry it
            }
        }
        try {
            return r.result().get(RECORDING_RESULT_WAIT_MS, java.util.concurrent.TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.TimeoutException | java.util.concurrent.ExecutionException e) {
            LOG.atWarn().addKeyValue("event", "recording.result_missing").addKeyValue("tenant_id", r.tenantId())
                    .addKeyValue("interaction_id", r.interactionId()).addKeyValue("leg_id", legId)
                    .log("a recording's result didn't come from parity-ms: TC reports its own figures");
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    /**
     * The results a detach ({@code legId}: that leg's) or the call's delete ({@code legId} null: every
     * leg of the call) brought, to the recordings they were; the others had none to bring (null), so
     * nobody waits for them.
     */
    private void recordingsReleased(String interactionId, String legId,
            java.util.Map<MediaPort.Side, NodePort.RecordingResult> results) {
        recordings.forEach((leg, r) -> {
            if (r.interactionId().equals(interactionId) && (legId == null || legId.equals(leg))) {
                r.result().complete(results.get(r.side()));
            }
        });
    }

    /** Test hook: the parity-ms call and side a leg's media events come under. */
    public java.util.Map.Entry<String, MediaPort.Side> mediaKey(String legId) {
        Leg l = legs.get(legId);
        return l == null ? null : java.util.Map.entry(l.interactionId, sideOf(l));
    }

    /** Test hook: the far end's holds and resumes TC reported for a leg. */
    public int remoteHolds(String legId) {
        return remoteHolds.getOrDefault(legId, 0);
    }

    /** The leg a parity-ms event names: A is the customer's, B the agent's attached leg. */
    private Leg legOf(String interactionId, MediaPort.Side side) {
        Leg found = null;
        for (Leg l : legs.values()) {
            if (l.ended || !l.interactionId.equals(interactionId)) {
                continue;
            }
            boolean isA = l.inbound;
            if ((side == MediaPort.Side.A) == isA && (isA || l.offer != null) && (found == null || l.seq > found.seq)) {
                found = l;
            }
        }
        return found;
    }

    private final ConcurrentHashMap<String, Integer> remoteHolds = new ConcurrentHashMap<>();

    private final class MediaEvents implements MediaPort.Events {
        @Override
        public boolean live(String interactionId) {
            return legs.values().stream().anyMatch(l -> !l.ended && l.interactionId.equals(interactionId));
        }

        @Override
        public long syncedForMs() {
            long at = syncedAtNanos;
            return at < 0 || !ready || !esl.live() ? -1 : (System.nanoTime() - at) / 1_000_000;
        }

        @Override
        public boolean confirmOrphan(String interactionId) {
            if (!ready || closed || live(interactionId)) {
                return false;
            }
            try {
                for (JsonNode row : JSON.readTree(esl.api("show channels as json")).path("rows")) {
                    String uuid = row.path("uuid").asString("");
                    Leg known = byUuid.get(uuid);
                    if (known == null) {
                        known = byLoopback.get(uuid);
                    }
                    if (known != null) {
                        if (known.interactionId.equals(interactionId)) {
                            return false;
                        }
                        continue;
                    }
                    String dump = esl.api("uuid_dump " + uuid + " json");
                    if (!dump.startsWith("-ERR")
                            && interactionId.equals(text(JSON.readTree(dump), "variable_cc_interaction_id"))) {
                        return false;
                    }
                }
                return true;
            } catch (EslClient.Unavailable | EslClient.Timeout | RuntimeException e) {
                // freeswitch can't say: nothing is deleted.
                return false;
            }
        }

        @Override
        public void dtmfAction(String interactionId, MediaPort.Side side, String action, String result, boolean muted,
                String recording) {
            Leg l = legOf(interactionId, side);
            Listener li = listener;
            if (l != null && li != null) {
                li.dtmfAction(l.legId, action, result, muted, recording);
            }
        }

        @Override
        public void activity(String interactionId, MediaPort.Side side, boolean active) {
            Leg l = legOf(interactionId, side);
            Listener li = listener;
            if (l != null && li != null) {
                li.mediaActivity(l.legId, active);
            }
        }

        @Override
        public void played(String interactionId, MediaPort.Side side, String reason, long playedMs) {
            played(interactionId, side, reason, playedMs, "");
        }

        @Override
        public void played(String interactionId, MediaPort.Side side, String reason, long playedMs, String playId) {
            Leg l = legOf(interactionId, side);
            Listener li = listener;
            if (l != null && li != null) {
                li.playFinished(l.legId, reason, playedMs, playId);
            }
        }

        @Override
        public void playStarted(String interactionId, MediaPort.Side side, String playId) {
            Leg l = legOf(interactionId, side);
            Listener li = listener;
            if (l != null && li != null) {
                li.playStarted(l.legId, playId);
            }
        }

        @Override
        public void playFailed(String interactionId, MediaPort.Side side, String playId, String reason) {
            Leg l = legOf(interactionId, side);
            Listener li = listener;
            if (l != null && li != null) {
                li.playFailed(l.legId, playId, reason);
            }
        }

        @Override
        public void digit(String interactionId, MediaPort.Side side, String digit, int durationMs, String source) {
            Leg l = legOf(interactionId, side);
            Listener li = listener;
            if (l != null && li != null) {
                li.digit(l.legId, digit, durationMs, source);
            }
        }

        @Override
        public void ended(String interactionId, String reason) {
            // parity-ms ended the call itself (idle timeout, shutdown): its legs have no media left.
            LOG.atWarn().addKeyValue("event", "media.session_ended").addKeyValue("interaction_id", interactionId)
                    .addKeyValue("reason", causeText(reason.toUpperCase(
                    java.util.Locale.ROOT))).log("parity-ms ended a session's media: its legs end");
            for (Leg l : List.copyOf(legs.values())) {
                if (l.interactionId.equals(interactionId) && !l.ended) {
                    l.mediaReleased = true;
                    l.killedByTc = true;
                    work(l, () -> kill(l.uuid, "MEDIA_TIMEOUT"));
                    RealNode.this.ended(l, EndCause.MEDIA_FAILED, 0, EndedBy.SYSTEM);
                }
            }
        }
    }

    @Override
    public void mediaClaim(String interactionId, long epoch) throws Unavailable {
        Optional<Leg> any = legs.values().stream().filter(l -> l.interactionId.equals(interactionId)).findFirst();
        if (any.isPresent()) {
            media.claim(any.get().tenantId, interactionId, epoch);
        }
    }

    @Override
    public void mediaUpdate(String interactionId, long epoch) throws Unavailable {
        // The re-INVITE's handler moved the media already (onReinvite).
    }

    @Override
    public int freeLegs() {
        long used = legs.values().stream().filter(l -> !l.ended && (l.state == LegState.ANSWERED || !l.inbound)).count();
        return (int) Math.max(0, maxLegs - used);
    }

    @Override
    public List<Channel> channels() {
        return legs.values().stream().filter(l -> !l.ended).sorted(java.util.Comparator.comparingLong(l -> l.seq))
                .map(Leg::channel).toList();
    }

    @Override
    public Optional<Channel> channel(String legId) {
        Leg l = legs.get(legId);
        if (l == null || l.ended) {
            return Optional.empty();
        }
        try {
            String r = api("uuid_exists " + l.uuid);
            if (!"true".equals(r.trim())) {
                return Optional.empty();
            }
        } catch (Unavailable e) {
            return Optional.of(l.channel());
        }
        return Optional.of(l.channel());
    }

    @Override
    public List<Journal.LegEnded> cdrs(long sinceMs) {
        return CdrFiles.read(config.cdrDir(), sinceMs);
    }

    @Override
    public long mediaEpoch(String interactionId) {
        return media.epoch(interactionId);
    }

    @Override
    public List<String> invariantViolations() {
        List<String> out = new ArrayList<>();
        originates.forEach((leg, n) -> {
            if (n > 1) {
                out.add("leg " + leg + " was originated " + n + " times");
            }
        });
        return out;
    }

    /** The node's metrics (TC ADR 0002 §1, §5, §7): every one alerted when it moves, but re-INVITEs. */
    public void metrics(io.micrometer.core.instrument.MeterRegistry r) {
        counter(r, "telephony_controller_reinvites", reinvites, "Re-INVITEs (c+) answered by freeswitch, by TC's hook");
        counter(r, "telephony_controller_reinvite_unanswerable", reinviteUnanswerable,
                "Re-INVITEs TC can't answer (another codec): alerted on every one (R2-5)");
        counter(r, "telephony_controller_reinvite_sdp_unchanged_identical", reinviteUnchangedIdentical,
                "Same-SDP refreshes; a high rate is alerted (R1-9)");
        counter(r, "telephony_controller_reinvite_sdp_unchanged_changed", reinviteUnchangedChanged,
                "SDPs changed without a new o= version: alerted (R1-9)");
        counter(r, "telephony_controller_safety_hangup_missing", safetyHangupMissing,
                "Claims whose safety hangup wasn't there to delete: alerted (R1-4)");
        counter(r, "telephony_controller_hangup_hook_mismatches", hookMismatches,
                "Hangup hooks that didn't read back as written: alerted (R1-1)");
        io.micrometer.core.instrument.Gauge.builder("telephony_controller_freeswitch_rtp_ports_open", canaryPorts,
                AtomicLong::get).description("Sockets in freeswitch's RTP block (the canary): above 0 is alerted")
                .register(r);
        io.micrometer.core.instrument.FunctionCounter.builder("telephony_controller_esl_late_replies", esl,
                EslClient::lateReplies).description("ESL replies after their command timed out").register(r);
        io.micrometer.core.instrument.FunctionCounter.builder("telephony_controller_esl_reconnects", esl,
                EslClient::reconnects).description("ESL cycles after the first").register(r);
        io.micrometer.core.instrument.Gauge.builder("telephony_controller_esl_event_lag_seconds", esl,
                c -> c.probeLagNanos() / 1e9).description("The ESL probe's round trip").register(r);
        io.micrometer.core.instrument.Gauge.builder("telephony_controller_esl_live", this, n -> n.live() ? 1 : 0)
                .description("ESL connected, subscribed and snapshotted").register(r);
    }

    private static void counter(io.micrometer.core.instrument.MeterRegistry r, String name, AtomicLong v, String d) {
        io.micrometer.core.instrument.FunctionCounter.builder(name, v, AtomicLong::get).description(d).register(r);
    }

    // ---- test hooks (the real-node runner) ------------------------------------------------------

    /** The last state the engine was told of for a leg, or null. */
    public LegState reported(String legId) {
        return reported.get(legId);
    }

    /** How many re-INVITE outcomes the engine was told of for a leg. */
    public int mediaReports(String legId) {
        return mediaReports.getOrDefault(legId, 0);
    }

    /** Whether this node knows a live leg. */
    public boolean hasLeg(String legId) {
        Leg l = legs.get(legId);
        return l != null && !l.ended;
    }

    /** The freeswitch uuid of a live leg, or null. */
    public String uuidOf(String legId) {
        Leg l = legs.get(legId);
        return l == null ? null : l.uuid;
    }

    /** Test hook: the G-3 measurement compares the cost with the loopback pairs kept and torn down. */
    public void setKeepLoopback(boolean keep) {
        keepLoopback = keep;
    }

    /** A script's {@code capacity}: this many legs free from now. */
    public void setFreeLegs(int free) {
        long used = legs.values().stream().filter(l -> !l.ended && (l.state == LegState.ANSWERED || !l.inbound)).count();
        maxLegs = (int) (free + used);
    }

    /** A script's {@code parity_ms_lost}: the media is gone; the engine hears it. */
    public void mediaLost() {
        // Its recordings went with it: nobody waits for their results.
        recordings.values().forEach(r -> r.result().complete(null));
        recordings.clear();
        Listener li = listener;
        if (li != null) {
            li.lost(Part.PARITY_MS);
        }
    }

    /** A script's reset: this node forgets its legs (freeswitch's channels are hung up by the runner). */
    public void resetForTest(int freeLegs) {
        legs.clear();
        byUuid.clear();
        byLoopback.clear();
        answers.clear();
        originates.clear();
        heldCoupled.clear();
        reported.clear();
        mediaReports.clear();
        remoteHolds.clear();
        recordings.clear();
        maxLegs = freeLegs;
        partitioned = false;
    }

    /** Freeswitch's RTP ports in use (the canary, §7): above 0, freeswitch took media. */
    public long canaryPortsOpen() {
        return canaryPorts.get();
    }

    // ---- helpers ------------------------------------------------------------------------------------

    private Leg need(String legId) throws Unavailable {
        Leg l = legs.get(legId);
        if (l == null || l.uuid == null) {
            throw new Unavailable(Part.FREESWITCH);
        }
        return l;
    }

    private String api(String cmd) throws Unavailable {
        if (partitioned || !ready) {
            throw new Unavailable(Part.FREESWITCH);
        }
        try {
            return esl.api(cmd);
        } catch (EslClient.Unavailable | EslClient.Timeout e) {
            throw new Unavailable(Part.FREESWITCH);
        }
    }

    private String getvar(String uuid, String name) throws Unavailable {
        String v = api("uuid_getvar " + uuid + " " + name);
        return v.startsWith("-ERR") || "_undef_".equals(v.trim()) ? null : v;
    }

    private void exec(String uuid, String app, String arg) throws Unavailable {
        if (partitioned) {
            throw new Unavailable(Part.FREESWITCH);
        }
        try {
            ok(esl.execute(uuid, app, arg));
        } catch (EslClient.Unavailable | EslClient.Timeout e) {
            throw new Unavailable(Part.FREESWITCH);
        }
    }

    private static void ok(String outcome) throws EslClient.Unavailable {
        if (!"+OK".equals(outcome)) {
            throw new EslClient.Unavailable();
        }
    }

    private void respond(String uuid, int status) {
        try {
            exec(uuid, "respond", Integer.toString(status));
        } catch (Unavailable e) {
            LOG.atWarn().addKeyValue("event", "channel.refuse_failed").log("couldn't refuse a channel");
        }
    }

    private void kill(String uuid, String cause) {
        if (uuid == null) {
            return;
        }
        try {
            esl.api("uuid_kill " + uuid + " " + cause);
        } catch (EslClient.Unavailable | EslClient.Timeout e) {
            LOG.atWarn().addKeyValue("event", "channel.kill_failed").log("couldn't kill a channel");
        }
    }

    private void deleteBackstop(Leg l) {
        if (l.backstop) {
            try {
                api("sched_del " + l.uuid);
                l.backstop = false;
            } catch (Unavailable e) {
                about(LOG.atWarn(), l).addKeyValue("event", "arrival.backstop_delete_failed")
                        .log("couldn't delete a claimed arrival's backstop");
            }
        }
    }

    private void markEnded(Leg l) {
        // The engine records the end from TC's own return; the hangup event that follows is quiet.
        l.ended = true;
        l.state = LegState.ENDED;
        legs.remove(l.legId, l);
        releaseMedia(l);
    }

    /**
     * A leg is gone from {@code legs}: its media follows, off the ESL reader (a blocking request).
     * The session's call ends with its last leg; an agent's leg B is detached, so another agent can
     * be attached. Both ends of a leg (TC's own hangup and the far end's) come here once.
     */
    private void releaseMedia(Leg l) {
        if (l.mediaReleased) {
            return;
        }
        l.mediaReleased = true;
        boolean last = legs.values().stream().noneMatch(o -> o.interactionId.equals(l.interactionId));
        if (!last && (l.offer == null || l.inbound)) {
            return;
        }
        work(l, () -> {
            try {
                if (last) {
                    recordingsReleased(l.interactionId, null, media.end(l.tenantId, l.interactionId, l.ownerEpoch));
                } else {
                    NodePort.RecordingResult b = media.detach(l.tenantId, l.interactionId, l.legId, l.ownerEpoch);
                    recordingsReleased(l.interactionId, l.legId, b == null ? Map.of() : Map.of(MediaPort.Side.B, b));
                }
            } catch (Unavailable e) {
                about(LOG.atWarn(), l).addKeyValue("event", "leg.media_release_failed").log("parity-ms couldn't release a leg's media");
                recordingsReleased(l.interactionId, last ? null : l.legId, Map.of());
            }
        });
    }

    private static int deleted(String reply) {
        int i = reply.indexOf("Deleted:");
        if (!reply.startsWith("+OK") || i < 0) {
            return 0;
        }
        return (int) parseLong(reply.substring(i + "Deleted:".length()).trim());
    }

    /** One leg's ESL work at a time, off ESL's reader. */
    private void work(Leg l, Runnable r) {
        Thread.ofVirtual().name("tc-leg").start(() -> {
            l.work.lock();
            try {
                r.run();
            } finally {
                l.work.unlock();
            }
        });
    }

    /**
     * The bridge from loopback-b to the far end, built when loopback-b parks: with the fence the leg
     * has then (a raise while the leg was still being originated is in it, R1-7).
     */
    private String bridgeString(Leg l) {
        String reinviteHook = "event Event-Name=CUSTOM,Event-Subclass=parity::reinvite,cc_leg=" + l.legId;
        l.bakedEpoch = l.ownerEpoch;
        FsText.DialString d = new FsText.DialString()
                .var("origination_uuid", l.legId).var("cc_managed", "true")
                .var("cc_role", l.role.name().toLowerCase(java.util.Locale.ROOT)).var("cc_leg_id", l.legId)
                .var("cc_session_token", l.token).var("cc_interaction_id", FsText.uuid(l.interactionId))
                .var("cc_tenant_id", FsText.name(l.tenantId)).var("cc_bin", FsText.name(l.bin))
                .var("cc_endpoint", l.endpoint.name()).var("cc_owner_epoch", Long.toString(l.ownerEpoch))
                .var("park_after_bridge", "true").var("sip_ignore_reinvites", "true")
                .own("execute_on_sip_reinvite", reinviteHook)
                .var("originate_timeout", Integer.toString(l.ringTimeoutS + 5))
                .var("call_timeout", Integer.toString(l.ringTimeoutS + 5));
        if (l.ownerReplica != null) {
            d.var("cc_owner_replica", FsText.name(l.ownerReplica));
        }
        if (l.ownerBin != null) {
            d.var("cc_owner_bin", FsText.name(l.ownerBin));
        }
        if (l.fingerprint != null) {
            d.var("cc_fingerprint", FsText.name(l.fingerprint));
        }
        if (l.callerId != null && !l.callerId.isEmpty()) {
            d.var("origination_caller_id_number", FsText.number(l.callerId));
        }
        if (l.calledNumber == null) {
            // An agent's endpoint (not the PSTN): the headers its page trusts, from TC's own values (TC-031).
            d.parityHeaders(l.interactionId, l.legId);
        }
        if (config.testHeaders()) {
            d.var("sip_h_X-Test-Leg-ID", l.legId);
        }
        return d.build(l.target);
    }

    /** A fence raised after the far end's channel was dialled: written now that the channel exists. */
    private void catchUpFence(Leg l) {
        if (l.ownerEpoch > l.bakedEpoch && l.uuid != null) {
            long e = l.ownerEpoch;
            work(l, () -> {
                try {
                    api("uuid_setvar_multi " + l.uuid + " cc_owner_epoch=" + e
                            + (l.ownerReplica == null ? "" : ";cc_owner_replica=" + l.ownerReplica)
                            + (l.ownerBin == null ? "" : ";cc_owner_bin=" + l.ownerBin));
                    l.bakedEpoch = e;
                } catch (Unavailable ex) {
                    about(LOG.atWarn(), l).addKeyValue("event", "leg.fence_write_failed").log("couldn't write a raised fence on a new leg");
                }
            });
        }
    }

    /**
     * An agent leg's dial target. With a per-tenant registrar (TC-056) the directory domain comes
     * from the leg's tenant (the session's, IE's verified owner), never from the contact, and a
     * contact outside it is refused here, before the leg or its media exist.
     */
    static String agentTarget(Config config, String tenantId, String contact, Endpoint endpoint) throws Unavailable {
        if (contact == null) {
            throw new Unavailable(Part.FREESWITCH);
        }
        java.util.regex.Matcher m = FsText.SIP_URI.matcher(contact);
        if (!m.matches()) {
            throw new Refused(Refused.Why.INVALID_TARGET, "not an agent's SIP URI");
        }
        String profile = endpoint == Endpoint.WEBRTC ? "agents" : "deskphones";
        String domain = config.agentRegistrar() == null ? null
                : config.agentRegistrar().directoryDomain(tenantId, m.group(2));
        if (domain != null) {
            return "user/" + m.group(1) + "@" + domain;
        }
        String uri = "sip:" + m.group(1) + "@" + m.group(2) + (m.group(3) == null ? "" : m.group(3));
        if (config.agentProxy() != null) {
            return "sofia/" + profile + "/" + uri + ";fs_path=<" + config.agentProxy() + ">";
        }
        return "sofia/" + profile + "/" + uri;
    }

    private String pstnTarget(Dial dial) throws Unavailable {
        String hostPort = config.trunks().get(FsText.name(dial.trunkId()));
        if (hostPort == null || !FsText.HOST_PORT.matcher(hostPort).matches()) {
            throw new Unavailable(Part.FREESWITCH);
        }
        return "sofia/carrier/" + FsText.number(dial.pstnNumber()) + "@" + hostPort;
    }

    /** A freeswitch cause for a log line: its upper-case name, or OTHER (never free text). */
    private static String causeText(String c) {
        return c != null && c.matches("[A-Z_]{1,48}") ? c : "OTHER";
    }

    private static String fsCause(EndCause c, Leg l) {
        return switch (c) {
            case NORMAL -> "NORMAL_CLEARING";
            case NO_ANSWER -> "NO_ANSWER";
            case BUSY -> "USER_BUSY";
            case DECLINED -> "CALL_REJECTED";
            case UNREACHABLE -> "UNALLOCATED_NUMBER";
            case CANCELED -> l.state == LegState.ANSWERED ? "NORMAL_CLEARING" : "ORIGINATOR_CANCEL";
            case MEDIA_FAILED -> "MEDIA_TIMEOUT";
            case SYSTEM -> "NORMAL_TEMPORARY_FAILURE";
        };
    }

    static EndCause cause(String fs) {
        if (fs == null) {
            return EndCause.NORMAL;
        }
        return switch (fs) {
            case "NORMAL_CLEARING", "SUCCESS" -> EndCause.NORMAL;
            case "USER_BUSY" -> EndCause.BUSY;
            case "NO_ANSWER", "NO_USER_RESPONSE", "ALLOTTED_TIMEOUT", "PROGRESS_TIMEOUT" -> EndCause.NO_ANSWER;
            case "CALL_REJECTED" -> EndCause.DECLINED;
            case "ORIGINATOR_CANCEL", "LOSE_RACE" -> EndCause.CANCELED;
            case "UNALLOCATED_NUMBER", "NO_ROUTE_DESTINATION", "USER_NOT_REGISTERED", "DESTINATION_OUT_OF_ORDER",
                    "RECOVERY_ON_TIMER_EXPIRE", "INVALID_NUMBER_FORMAT", "SUBSCRIBER_ABSENT" -> EndCause.UNREACHABLE;
            case "MEDIA_TIMEOUT", "INCOMPATIBLE_DESTINATION", "BEARERCAPABILITY_NOTIMPL" -> EndCause.MEDIA_FAILED;
            default -> EndCause.SYSTEM;
        };
    }

    private static Role role(String r) {
        return switch (r) {
            case "agent" -> Role.AGENT;
            case "supervisor" -> Role.SUPERVISOR;
            case "external" -> Role.EXTERNAL;
            default -> Role.CUSTOMER;
        };
    }

    private static Endpoint endpoint(String e, boolean inbound) {
        if (inbound || e == null) {
            return Endpoint.SIP;
        }
        return "WEBRTC".equals(e) ? Endpoint.WEBRTC : Endpoint.SIP;
    }

    private static LegState state(String answerState, boolean inbound) {
        if ("answered".equals(answerState)) {
            return LegState.ANSWERED;
        }
        if ("ringing".equals(answerState) || "early".equals(answerState)) {
            return LegState.RINGING;
        }
        return inbound ? LegState.PARKED : LegState.ORIGINATING;
    }

    private static String text(JsonNode v, String key) {
        JsonNode n = v.get(key);
        return n == null || n.isNull() ? null : n.asString();
    }

    static long parseLong(String s) {
        if (s == null) {
            return 0;
        }
        try {
            return Long.parseLong(s.trim());
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private static void sleep(long ms) {
        if (ms <= 0) {
            return;
        }
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Every 10 s: sockets bound in freeswitch's RTP block (it must never take media, §7). */
    private void canary() {
        while (!closed) {
            long n = 0;
            for (String f : List.of("/proc/net/udp", "/proc/net/udp6")) {
                try (Stream<String> lines = Files.lines(Path.of(f))) {
                    n += lines.skip(1).filter(this::inCanary).count();
                } catch (IOException | RuntimeException e) {
                    // not Linux, or not readable: the gauge stays as it was
                }
            }
            canaryPorts.set(n);
            if (n > 0) {
                LOG.atError().addKeyValue("event", "freeswitch.media_bound").addKeyValue("ports", n)
                        .log("freeswitch bound RTP ports: it is taking media");
            }
            sleep(10_000);
        }
    }

    private boolean inCanary(String line) {
        String[] f = line.trim().split("\\s+", 3);
        if (f.length < 2) {
            return false;
        }
        int colon = f[1].lastIndexOf(':');
        if (colon < 0) {
            return false;
        }
        try {
            int port = Integer.parseInt(f[1].substring(colon + 1), 16);
            return port >= config.canaryLow() && port <= config.canaryHigh();
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
