package parity.telephonycontroller.esl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The node's ESL client (TC ADR 0002 §1): one event connection and a pool of command connections,
 * all on {@code 127.0.0.1}, kept up by a supervisor on a virtual thread.
 *
 * <ul>
 *   <li>Each cycle: {@link Handler#connecting()}, then the subscription (so the handler can buffer
 *       every event from the first one), the command connections, freeswitch's {@code core_uuid},
 *       then {@link Handler#up}. The handler snapshots and applies its buffer (§2.4).
 *   <li>A probe ({@code sendevent CUSTOM parity::probe}) every {@code probeInterval} must come back on
 *       the event connection; none for {@code probeDeadline} → every connection closes and the cycle
 *       starts again after a backoff with full jitter. Nothing else closes a connection (R1-3).
 *   <li>A command goes to the least-loaded command connection; with all of them at their pipeline
 *       limit it fails at once ({@link Unavailable}, R1-3). {@code bgapi} jobs carry our own
 *       {@code Job-UUID}, registered before the write, and complete from {@code BACKGROUND_JOB}.
 * </ul>
 */
public final class EslClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(EslClient.class);

    /** The connection's settings. */
    public record Config(String host, int port, String password, int commandConnections, int pipelineLimit,
            Duration connectTimeout, Duration commandTimeout, Duration probeInterval, Duration probeDeadline,
            Duration backoffMin, Duration backoffMax) {

        public static Config defaults(String host, int port, String password) {
            return new Config(host, port, password, 4, 32, Duration.ofSeconds(2), Duration.ofSeconds(1),
                    Duration.ofSeconds(1), Duration.ofSeconds(5), Duration.ofMillis(100), Duration.ofSeconds(5));
        }
    }

    /** What the node's side does with ESL. Called on ESL threads: never block in {@link #event}. */
    public interface Handler {
        /** A new cycle: buffer events from now until {@link #up}. */
        void connecting();

        /** Subscribed and connected: snapshot, then apply what was buffered. */
        void up(String coreUuid);

        /** One event, in freeswitch's order, on the event connection's reader. */
        void event(NodeEvent e);

        /** The connections are gone; a new cycle follows. */
        void down();
    }

    /** ESL is down, or every connection is at its limit: nothing was sent. */
    public static final class Unavailable extends Exception {
        private static final long serialVersionUID = 1L;

        public Unavailable() {
            super("freeswitch's ESL is unavailable", null, false, false);
        }
    }

    /** The command was sent and no reply came in time: its outcome is unknown. */
    public static final class Timeout extends Exception {
        private static final long serialVersionUID = 1L;

        public Timeout() {
            super("no ESL reply in time; the outcome is unknown", null, false, false);
        }
    }

    /** The subscription TC needs, then its filters (§1). */
    public static final List<String> EVENTS = List.of("CHANNEL_PARK", "CHANNEL_PROGRESS", "CHANNEL_PROGRESS_MEDIA",
            "CHANNEL_ANSWER", "CHANNEL_BRIDGE", "CHANNEL_UNBRIDGE", "CHANNEL_EXECUTE_COMPLETE",
            "CHANNEL_HANGUP_COMPLETE", "BACKGROUND_JOB", "HEARTBEAT", "SHUTDOWN", "CUSTOM", "parity::reinvite",
            "parity::probe");
    public static final List<String> FILTERS = List.of("variable_cc_managed true", "Event-Name BACKGROUND_JOB",
            "Event-Name HEARTBEAT", "Event-Name SHUTDOWN", "Event-Subclass parity::reinvite",
            "Event-Subclass parity::probe", "Application -set", "Application -export", "Application -park",
            "Application -sched_hangup", "Application -event", "Application -answer", "Application -ring_ready",
            "Application -multiset", "Application -sched_cancel");

    private final Config config;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition stateChanged = lock.newCondition();
    private final ConcurrentHashMap<String, CompletableFuture<String>> jobs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, Long> probesSent = new ConcurrentHashMap<>();
    private final AtomicLong lateReplies = new AtomicLong();
    private final AtomicLong probeSeq = new AtomicLong();
    private final AtomicLong reconnects = new AtomicLong();
    private final AtomicLong lastProbeLagNanos = new AtomicLong();
    private volatile List<EslConnection> commands = List.of();
    private volatile EslConnection events;
    private volatile String coreUuid;
    private volatile boolean live;
    /** The supervisor reported freeswitch unavailable and hasn't reported it back yet (its thread only). */
    private boolean downReported;
    private volatile boolean closed;
    private volatile long lastProbeSeenNanos;
    private Thread supervisor;

    public EslClient(Config config) {
        this.config = config;
    }

    /** Starts the supervisor; it connects, and reconnects for as long as the client is open. */
    public void start(Handler handler) {
        supervisor = Thread.ofVirtual().name("esl-supervisor").start(() -> supervise(handler));
    }

    public boolean live() {
        return live;
    }

    public String coreUuid() {
        return coreUuid;
    }

    /** Replies that came after their command timed out (a metric). */
    public long lateReplies() {
        return lateReplies.get();
    }

    /** Cycles started after the first (a metric; a stall shorter than the probe deadline adds none). */
    public long reconnects() {
        return reconnects.get();
    }

    /** The last probe's round trip, in nanoseconds. */
    public long probeLagNanos() {
        return lastProbeLagNanos.get();
    }

    /** Waits until the client is live, at most {@code timeout}; true if it is. */
    public boolean awaitLive(Duration timeout) throws InterruptedException {
        long until = System.nanoTime() + timeout.toNanos();
        lock.lock();
        try {
            while (!live && !closed) {
                long left = until - System.nanoTime();
                if (left <= 0) {
                    return false;
                }
                stateChanged.awaitNanos(left);
            }
            return live;
        } finally {
            lock.unlock();
        }
    }

    /** {@code api <cmd>}: the response body. A reply starting {@code -ERR} is returned as is. */
    public String api(String cmd) throws Unavailable, Timeout {
        return call(EslCodec.command("api " + cmd)).bodyText();
    }

    /** {@code sendmsg} executing {@code app} on a channel, its argument as a body (§1). */
    public String execute(String uuid, String app, String arg) throws Unavailable, Timeout {
        Map<String, String> h = new LinkedHashMap<>();
        h.put("call-command", "execute");
        h.put("execute-app-name", app);
        h.put("event-lock", "true");
        byte[] body = arg == null ? null : arg.getBytes(StandardCharsets.UTF_8);
        EslFrame r = call(EslCodec.command("sendmsg " + uuid, h, body));
        return r.replyOutcome();
    }

    /**
     * {@code bgapi <cmd>} with our own {@code Job-UUID}: the future completes with the job's
     * result ({@code +OK …} or {@code -ERR …}) from {@code BACKGROUND_JOB}, or exceptionally when ESL
     * goes down first (the outcome is then unknown).
     */
    public CompletableFuture<String> bgapi(String cmd) throws Unavailable, Timeout {
        String job = UUID.randomUUID().toString();
        CompletableFuture<String> f = new CompletableFuture<>();
        jobs.put(job, f);
        try {
            EslFrame r = call(EslCodec.command("bgapi " + cmd, Map.of("Job-UUID", job), null));
            if (!"+OK".equals(r.replyOutcome())) {
                jobs.remove(job);
                f.complete(r.header("Reply-Text") == null ? "-ERR" : "-ERR refused");
            }
        } catch (Unavailable | Timeout | RuntimeException e) {
            if (e instanceof Unavailable) {
                jobs.remove(job);
            }
            throw e;
        }
        return f;
    }

    /** {@code sendevent CUSTOM} with a subclass and headers. */
    public void sendCustomEvent(String subclass, Map<String, String> headers) throws Unavailable, Timeout {
        Map<String, String> h = new LinkedHashMap<>();
        h.put("Event-Subclass", subclass);
        h.putAll(headers);
        call(EslCodec.command("sendevent CUSTOM", h, null));
    }

    private EslFrame call(byte[] block) throws Unavailable, Timeout {
        if (!live) {
            throw new Unavailable();
        }
        List<EslConnection> pool = new ArrayList<>(commands);
        // A connection holding an overdue command is slow: the others first, then the least loaded.
        pool.sort(java.util.Comparator.comparingInt(EslConnection::overdue).thenComparingInt(EslConnection::outstanding));
        for (EslConnection c : pool) {
            if (!c.isOpen()) {
                continue;
            }
            try {
                return c.call(block, config.commandTimeout());
            } catch (EslConnection.Busy e) {
                // the next connection
            } catch (EslConnection.Timeout e) {
                throw new Timeout();
            } catch (EslConnection.Closed | IOException e) {
                throw new Unavailable();
            }
        }
        throw new Unavailable();
    }

    @Override
    public void close() {
        closed = true;
        if (supervisor != null) {
            supervisor.interrupt();
        }
        closeAll();
        signal();
    }

    // ---- the supervisor ------------------------------------------------------------------------

    private void supervise(Handler handler) {
        int attempt = 0;
        boolean first = true;
        while (!closed) {
            boolean wasUp = false;
            try {
                if (!first) {
                    reconnects.incrementAndGet();
                }
                first = false;
                handler.connecting();
                connect(handler);
                live = true;
                signal();
                wasUp = true;
                attempt = 0;
                if (downReported) {
                    downReported = false;
                    LOG.atInfo().addKeyValue("event", "dependency.restored").addKeyValue("dependency", "freeswitch")
                            .log("ESL is connected again");
                }
                handler.up(coreUuid);
                watch();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                // Once per outage: the reconnect attempts that follow are debug only.
                if (!downReported && !closed) {
                    downReported = true;
                    LOG.atWarn().addKeyValue("event", "dependency.unavailable").addKeyValue("dependency", "freeswitch")
                            .setCause(e).log("ESL cycle failed; reconnecting");
                } else {
                    LOG.atDebug().addKeyValue("event", "esl.cycle_failed").addKeyValue("error_kind", e.getClass().getSimpleName())
                            .log("ESL cycle failed");
                }
            } finally {
                live = false;
                closeAll();
                failJobs();
                signal();
                if (wasUp || attempt > 0) {
                    handler.down();
                }
            }
            if (closed) {
                break;
            }
            long cap = Math.min(config.backoffMax().toMillis(), config.backoffMin().toMillis() << Math.min(attempt, 16));
            attempt++;
            try {
                Thread.sleep(ThreadLocalRandom.current().nextLong(Math.max(1, cap) + 1));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void connect(Handler handler) throws Exception {
        InetSocketAddress address = new InetSocketAddress(config.host(), config.port());
        EslConnection.Listener eventListener = new EslConnection.Listener() {
            @Override
            public void event(EslFrame frame) {
                onEvent(frame, handler);
            }

            @Override
            public void closed(EslConnection connection) {
                signal();
            }
        };
        events = EslConnection.open(address, config.password(), config.connectTimeout(), config.pipelineLimit(),
                eventListener, lateReplies);
        lastProbeSeenNanos = System.nanoTime();
        expectOk(events.call(EslCodec.command("event json " + String.join(" ", EVENTS)), config.connectTimeout()));
        for (String f : FILTERS) {
            expectOk(events.call(EslCodec.command("filter " + f), config.connectTimeout()));
        }
        EslConnection.Listener commandListener = new EslConnection.Listener() {
            @Override
            public void event(EslFrame frame) {
                // command connections subscribe to nothing
            }

            @Override
            public void closed(EslConnection connection) {
                signal();
            }
        };
        List<EslConnection> cmds = new ArrayList<>();
        for (int i = 0; i < config.commandConnections(); i++) {
            cmds.add(EslConnection.open(address, config.password(), config.connectTimeout(), config.pipelineLimit(),
                    commandListener, lateReplies));
        }
        commands = List.copyOf(cmds);
        EslFrame r = cmds.get(0).call(EslCodec.command("api global_getvar core_uuid"), config.connectTimeout());
        coreUuid = r.bodyText().trim();
    }

    private static void expectOk(EslFrame reply) throws IOException {
        if (!"+OK".equals(reply.replyOutcome())) {
            throw new IOException("subscription refused");
        }
    }

    /** Probes until the event connection is presumed dead or a connection closes. */
    private void watch() throws InterruptedException {
        long interval = config.probeInterval().toNanos();
        long deadline = config.probeDeadline().toNanos();
        long next = System.nanoTime();
        while (!closed) {
            if (!events.isOpen() || commands.stream().anyMatch(c -> !c.isOpen())) {
                return;
            }
            long now = System.nanoTime();
            if (now - lastProbeSeenNanos > deadline) {
                LOG.atWarn().addKeyValue("event", "esl.probe_timed_out").addKeyValue("deadline_ms", config.probeDeadline().toMillis())
                        .log("no ESL probe came back in time; reconnecting");
                return;
            }
            if (now >= next) {
                next = now + interval;
                Thread.ofVirtual().name("esl-probe").start(this::probe);
            }
            lock.lock();
            try {
                stateChanged.awaitNanos(Math.min(interval, 100_000_000L));
            } finally {
                lock.unlock();
            }
        }
    }

    private void probe() {
        long seq = probeSeq.incrementAndGet();
        probesSent.put(seq, System.nanoTime());
        if (probesSent.size() > 64) {
            probesSent.keySet().removeIf(k -> k < seq - 32);
        }
        try {
            sendCustomEvent("parity::probe", Map.of("parity-probe-seq", Long.toString(seq)));
        } catch (Unavailable | Timeout e) {
            // the deadline decides
        }
    }

    private void onEvent(EslFrame frame, Handler handler) {
        NodeEvent e;
        try {
            e = NodeEvent.parse(frame.body());
        } catch (EslProtocolException ex) {
            LOG.atWarn().addKeyValue("event", "esl_event.skipped").addKeyValue("reason", ex.reason().name())
                    .log("an ESL event that doesn't parse; skipped");
            return;
        }
        if ("CUSTOM".equals(e.name()) && "parity::probe".equals(e.subclass())) {
            long now = System.nanoTime();
            lastProbeSeenNanos = now;
            String seq = e.get("parity-probe-seq");
            Long sent = seq == null ? null : probesSent.remove(parseLong(seq));
            if (sent != null) {
                lastProbeLagNanos.set(now - sent);
            }
            return;
        }
        if ("BACKGROUND_JOB".equals(e.name())) {
            String job = e.get("Job-UUID");
            CompletableFuture<String> f = job == null ? null : jobs.remove(job);
            if (f != null) {
                String body = e.get("_body");
                f.complete(body == null ? "" : body.trim());
            }
            return;
        }
        handler.event(e);
    }

    private static long parseLong(String s) {
        try {
            return Long.parseLong(s.trim());
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private void failJobs() {
        for (String job : List.copyOf(jobs.keySet())) {
            CompletableFuture<String> f = jobs.remove(job);
            if (f != null) {
                f.completeExceptionally(new Unavailable());
            }
        }
    }

    private void closeAll() {
        EslConnection e = events;
        if (e != null) {
            e.close();
        }
        for (EslConnection c : commands) {
            c.close();
        }
    }

    private void signal() {
        lock.lock();
        try {
            stateChanged.signalAll();
        } finally {
            lock.unlock();
        }
    }
}
