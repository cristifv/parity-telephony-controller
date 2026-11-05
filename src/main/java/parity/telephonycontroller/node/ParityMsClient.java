package parity.telephonycontroller.node;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * parity-ms through its own API (requirement 41; TC PLAN step 4): newline JSON over TCP on
 * loopback. A session's media is one call named by its interaction id: {@code open} answers the
 * customer (leg A) alone; {@code attach} makes the offer for an agent (leg B), whose answer comes
 * with {@code answer}; {@code detach} drops B when the agent leaves, so another can follow;
 * {@code update} follows a re-INVITE; {@code claim} raises the fence; {@code delete} ends it.
 *
 * <ul>
 *   <li><b>Fence and tenancy:</b> every mutating request carries {@code owner_epoch} and
 *       {@code tenant_id} (parity-ms checks the tenant only when it is given, MS-029).
 *   <li><b>Idempotency:</b> each request's {@code request_id} names the operation, its subject and
 *       the fence, so a repeat (an IE retry after a timeout) gets parity-ms's first outcome.
 *   <li><b>Pool:</b> at most {@code connections} requests at once, one per connection; taking a
 *       slot waits at most 50 ms (R1-7), else {@code PARITY_MS_UNAVAILABLE}. A request that fails
 *       closes its connection.
 *   <li><b>Restarts:</b> a watcher asks {@code hello} every second; a new incarnation means
 *       parity-ms restarted and every session's media is gone ({@link #onLost}).
 *   <li><b>Never logged:</b> SDPs and parity-ms's error messages (they can echo request values,
 *       MS-035); only the error code.
 * </ul>
 */
public final class ParityMsClient implements MediaPort, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ParityMsClient.class);
    private static final JsonMapper JSON = JsonMapper.builder().build();

    /**
     * Where parity-ms listens, the budgets, the orphan sweep's grace after each full sync, and
     * whether TC may record into parity-ms's null sink (nothing written; local test profiles only,
     * checked by the wiring; MS-012).
     */
    public record Config(String host, int port, Duration timeout, int connections, Duration sweepGrace,
            boolean nullSink) {
        /** The defaults' grace: 3 minutes. */
        public Config(String host, int port, Duration timeout, int connections) {
            this(host, port, timeout, connections, Duration.ofMinutes(3));
        }

        /** No recording sink. */
        public Config(String host, int port, Duration timeout, int connections, Duration sweepGrace) {
            this(host, port, timeout, connections, sweepGrace, false);
        }
    }

    private static final class Conn {
        final Socket socket;
        final BufferedReader in;
        final OutputStream out;

        Conn(Socket socket) throws IOException {
            this.socket = socket;
            this.in = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            this.out = socket.getOutputStream();
        }
    }

    /** A request parity-ms refused, by its error code. */
    static final class Refused extends Exception {
        private static final long serialVersionUID = 1L;
        /** parity-ms's error code (a short snake_case word, never its message). */
        String code = "other";

        Refused() {
            super("parity-ms refused a request", null, false, false);
        }
    }

    private final Config config;
    private final Semaphore slots;
    private final ConcurrentLinkedQueue<Conn> idle = new ConcurrentLinkedQueue<>();
    /** Sessions with media, and the highest fence TC sent each (0: found by {@code list}). */
    private final ConcurrentHashMap<String, Long> sessions = new ConcurrentHashMap<>();
    private final AtomicLong ids = new AtomicLong();
    final AtomicLong refusals = new AtomicLong();
    final AtomicLong staleEpochs = new AtomicLong();
    private volatile String incarnation;
    /** parity-ms's recording sink ({@code hello}): {@code file}, {@code null}, or unknown (an old one). */
    private volatile String parityMsSink = "";
    /** What parity-ms does beyond its base API ({@code hello.features}): play_loop, dtmf_action_locks, ... */
    private volatile java.util.Set<String> features = java.util.Set.of();
    /** How long parity-ms waits for a prompt to load ({@code hello.prompt_wait_ms}); -1: not said (older). */
    private volatile long promptWaitMs = -1;
    /** Agent DTMF codes dropped because an older parity-ms can't lock their facet (TC-048). */
    final AtomicLong dtmfCodesDropped = new AtomicLong();
    private volatile MediaPort.Events events;
    /** The last event processed, for a resubscribe within the same incarnation. */
    private volatile long lastNodeSeq;
    private volatile String eventsIncarnation;
    private volatile Socket eventSocket;
    private volatile Runnable onLost = () -> {};
    private volatile boolean closed;
    /** parity-ms was reported unavailable and not back yet (log-record.v1 dependency.*, once per outage). */
    private volatile boolean downReported;

    public ParityMsClient(Config config) {
        this.config = config;
        this.slots = new Semaphore(config.connections());
    }

    /** Called when parity-ms restarted (a new incarnation): every session's media is gone. */
    public void onLost(Runnable r) {
        onLost = r;
    }

    /** The sessions that still have media, from {@code list}; then the restart watcher starts. */
    @Override
    public void start() {
        try {
            incarnation = hello(request(cmd("hello")));
            for (JsonNode id : request(cmd("list")).path("calls")) {
                sessions.putIfAbsent(id.asString(), 0L);
            }
        } catch (NodePort.Unavailable | Refused e) {
            downReported = true;
            LOG.atWarn().addKeyValue("event", "dependency.unavailable").addKeyValue("dependency", "parity-ms").setCause(e)
                    .log("parity-ms didn't answer at start: sessions are rebuilt without their media");
        }
        Thread.ofVirtual().name("tc-parity-ms-watch").start(this::watch);
        Thread.ofVirtual().name("tc-parity-ms-events").start(this::eventLoop);
        Thread.ofVirtual().name("tc-parity-ms-sweep").start(this::sweepLoop);
    }

    // ---- The sweep (ADR 0003 D7): orphaned calls, and media activity -------------------------------

    /**
     * Every 5 s, but only once the node has been in sync for the grace (after every start and ESL
     * reconnect; must-fix 1): a partial view must never look like an orphan.
     */
    static final long SWEEP_MS = 5_000;
    /** A max fence: deleting an orphan needs one at or above the call's (it is gone afterwards anyway). */
    private static final long SWEEP_EPOCH = (1L << 53) - 1;
    private final ConcurrentHashMap<String, Integer> orphanSweeps = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, long[]> packets = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, int[]> quiet = new ConcurrentHashMap<>();
    final AtomicLong orphansDeleted = new AtomicLong();

    private void sweepLoop() {
        while (!closed) {
            try {
                MediaPort.Events sink = events;
                if (sink != null && sink.syncedForMs() >= config.sweepGrace().toMillis()) {
                    sweep();
                }
            } catch (RuntimeException e) {
                LOG.atWarn().addKeyValue("event", "media_sweep.failed").addKeyValue("error_kind", e.getClass().getSimpleName())
                        .log("a media sweep failed");
            }
            sleepQuietly(SWEEP_MS);
        }
    }

    /**
     * One sweep: a parity-ms call with no live TC leg on two sweeps in a row is deleted (a leak
     * TC's own deletes missed); for the others, a leg whose received packets didn't grow over two
     * sweeps is reported inactive, and active again once they grow.
     */
    void sweep() {
        MediaPort.Events sink = events;
        if (sink == null) {
            return;
        }
        java.util.Set<String> calls = new java.util.HashSet<>();
        try {
            for (JsonNode id : request(cmd("list")).path("calls")) {
                calls.add(id.asString());
            }
        } catch (NodePort.Unavailable | Refused e) {
            return;
        }
        orphanSweeps.keySet().retainAll(calls);
        packets.keySet().removeIf(k -> !calls.contains(k.substring(0, k.length() - 2)));
        quiet.keySet().removeIf(k -> !calls.contains(k.substring(0, k.length() - 2)));
        for (String callId : calls) {
            if (!sink.live(callId)) {
                // Twice in a row, then confirmed with freeswitch right before the delete.
                if (orphanSweeps.merge(callId, 1, Integer::sum) >= 2 && sink.syncedForMs() >= config.sweepGrace().toMillis()
                        && sink.confirmOrphan(callId)) {
                    deleteOrphan(callId);
                }
                continue;
            }
            orphanSweeps.remove(callId);
            JsonNode q;
            try {
                q = request(cmd("query").put("call_id", callId));
            } catch (NodePort.Unavailable | Refused e) {
                continue;
            }
            for (Side side : Side.values()) {
                JsonNode leg = q.path(side(side));
                if (leg.isMissingNode() || leg.isNull()) {
                    continue;
                }
                activity(sink, callId, side, leg.path("rtp").path("received").path("packets").asLong(0));
            }
        }
    }

    private void activity(MediaPort.Events sink, String callId, Side side, long received) {
        String k = callId + "/" + side(side);
        long[] before = packets.put(k, new long[] {received});
        if (before == null) {
            return;
        }
        int[] q = quiet.computeIfAbsent(k, x -> new int[1]);
        if (received > before[0]) {
            if (q[0] >= 2) {
                sink.activity(callId, side, true);
            }
            q[0] = 0;
        } else if (++q[0] == 2) {
            sink.activity(callId, side, false);
        }
    }

    private void deleteOrphan(String callId) {
        try {
            request(cmd("delete").put("call_id", callId).put("owner_epoch", SWEEP_EPOCH)
                    .put("request_id", "tc.sweep." + callId));
            orphansDeleted.incrementAndGet();
            sessions.remove(callId);
            orphanSweeps.remove(callId);
            LOG.atWarn().addKeyValue("event", "media_call.orphan_deleted").addKeyValue("interaction_id", callId)
                    .log("deleted a parity-ms call with no live leg in TC");
        } catch (NodePort.Unavailable | Refused e) {
            LOG.atWarn().addKeyValue("event", "media_call.orphan_delete_failed").addKeyValue("interaction_id", callId)
                    .log("couldn't delete an orphaned parity-ms call");
        }
    }

    @Override
    public void subscribe(MediaPort.Events e) {
        events = e;
    }

    /** Takes what {@code hello} says about parity-ms's features and sink; returns its incarnation. */
    private String hello(JsonNode h) {
        java.util.Set<String> f = new java.util.HashSet<>();
        h.path("features").forEach(x -> f.add(x.asString("")));
        features = java.util.Set.copyOf(f);
        long wait = h.path("prompt_wait_ms").asLong(-1);
        if (wait != promptWaitMs) {
            promptWaitMs = wait;
            if (promptWaitTooLong()) {
                // A play TC gave up on could still start (MS-042): plays are refused until it is fixed.
                LOG.atError().addKeyValue("event", "media.prompt_wait_refused").addKeyValue("prompt_wait_ms", wait).addKeyValue("timeout_ms", config.timeout().toMillis())
                        .log("parity-ms waits for a prompt as long as TC's timeout or longer: plays are refused");
            }
        }
        String sink = h.path("recording_sink").asString("");
        if (!sink.equals(parityMsSink)) {
            parityMsSink = sink;
            LOG.atInfo().addKeyValue("event", "media.recording_sink_reported").addKeyValue("sink", sink.isEmpty() ? "unknown" : sink)
                    .addKeyValue("tc_allows_null_sink", config.nullSink()).log("parity-ms's recording sink");
        }
        return h.path("incarnation").asString(null);
    }

    /** parity-ms's prompt wait from its {@code hello}, in milliseconds; -1 if it doesn't say (an older one). */
    public long promptWaitMs() {
        return promptWaitMs;
    }

    /**
     * parity-ms waits for a prompt at least as long as TC waits for its answer: a play TC gave up
     * on (and retried, or reported failed) could start after all, a loop nobody tracks (MS-042).
     */
    public boolean promptWaitTooLong() {
        long wait = promptWaitMs;
        // Asynchronous plays (the play saga, ADR 0004) never wait for a prompt: no gate then.
        return !asyncPlays() && wait >= 0 && wait >= config.timeout().toMillis();
    }

    /**
     * {@code null} only when TC's configuration allows the null sink and parity-ms runs it: TC never
     * records into parity-ms's file sink (raw PCM in clear, MS-012) until R9.
     */
    @Override
    public String recordingSink() {
        return config.nullSink() && "null".equals(parityMsSink) ? "null" : "none";
    }

    /** The event stream (ADR 0003 D5): {@code subscribe}, replaying from the last event of this run. */
    private void eventLoop() {
        while (!closed) {
            try (Socket s = new Socket()) {
                eventSocket = s;
                s.setTcpNoDelay(true);
                s.connect(new InetSocketAddress(config.host(), config.port()), (int) config.timeout().toMillis());
                BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
                ObjectNode sub = cmd("subscribe");
                if (eventsIncarnation != null) {
                    sub.put("incarnation", eventsIncarnation).put("after_node_seq", lastNodeSeq);
                }
                s.getOutputStream().write((JSON.writeValueAsString(sub) + "\n").getBytes(StandardCharsets.UTF_8));
                JsonNode answer = JSON.readTree(in.readLine());
                if (!answer.path("ok").asBoolean(false)) {
                    // replay_gap: older than the ring; take the live tail (the sweep reconciles).
                    eventsIncarnation = null;
                    LOG.atWarn().addKeyValue("event", "media.subscribe_refused")
                            .addKeyValue("error_code", code(answer.path("error").path("code").asString("")))
                            .log("parity-ms refused the event subscription");
                    sleepQuietly(1_000);
                    continue;
                }
                String inc = answer.path("result").path("incarnation").asString(null);
                if (inc != null && !inc.equals(eventsIncarnation)) {
                    eventsIncarnation = inc;
                    lastNodeSeq = 0;
                }
                String line;
                while ((line = in.readLine()) != null) {
                    JsonNode ev = JSON.readTree(line).path("event");
                    long seq = ev.path("node_seq").asLong(0);
                    if (seq > 0) {
                        lastNodeSeq = seq;
                    }
                    dispatch(ev);
                }
            } catch (IOException | RuntimeException e) {
                // parity-ms down or the stream replaced: subscribe again
            } finally {
                eventSocket = null;
            }
            sleepQuietly(1_000);
        }
    }

    private void dispatch(JsonNode ev) {
        MediaPort.Events sink = events;
        String type = ev.path("type").asString("");
        String callId = ev.path("call_id").asString(ev.path("key").asString(""));
        if (sink == null || callId.isEmpty()) {
            return;
        }
        Side side = "b".equals(ev.path("leg").asString("a")) ? Side.B : Side.A;
        switch (type) {
            case "play.finished" -> sink.played(callId, side, ev.path("reason").asString(""), ev.path("played_ms").asLong(0),
                    ev.path("play_id").asString(""));
            // The play saga (ADR 0004): only from a parity-ms whose plays TC sends asynchronously.
            case "play.started" -> {
                if (asyncPlays()) {
                    sink.playStarted(callId, side, ev.path("play_id").asString(""));
                }
            }
            case "play.failed" -> {
                if (asyncPlays()) {
                    sink.playFailed(callId, side, ev.path("play_id").asString(""), code(ev.path("reason").asString("")));
                }
            }
            case "dtmf.action" -> sink.dtmfAction(callId, side, ev.path("action").asString("").toUpperCase(java.util.Locale.ROOT),
                    ev.path("result").asString("").toUpperCase(java.util.Locale.ROOT),
                    ev.path("state").path("muted").asBoolean(false),
                    ev.path("state").path("recording").asString("").toUpperCase(java.util.Locale.ROOT));
            case "dtmf" -> sink.digit(callId, side, ev.path("digit").asString(""), ev.path("duration_ms").asInt(0),
                    ev.path("source").asString(""));
            case "call.ended" -> {
                String reason = ev.path("reason").asString("");
                if (!"deleted".equals(reason) && sessions.remove(callId) != null) {
                    sink.ended(callId, reason);
                }
            }
            default -> {
                // others (leg.attached, recording.*, thread.*) aren't needed yet
            }
        }
    }

    private static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ---- Step 4: media commands (ADR 0003 D6) ------------------------------------------------------

    private static String side(Side s) {
        return s == Side.A ? "a" : "b";
    }

    @Override
    public void play(String tenantId, String interactionId, Side side, String file, long epoch) throws NodePort.Unavailable {
        play(tenantId, interactionId, side, file, false, epoch);
    }

    /**
     * Plays a prompt; {@code loop}: parity-ms repeats it with no gap until stopped (R7), and says so
     * by echoing {@code loop} (an older one ignores the field and plays it once).
     */
    @Override
    public boolean play(String tenantId, String interactionId, Side side, String file, boolean loop, long epoch)
            throws NodePort.Unavailable {
        return play(tenantId, interactionId, side, file, loop, null, epoch);
    }

    /**
     * The same, under TC's {@code playId} for this attempt: parity-ms echoes it in {@code play.finished}
     * (an older one ignores it, and plays end in order). Refused while parity-ms's prompt wait isn't
     * shorter than TC's timeout (MS-042).
     */
    @Override
    public boolean play(String tenantId, String interactionId, Side side, String file, boolean loop, String playId,
            long epoch) throws NodePort.Unavailable {
        if (promptWaitTooLong()) {
            refusals.incrementAndGet();
            throw new NodePort.Unavailable(NodePort.Part.PARITY_MS);
        }
        ObjectNode c = mutate("play", tenantId, interactionId, epoch, null).put("leg", side(side)).put("file", file);
        if (loop) {
            c.put("loop", true);
        }
        if (playId != null && !playId.isEmpty()) {
            c.put("play_id", playId);
        }
        try {
            return request(c).path("loop").asBoolean(false);
        } catch (Refused e) {
            if (("not_found".equals(e.code) || "bad_request".equals(e.code)) && sessions.containsKey(interactionId)) {
                // The call is there, so the file is missing, unreadable or corrupt: PROMPT_UNKNOWN, and
                // only this Play fails (R7).
                LOG.atWarn().addKeyValue("event", "play.prompt_unavailable").addKeyValue("interaction_id", interactionId).addKeyValue("code", e.code)
                        .log("parity-ms can't play a prompt");
                throw new NodePort.Unavailable(NodePort.Part.PROMPT);
            }
            throw refused(e, "play", interactionId);
        }
    }

    /** parity-ms answers {@code play} at once and reports by {@code play_id} (its {@code play_async}). */
    @Override
    public boolean asyncPlays() {
        return features.contains("play_async");
    }

    /**
     * The play saga (ADR 0004): {@code play} with {@code async}, under TC's id for the play's whole
     * life. parity-ms answers at once ({@code existing} for an id it had); {@code play.started} or
     * {@code play.failed} follows on the event stream. A refusal (the leg, the fence) is final; no
     * answer is {@link NodePort.Unavailable}: parity-ms may have it or not, and a retry repeats the id.
     */
    @Override
    public NodePort.PlaySent playAsync(String tenantId, String interactionId, Side side, String file, boolean loop,
            String playId, long epoch) throws NodePort.Unavailable {
        ObjectNode c = mutate("play", tenantId, interactionId, epoch, null).put("leg", side(side)).put("file", file)
                .put("play_id", playId).put("async", true);
        if (loop) {
            c.put("loop", true);
        }
        try {
            return request(c).path("existing").asBoolean(false) ? NodePort.PlaySent.EXISTING : NodePort.PlaySent.ACCEPTED;
        } catch (Refused e) {
            refused(e, "play", interactionId);
            return NodePort.PlaySent.REFUSED;
        }
    }

    /** The play saga (ADR 0004): {@code stop_play} of that play only; repeat-safe in parity-ms. */
    @Override
    public void stopPlay(String tenantId, String interactionId, Side side, String playId, long epoch)
            throws NodePort.Unavailable {
        call(mutate("stop_play", tenantId, interactionId, epoch, null).put("leg", side(side)).put("play_id", playId));
    }

    /** A supervisor's mode codes (the MODE facet). */
    private static final java.util.Set<String> MODE_ACTIONS = java.util.Set.of("LISTEN", "WHISPER", "BARGE");

    @Override
    public void dtmfActions(String tenantId, String interactionId, Side side, String role,
            java.util.List<NodePort.DtmfBinding> bindings, int interDigitMs, java.util.List<String> lock,
            java.util.List<String> unlock, long epoch) throws NodePort.Unavailable {
        ObjectNode c = mutate("set_dtmf_actions", tenantId, interactionId, epoch, null).put("leg", side(side))
                .put("role", role).put("inter_digit_timeout_ms", interDigitMs);
        boolean locks = features.contains("dtmf_action_locks");
        // An older parity-ms can't lock RECORDING or MODE with the codes: the codes that would change a
        // locked facet are left out instead (TC-048), so the leg keeps its other codes and the call
        // goes on; DTMF still can't change what is locked (consent).
        java.util.List<NodePort.DtmfBinding> sent = locks ? bindings : bindings.stream()
                .filter(b -> !(lock.contains("RECORDING") && b.action().startsWith("RECORDING_"))
                        && !(lock.contains("MODE") && MODE_ACTIONS.contains(b.action())))
                .toList();
        if (sent.size() < bindings.size()) {
            dtmfCodesDropped.addAndGet(bindings.size() - sent.size());
            LOG.atWarn().addKeyValue("event", "media.dtmf_actions_limited").addKeyValue("tenant_id", tenantId)
                    .addKeyValue("interaction_id", interactionId).addKeyValue("dropped", bindings.size() - sent.size())
                    .log("parity-ms can't lock the recording or mode with the DTMF codes (an older release): those codes are left out");
        }
        tools.jackson.databind.node.ArrayNode actions = c.putArray("actions");
        for (NodePort.DtmfBinding b : sent) {
            actions.addObject().put("sequence", b.sequence()).put("action", b.action().toLowerCase(java.util.Locale.ROOT));
        }
        if (locks) {
            // The facets ride on the codes' own request (TC-028): RECORDING (the session's, consent)
            // and MODE pinned without a recording or mode command.
            tools.jackson.databind.node.ArrayNode l = c.putArray("lock");
            lock.forEach(f -> l.add(f.toLowerCase(java.util.Locale.ROOT)));
            tools.jackson.databind.node.ArrayNode u = c.putArray("unlock");
            unlock.forEach(f -> u.add(f.toLowerCase(java.util.Locale.ROOT)));
            call(c);
            return;
        }
        call(c);
        if (lock.contains("MUTE")) {
            // Pins the leg's mute as it is: DTMF can't change it (a lock rides on mute/unmute).
            JsonNode q = call(cmd("query").put("call_id", interactionId));
            boolean muted = q.path(side(side)).path("muted").asBoolean(false);
            ObjectNode m = mutate(muted ? "mute" : "unmute", tenantId, interactionId, epoch, null).put("leg", side(side));
            m.putArray("lock").add("mute");
            call(m);
        }
    }

    @Override
    public boolean stopPlay(String tenantId, String interactionId, Side side, long epoch) throws NodePort.Unavailable {
        return call(mutate("stop_play", tenantId, interactionId, epoch, null).put("leg", side(side)))
                .path("stopped").asBoolean(false);
    }

    @Override
    public boolean mute(String tenantId, String interactionId, Side side, boolean muted, long epoch)
            throws NodePort.Unavailable {
        return call(mutate(muted ? "mute" : "unmute", tenantId, interactionId, epoch, null).put("leg", side(side)))
                .path("changed").asBoolean(false);
    }

    @Override
    public void sendDtmf(String tenantId, String interactionId, Side side, String digits, long epoch)
            throws NodePort.Unavailable {
        call(mutate("send_dtmf", tenantId, interactionId, epoch, null).put("leg", side(side)).put("digits", digits));
    }

    @Override
    public void reportDigits(String tenantId, String interactionId, Side side, String mode, long epoch)
            throws NodePort.Unavailable {
        call(mutate("set_leg", tenantId, interactionId, epoch, null).put("leg", side(side)).put("report_digits", mode));
    }

    private void watch() {
        while (!closed) {
            try {
                Thread.sleep(1_000);
            } catch (InterruptedException e) {
                return;
            }
            try {
                String now = hello(request(cmd("hello")));
                if (downReported) {
                    downReported = false;
                    LOG.atInfo().addKeyValue("event", "dependency.restored").addKeyValue("dependency", "parity-ms")
                            .log("parity-ms answers again");
                }
                String before = incarnation;
                incarnation = now;
                if (before != null && now != null && !before.equals(now)) {
                    LOG.atError().addKeyValue("event", "media.restarted")
                            .log("parity-ms restarted (a new incarnation): every session's media is gone");
                    sessions.clear();
                    onLost.run();
                }
            } catch (NodePort.Unavailable | Refused e) {
                // down: the next hello tells whether it came back as itself. Reported once per outage.
                if (!downReported && !closed) {
                    downReported = true;
                    LOG.atWarn().addKeyValue("event", "dependency.unavailable").addKeyValue("dependency", "parity-ms")
                            .setCause(e).log("parity-ms doesn't answer its hello");
                }
            }
        }
    }

    public boolean live() {
        try {
            request(cmd("ping"));
            return true;
        } catch (NodePort.Unavailable | Refused e) {
            return false;
        }
    }

    @Override
    public String open(String tenantId, String interactionId, String offerA, long epoch) throws NodePort.Unavailable {
        ObjectNode c = mutate("open", tenantId, interactionId, epoch, "tc.open." + interactionId + "." + epoch)
                .put("interaction_id", interactionId).put("sdp", offerA);
        c.putArray("codecs").add("PCMU").add("PCMA");
        JsonNode r = call(c);
        sessions.merge(interactionId, epoch, Math::max);
        return r.path("sdp").asString("");
    }

    @Override
    public String attach(String tenantId, String interactionId, String leg, boolean webrtc, long epoch)
            throws NodePort.Unavailable {
        if (!sessions.containsKey(interactionId)) {
            // A first leg TC originates (agent-first, outbound): parity-ms's API has no offer for an
            // A it would have to call (BUGS.md TC-008).
            LOG.atWarn().addKeyValue("event", "media_call.missing").addKeyValue("tenant_id", tenantId)
                    .addKeyValue("interaction_id", interactionId)
                    .log("a first leg TC originates has no parity-ms call to attach to");
            throw new NodePort.Unavailable(NodePort.Part.PARITY_MS);
        }
        String rid = "tc.attach." + leg + "." + epoch;
        JsonNode r;
        try {
            r = request(attachCmd(tenantId, interactionId, epoch, rid, webrtc));
        } catch (Refused e) {
            if (!"already_exists".equals(e.code)) {
                throw refused(e, "attach", interactionId);
            }
            // The previous agent's detach may still be on its way (it runs off the ESL reader): once
            // more after it. A B that is really there (a third party) stays refused.
            sleep(100);
            try {
                r = request(attachCmd(tenantId, interactionId, epoch, rid + ".r", webrtc));
            } catch (Refused again) {
                throw refused(again, "attach", interactionId);
            }
        }
        sessions.merge(interactionId, epoch, Math::max);
        return r.path("sdp").asString("");
    }

    private ObjectNode attachCmd(String tenantId, String interactionId, long epoch, String requestId, boolean webrtc) {
        ObjectNode c = mutate("attach", tenantId, interactionId, epoch, requestId).put("leg", "b");
        if (webrtc) {
            c.put("webrtc", true);
        }
        return c;
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void complete(String tenantId, String interactionId, String leg, String answer, long epoch)
            throws NodePort.Unavailable {
        call(mutate("answer", tenantId, interactionId, epoch, "tc.answer." + leg + "." + epoch).put("leg", "b")
                .put("sdp", answer));
    }

    @Override
    public NodePort.RecordingResult detach(String tenantId, String interactionId, String leg, long epoch)
            throws NodePort.Unavailable {
        if (!sessions.containsKey(interactionId)) {
            return null;
        }
        JsonNode r = call(mutate("detach", tenantId, interactionId, epoch, "tc.detach." + leg + "." + epoch).put("leg", "b"));
        return recordingResult(r.path("stats").path("recording"));
    }

    @Override
    public void startRecording(String tenantId, String interactionId, Side side, String recordingId, long epoch)
            throws NodePort.Unavailable {
        if (!"null".equals(recordingSink())) {
            throw new NodePort.Unavailable(NodePort.Part.RECORDING);
        }
        ObjectNode c = mutate("start_recording", tenantId, interactionId, epoch, "tc.rec." + recordingId).put("leg", side(side));
        try {
            JsonNode r = request(c);
            // parity-ms's own answer must agree: a path means a file was made (never in clear, MS-012).
            if (!r.path("path").asString("").isEmpty()) {
                LOG.atError().addKeyValue("event", "recording.sink_violated").addKeyValue("tenant_id", tenantId)
                        .addKeyValue("interaction_id", interactionId)
                        .log("parity-ms opened a recording file although its sink is null: stopped at once");
                stopRecording(tenantId, interactionId, side, epoch);
                throw new NodePort.Unavailable(NodePort.Part.RECORDING);
            }
        } catch (Refused e) {
            throw "invalid_state".equals(e.code) ? new NodePort.Unavailable(NodePort.Part.RECORDING)
                    : refused(e, "start_recording", interactionId);
        }
    }

    @Override
    public NodePort.RecordingResult stopRecording(String tenantId, String interactionId, Side side, long epoch)
            throws NodePort.Unavailable {
        try {
            return recordingResult(request(mutate("stop_recording", tenantId, interactionId, epoch, null).put("leg", side(side))));
        } catch (Refused e) {
            if ("not_found".equals(e.code) || "invalid_state".equals(e.code)) {
                // Not recording any more (its leg went, or it was stopped): nothing to stop here.
                return null;
            }
            throw refused(e, "stop_recording", interactionId);
        }
    }

    /** parity-ms's final recording status as TC reports it; null if there was none. */
    static NodePort.RecordingResult recordingResult(JsonNode st) {
        if (st == null || st.isMissingNode() || st.isNull() || !st.isObject()) {
            return null;
        }
        java.util.List<NodePort.RecordingPause> pauses = new java.util.ArrayList<>();
        for (JsonNode p : st.path("pauses")) {
            parity.telephonycontroller.model.Kinds.Role role = switch (p.path("role").asString("")) {
                case "agent" -> parity.telephonycontroller.model.Kinds.Role.AGENT;
                case "supervisor" -> parity.telephonycontroller.model.Kinds.Role.SUPERVISOR;
                case "customer" -> parity.telephonycontroller.model.Kinds.Role.CUSTOMER;
                default -> null;
            };
            pauses.add(new NodePort.RecordingPause(p.path("start_ms").asLong(0), p.path("end_ms").asLong(0), role,
                    "dtmf".equals(p.path("reason").asString(""))));
        }
        return new NodePort.RecordingResult(st.path("duration_ms").asLong(0), java.util.List.copyOf(pauses),
                st.path("path").asString(""), st.path("bytes").asLong(0), "");
    }

    @Override
    public void updateRemote(String tenantId, String interactionId, Side side, String sdp, long epoch)
            throws NodePort.Unavailable {
        if (!sessions.containsKey(interactionId)) {
            return;
        }
        Sdp parsed = Sdp.parse(sdp);
        String which = side == Side.A ? "a" : "b";
        ObjectNode c = mutate("update", tenantId, interactionId, epoch,
                "tc.update." + interactionId + "." + which + "." + parsed.version() + "." + epoch);
        c.putObject(which).put("remote", parsed.address() + ":" + parsed.port());
        call(c);
    }

    @Override
    public void claim(String tenantId, String interactionId, long epoch) throws NodePort.Unavailable {
        if (!sessions.containsKey(interactionId)) {
            return;
        }
        call(cmd("claim").put("call_id", interactionId).put("owner_epoch", epoch));
        sessions.merge(interactionId, epoch, Math::max);
    }

    @Override
    public java.util.Map<Side, NodePort.RecordingResult> end(String tenantId, String interactionId, long epoch)
            throws NodePort.Unavailable {
        Long known = sessions.remove(interactionId);
        if (known == null) {
            return java.util.Map.of();
        }
        JsonNode r = call(mutate("delete", tenantId, interactionId, Math.max(known, epoch), "tc.delete." + interactionId));
        java.util.Map<Side, NodePort.RecordingResult> out = new java.util.EnumMap<>(Side.class);
        for (Side side : Side.values()) {
            NodePort.RecordingResult res = recordingResult(r.path(side(side)).path("recording"));
            if (res != null) {
                out.put(side, res);
            }
        }
        return out;
    }

    @Override
    public long epoch(String interactionId) {
        return sessions.getOrDefault(interactionId, -1L);
    }

    @Override
    public void close() {
        closed = true;
        Socket es = eventSocket;
        if (es != null) {
            try {
                es.close();
            } catch (IOException e) {
                // closing
            }
        }
        Conn c;
        while ((c = idle.poll()) != null) {
            closeQuietly(c);
        }
    }

    // ---- the wire ------------------------------------------------------------------------------

    private ObjectNode cmd(String name) {
        return JSON.createObjectNode().put("cmd", name).put("id", Long.toString(ids.incrementAndGet()));
    }

    /** A mutating request; {@code requestId} null for the step-4 commands, which TC dedupes itself (by command id). */
    private ObjectNode mutate(String name, String tenantId, String callId, long epoch, String requestId) {
        ObjectNode o = cmd(name).put("call_id", callId).put("tenant_id", tenantId);
        if (requestId != null) {
            o.put("request_id", requestId);
        }
        if (epoch > 0) {
            o.put("owner_epoch", epoch);
        }
        return o;
    }

    /** A request whose refusal is parity-ms being unavailable for it (counted and logged by code). */
    private JsonNode call(ObjectNode req) throws NodePort.Unavailable {
        try {
            return request(req);
        } catch (Refused e) {
            throw refused(e, req.path("cmd").asString(""), req.path("call_id").asString(""));
        }
    }

    private NodePort.Unavailable refused(Refused e, String cmd, String interactionId) {
        refusals.incrementAndGet();
        if ("stale_epoch".equals(e.code)) {
            staleEpochs.incrementAndGet();
        }
        LOG.atWarn().addKeyValue("event", "media.request_refused").addKeyValue("cmd", cmd).addKeyValue("error_code", e.code)
                .addKeyValue("interaction_id", interactionId).log("parity-ms refused a request");
        return new NodePort.Unavailable(NodePort.Part.PARITY_MS);
    }

    /**
     * One request and its answer, within {@code timeout} in all (the pool's slot, the connect and every
     * read): past it TC closes the connection, which tells parity-ms its client gave up (a play then
     * never starts, MS-042). A transport failure is parity-ms being unavailable (nothing known to have
     * changed).
     */
    private JsonNode request(ObjectNode req) throws NodePort.Unavailable, Refused {
        long deadline = System.nanoTime() + config.timeout().toNanos();
        try {
            if (!slots.tryAcquire(Math.min(50, config.timeout().toMillis()), TimeUnit.MILLISECONDS)) {
                throw new NodePort.Unavailable(NodePort.Part.PARITY_MS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new NodePort.Unavailable(NodePort.Part.PARITY_MS);
        }
        Conn c = null;
        try {
            c = idle.poll();
            if (c == null) {
                c = connect(remainingMs(deadline));
            }
            c.out.write((JSON.writeValueAsString(req) + "\n").getBytes(StandardCharsets.UTF_8));
            c.out.flush();
            String id = req.path("id").asString("");
            JsonNode r;
            do {
                c.socket.setSoTimeout(remainingMs(deadline));
                String line = c.in.readLine();
                if (line == null) {
                    throw new IOException("closed");
                }
                r = JSON.readTree(line);
            } while (!id.equals(r.path("id").asString("")));
            idle.add(c);
            c = null;
            if (!r.path("ok").asBoolean(false)) {
                Refused refused = new Refused();
                refused.code = code(r.path("error").path("code").asString(""));
                throw refused;
            }
            return r.path("result");
        } catch (IOException | RuntimeException e) {
            throw new NodePort.Unavailable(NodePort.Part.PARITY_MS);
        } finally {
            if (c != null) {
                closeQuietly(c);
            }
            slots.release();
        }
    }

    /** parity-ms's error code if it looks like one (never free text). */
    private static String code(String c) {
        return c.matches("[a-z_]{1,32}") ? c : "other";
    }

    /** What is left of a request's budget, in milliseconds (at least 1: 0 would mean no timeout). */
    private static int remainingMs(long deadline) throws java.net.SocketTimeoutException {
        long left = (deadline - System.nanoTime()) / 1_000_000;
        if (left <= 0) {
            throw new java.net.SocketTimeoutException();
        }
        return (int) Math.min(Integer.MAX_VALUE, left);
    }

    private Conn connect(int timeoutMs) throws IOException {
        Socket s = new Socket();
        s.setTcpNoDelay(true);
        s.connect(new InetSocketAddress(config.host(), config.port()), timeoutMs);
        return new Conn(s);
    }

    private static void closeQuietly(Conn c) {
        try {
            c.socket.close();
        } catch (IOException e) {
            // closing
        }
    }
}
