package parity.telephonycontroller.scripts;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import parity.telephonycontroller.api.GrpcServer;
import parity.telephonycontroller.esl.EslClient;
import parity.telephonycontroller.fake.ScriptHooks;
import parity.telephonycontroller.journal.FileJournal;
import parity.telephonycontroller.model.Kinds.LegState;
import parity.telephonycontroller.node.MediaPort;
import parity.telephonycontroller.node.RealNode;
import parity.telephonycontroller.node.SimulatedMedia;
import parity.telephonycontroller.owner.EtcdOwnerDirectory;
import parity.telephonycontroller.realnode.Freeswitch;
import parity.telephonycontroller.realnode.TestSipAgent;
import parity.telephonycontroller.session.EngineHolder;
import parity.telephonycontroller.session.TelephonyNode;
import parity.telephonycontroller.time.ManualClock;
import tools.jackson.databind.JsonNode;

/**
 * The scripts on real freeswitch (TC ADR 0002 §10): TC's engine and gRPC server in process, its
 * {@link RealNode} on ESL to freeswitch 1.10.12 with TC's configuration and the test overlay
 * (host network), etcd in a container, and {@link TestSipAgent} playing the carrier, the agents and
 * the PSTN. Media is simulated (SIP endpoints that ignore it, §6). The clock is the scripts'.
 *
 * <p>The far ends' injects become SIP; each waits until TC has heard of it. {@code freeswitch_lost}
 * kills and restarts freeswitch; {@code freeswitch_unreachable} is the node's partition switch.
 */
final class RealNodeTarget implements ScriptTarget {

    static final int AGENT_PORT = 5070;

    private final GenericContainer<?> etcd;
    private final EtcdTestWriter writer;
    private final Freeswitch freeswitch;
    private final TestSipAgent agent;
    private volatile MediaPort media;
    /** The simulated media, when the target runs with it (the scripts); null with parity-ms. */
    private final SimulatedMedia sim;
    private final RealNode.Config nodeConfig;
    private final Path journalDir;
    private volatile RealNode node;
    private final ManualClock clock = new ManualClock(0);
    private final FileJournal journal;
    private final EngineHolder holder;
    private final ScriptHooks hooks;
    private final GrpcServer server;
    private final ManagedChannel channel;
    /** An arrival's leg id → its carrier call's Call-ID. */
    private final Map<String, String> carrierCalls = new ConcurrentHashMap<>();
    private final Map<String, Integer> sdpVersions = new ConcurrentHashMap<>();
    private int nextPort = 41_000;

    RealNodeTarget() throws Exception {
        this(new SimulatedMedia(), null);
    }

    /**
     * With {@code media} (simulated, or parity-ms over TCP) and an agent registrar domain (agents
     * registered on freeswitch, e.g. the browser; null: every agent through the SIP agent).
     */
    @SuppressWarnings("resource")
    RealNodeTarget(MediaPort media, String registrarDomain) throws Exception {
        this.media = media;
        this.sim = media instanceof SimulatedMedia s ? s : null;
        if (sim != null) {
            // The recording script runs on the null sink (nothing written), as the fake profile.
            sim.setNullSink(true);
        }
        etcd = new GenericContainer<>(DockerImageName.parse(ServerTarget.ETCD_IMAGE).asCompatibleSubstituteFor("etcd"))
                .withLabel("parity.test", "parity-telephony-controller")
                .withCommand("etcd", "--listen-client-urls=http://0.0.0.0:2379",
                        "--advertise-client-urls=http://127.0.0.1:2379", "--data-dir=/tmp/etcd")
                .withExposedPorts(2379)
                .waitingFor(Wait.forHttp("/health").forPort(2379).withStartupTimeout(Duration.ofSeconds(60)));
        etcd.start();
        URI base = URI.create("http://" + etcd.getHost() + ":" + etcd.getMappedPort(2379));
        writer = new EtcdTestWriter(base, etcd.getContainerId());
        EtcdOwnerDirectory owners = new EtcdOwnerDirectory(base, Duration.ofMillis(500));
        freeswitch = Freeswitch.start();
        agent = new TestSipAgent(AGENT_PORT);
        nodeConfig = new RealNode.Config(1000, Duration.ofSeconds(3), 60, "sip:127.0.0.1:" + AGENT_PORT,
                parity.telephonycontroller.node.AgentRegistrar.parse(registrarDomain),
                Map.of("carrier-a", "127.0.0.1:" + AGENT_PORT), freeswitch.cdrDir(), 61_000, 61_099, true);
        node = newNode();
        journalDir = Files.createTempDirectory("tc-journal");
        journal = new FileJournal(journalDir);
        TelephonyNode.Config config = new TelephonyNode.Config("media-1", "test", Set.of("bin-a"),
                "00000000-0000-7000-8000-000000000000", List.of());
        holder = new EngineHolder(config, clock, owners, journal, node);
        holder.start();
        hooks = new ScriptHooks(holder, clock, writer, owners, journal::clear);
        server = new GrpcServer(0, holder, true).start();
        channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.port()).usePlaintext().build();
    }

    private RealNode newNode() throws InterruptedException {
        return newNode(0);
    }

    /** A new node; its first {@code failSnapshots} snapshots time out (the must-fix-1 test). */
    private RealNode newNode(int failSnapshots) throws InterruptedException {
        EslClient esl = new EslClient(EslClient.Config.defaults("127.0.0.1", 8021, Freeswitch.ESL_PASSWORD));
        RealNode n = new RealNode(nodeConfig, esl, media, clock);
        n.failNextSnapshots(failSnapshots);
        if (!n.start(Duration.ofSeconds(60))) {
            throw new IllegalStateException("ESL never came up");
        }
        return n;
    }

    RealNode node() {
        return node;
    }

    /**
     * TC's process dies and comes back (what a {@code kill -9} leaves): the engine stops without
     * ending anything, ESL closes, and a new node and engine rebuild from freeswitch's channels and
     * the journal under a new incarnation. Returns how long the rebuild took, in milliseconds (from
     * the new ESL connection to the engine answering).
     */
    long restartProcess(TelephonyNode.Config config) throws InterruptedException {
        return restartProcess(config, media);
    }

    /** As {@link #restartProcess(TelephonyNode.Config)}, the new process with its own media client. */
    long restartProcess(TelephonyNode.Config config, MediaPort newMedia) throws InterruptedException {
        return restartProcess(config, newMedia, 0);
    }

    /** As above, the new node's first {@code failSnapshots} snapshots timing out. */
    long restartProcess(TelephonyNode.Config config, MediaPort newMedia, int failSnapshots) throws InterruptedException {
        TelephonyNode old = holder.get();
        if (old != null) {
            old.close();
        }
        node.close();
        media = newMedia;
        long t0 = System.nanoTime();
        newMedia.start();
        node = newNode(failSnapshots);
        holder.reset(config, node);
        return (System.nanoTime() - t0) / 1_000_000;
    }

    TestSipAgent agent() {
        return agent;
    }

    EtcdTestWriter writer() {
        return writer;
    }

    ScriptHooks hooks() {
        return hooks;
    }

    EngineHolder holder() {
        return holder;
    }

    FileJournal journal() {
        return journal;
    }

    ManualClock clock() {
        return clock;
    }

    Freeswitch freeswitch() {
        return freeswitch;
    }

    @Override
    public Map<Long, Long> reset(JsonNode given) throws Exception {
        agent.hangUpAll();
        node.esl().api("hupall NORMAL_CLEARING cc_managed true");
        await(() -> {
            try {
                return node.esl().api("show channels count").contains("0 total");
            } catch (EslClient.Unavailable | EslClient.Timeout e) {
                return false;
            }
        }, 5_000, "freeswitch still has channels");
        agent.forget();
        // The scripts' clock is in the past: every CDR file looks recent to a rebuild, so they go.
        try (java.util.stream.Stream<Path> cdrs = Files.list(freeswitch.cdrDir())) {
            for (Path p : cdrs.toList()) {
                Files.deleteIfExists(p);
            }
        }
        carrierCalls.clear();
        sdpVersions.clear();
        if (sim != null) {
            sim.lose();
            sim.setUp(true);
        }
        int free = given.has("free_legs") ? given.get("free_legs").asInt() : 1000;
        node.resetForTest(free);
        return hooks.reset(given, n -> node);
    }

    @Override
    public long inject(String what, JsonNode a) throws Exception {
        String leg = a.has("leg_id") ? a.get("leg_id").asString() : null;
        switch (what) {
            case "arrival" -> arrival(a);
            case "leg_ring" -> {
                agentCall(leg).ring();
                awaitReported(leg, LegState.RINGING);
            }
            case "leg_answer" -> {
                agentCall(leg).answer(a.has("sdp") ? a.get("sdp").asString() : TestSipAgent.sdp(port(), 1, "0"));
                awaitReported(leg, LegState.ANSWERED);
            }
            case "leg_fail" -> {
                agentCall(leg).fail(a.path("sip_status").asInt(480));
                awaitReported(leg, LegState.ENDED);
            }
            case "leg_hangup" -> {
                TestSipAgent.Call c = carrierCalls.containsKey(leg) ? agent.call(carrierCalls.get(leg)) : agentCall(leg);
                c.bye();
                awaitReported(leg, LegState.ENDED);
            }
            case "caller_cancel" -> {
                agent.call(carrierCalls.get(leg)).cancel();
                awaitReported(leg, LegState.ENDED);
            }
            case "reinvite_address", "reinvite_codec" -> {
                TestSipAgent.Call c = agent.call(carrierCalls.get(leg));
                int before = node.mediaReports(leg);
                int v = sdpVersions.merge(leg, 1, Integer::sum) + 1;
                c.reinvite(what.equals("reinvite_address") ? TestSipAgent.sdp(port(), v, "0")
                        : TestSipAgent.sdp(port(), v, "8"));
                await(() -> node.mediaReports(leg) > before, 5_000, "TC never reported the re-INVITE");
            }
            case "freeswitch_unreachable" -> node.setPartitioned(true);
            case "freeswitch_reachable" -> node.setPartitioned(false);
            case "freeswitch_lost" -> {
                String core = node.esl().coreUuid();
                freeswitch.killAndRestart();
                await(() -> node.live() && node.esl().coreUuid() != null && !node.esl().coreUuid().equals(core), 60_000,
                        "freeswitch never came back");
                Thread.sleep(300);
            }
            case "parity_ms_unreachable" -> simulated().setUp(false);
            case "parity_ms_reachable" -> simulated().setUp(true);
            case "parity_ms_lost" -> {
                simulated().lose();
                node.mediaLost();
            }
            case "capacity" -> node.setFreeLegs(a.get("free_legs").asInt());
            // Step 4: what parity-ms (simulated here) reports, and the far end's hold by re-INVITE.
            case "play_done" -> simulated().played(key(leg).getKey(), key(leg).getValue(), a.get("reason").asString(),
                    a.path("played_ms").asLong(0));
            case "prompt_missing" -> simulated().promptMissing(a.get("prompt_id").asString());
            case "dtmf" -> simulated().digit(key(leg).getKey(), key(leg).getValue(), a.get("digit").asString(),
                    ScriptHooks.source(a));
            case "media_inactive", "media_active" -> simulated().activity(key(leg).getKey(), key(leg).getValue(),
                    what.equals("media_active"));
            case "dtmf_action_match" -> simulated().dtmfActionMatch(key(leg).getKey(), key(leg).getValue(),
                    a.get("action").asString().substring("DTMF_ACTION_KIND_".length()),
                    a.get("result").asString().substring("DTMF_ACTION_RESULT_".length()));
            case "reinvite_hold" -> {
                TestSipAgent.Call c = carrierCalls.containsKey(leg) ? agent.call(carrierCalls.get(leg)) : agentCall(leg);
                int before = node.remoteHolds(leg);
                int v = sdpVersions.merge(leg, 1, Integer::sum) + 1;
                // The same address and codec, a new o= version: only the direction changes.
                String sdp = c.localSdp().replaceFirst("o=test 1000 \\d+", "o=test 1000 " + v)
                        .replace("a=sendonly", "a=sendrecv");
                c.reinvite(a.get("on").asBoolean() ? sdp.replace("a=sendrecv", "a=sendonly") : sdp);
                await(() -> node.remoteHolds(leg) > before, 5_000, "TC never reported the hold");
            }
            default -> {
                return hooks.inject(what, a);
            }
        }
        return 0;
    }

    private void arrival(JsonNode a) throws Exception {
        String leg = a.get("leg_id").asString();
        String callId = a.get("sip_call_id").asString();
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("X-Interaction-ID", a.get("interaction_id").asString());
        headers.put("X-Tenant-ID", a.get("tenant_id").asString());
        headers.put("X-CC-Bin", a.get("bin").asString());
        if (a.has("trunk_id")) {
            headers.put("X-CC-Trunk", a.get("trunk_id").asString());
        }
        headers.put("X-Test-Leg-ID", leg);
        for (JsonNode h : a.path("headers")) {
            headers.put(h.get("name").asString(), h.get("value").asString());
        }
        String caller = a.has("caller_number") ? a.get("caller_number").asString() : "anonymous";
        sdpVersions.put(leg, 1);
        agent.invite("127.0.0.1:5080", a.get("called_number").asString(), caller, callId, headers,
                a.has("sdp") ? a.get("sdp").asString() : TestSipAgent.sdp(port(), 1, "0"));
        carrierCalls.put(leg, callId);
        awaitReported(leg, LegState.PARKED);
        // The engine accepts it in the session's turn (the token, the hook); give that a moment.
        Thread.sleep(150);
    }

    private java.util.Map.Entry<String, parity.telephonycontroller.node.MediaPort.Side> key(String leg) {
        java.util.Map.Entry<String, parity.telephonycontroller.node.MediaPort.Side> k = node.mediaKey(leg);
        if (k == null) {
            throw new AssertionError("no such leg on the node: " + leg);
        }
        return k;
    }

    private SimulatedMedia simulated() {
        if (sim == null) {
            throw new UnsupportedOperationException("this target runs with parity-ms");
        }
        return sim;
    }

    private TestSipAgent.Call agentCall(String leg) throws InterruptedException {
        TestSipAgent.Call c = agent.awaitIncoming(m -> leg.equals(m.h("x-test-leg-id")), Duration.ofSeconds(5));
        if (c == null) {
            throw new AssertionError("no INVITE reached the agent for leg " + leg);
        }
        return c;
    }

    private void awaitReported(String leg, LegState state) throws InterruptedException {
        await(() -> state == node.reported(leg) || (state != LegState.ENDED && node.reported(leg) == LegState.ENDED),
                5_000, "TC never reported " + state + " for leg " + leg);
    }

    private int port() {
        nextPort += 2;
        return nextPort;
    }

    private static void await(BooleanSupplier c, long ms, String what) throws InterruptedException {
        long until = System.currentTimeMillis() + ms;
        while (System.currentTimeMillis() < until) {
            if (c.getAsBoolean()) {
                return;
            }
            Thread.sleep(20);
        }
        if (!c.getAsBoolean()) {
            throw new AssertionError(what);
        }
    }

    @Override
    public void advance(long ms) {
        hooks.advance(ms);
    }

    @Override
    public List<String> invariants() {
        List<String> out = new ArrayList<>(hooks.invariants());
        if (node.canaryPortsOpen() > 0) {
            out.add("freeswitch bound RTP ports (the canary)");
        }
        return out;
    }

    @Override
    public ManagedChannel channel() {
        return channel;
    }

    @Override
    public String name() {
        return "real-node";
    }

    @Override
    public void close() {
        channel.shutdownNow();
        server.close();
        TelephonyNode t = holder.get();
        if (t != null) {
            t.close();
        }
        agent.hangUpAll();
        agent.close();
        node.close();
        journal.close();
        freeswitch.close();
        etcd.stop();
        parity.telephonycontroller.realnode.TempDirs.delete(journalDir);
    }
}
