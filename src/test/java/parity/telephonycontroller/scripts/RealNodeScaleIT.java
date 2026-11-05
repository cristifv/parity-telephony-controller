package parity.telephonycontroller.scripts;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import parity.contracts.telephony.v1.Endpoint;
import parity.contracts.telephony.v1.LegSnapshot;
import parity.contracts.telephony.v1.LegState;
import parity.contracts.telephony.v1.Session;
import parity.telephonycontroller.esl.EslClient;
import parity.telephonycontroller.model.UuidV7;
import parity.telephonycontroller.realnode.Freeswitch;
import parity.telephonycontroller.realnode.TestSipAgent;

/**
 * Step 3's scale evidence (TC ADR 0002 §10), one run each, media simulated: a TC process restart
 * rebuilding 100 live agent calls from freeswitch and the journal, and G-3, what keeping each
 * originated leg's loopback pair would cost freeswitch against tearing it down. Run with
 * {@code -Dtc.evidence=true}; results in {@code target/tc-evidence/real-scale.txt}.
 */
@EnabledIfSystemProperty(named = "tc.evidence", matches = "true")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RealNodeScaleIT {

    private static RealNodeTarget target;
    private static LiveCalls calls;
    private static final Map<String, String> REPORT = new LinkedHashMap<>();
    private static final int PARALLEL = 12;
    /** Calls {@link #connect} couldn't set up (a missing INVITE, a leg that never answered). */
    private static final java.util.concurrent.atomic.AtomicInteger FAILED = new java.util.concurrent.atomic.AtomicInteger();

    @BeforeAll
    static void start() throws Exception {
        target = new RealNodeTarget();
        calls = new LiveCalls(target);
    }

    @AfterAll
    static void stop() throws IOException {
        Path out = Path.of("target/tc-evidence");
        Files.createDirectories(out);
        StringBuilder b = new StringBuilder("# TC step 3: rebuild and G-3 (one run)\n");
        REPORT.forEach((k, v) -> b.append(k).append(": ").append(v).append('\n'));
        Files.writeString(out.resolve("real-scale.txt"), b.toString(), StandardCharsets.UTF_8);
        System.out.print(b);
        if (calls != null) {
            calls.close();
        }
        if (target != null) {
            target.close();
        }
    }

    /** n agent calls (customer + desk phone, answered), {@link #PARALLEL} at a time. */
    private static List<LiveCalls.Inbound> connect(int n) throws Exception {
        ConcurrentLinkedQueue<LiveCalls.Inbound> done = new ConcurrentLinkedQueue<>();
        Semaphore slots = new Semaphore(PARALLEL);
        List<Future<?>> all = new ArrayList<>();
        try (ExecutorService ex = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < n; i++) {
                slots.acquire();
                int port = 30_000 + 2 * i;
                all.add(ex.submit(() -> {
                    try {
                        LiveCalls.Inbound c = calls.arrive(TestSipAgent.sdp(port, 1, "0"));
                        calls.answer(c);
                        String leg = calls.addAgent(c, "sip:agent1@acme.agents.example", Endpoint.ENDPOINT_SIP);
                        TestSipAgent.Call a = target.agent().awaitIncoming(m -> leg.equals(m.h("x-test-leg-id")),
                                Duration.ofSeconds(20));
                        if (a == null) {
                            throw new AssertionError("no INVITE reached the agent");
                        }
                        a.answer(TestSipAgent.sdp(port + 10_000, 1, "0"));
                        calls.awaitState(leg, LegState.LEG_STATE_ANSWERED, 20_000);
                        done.add(c);
                        return null;
                    } catch (AssertionError e) {
                        FAILED.incrementAndGet();
                        return null;
                    } finally {
                        slots.release();
                    }
                }));
            }
            for (Future<?> f : all) {
                f.get();
            }
        }
        return new ArrayList<>(done);
    }

    /** Every call gone from freeswitch (hupall), and TC's view reset. */
    private static void clear() throws Exception {
        target.agent().hangUpAll();
        EslClient esl = target.node().esl();
        esl.api("hupall NORMAL_CLEARING cc_managed true");
        LiveCalls.await(() -> {
            try {
                return esl.api("show channels count").contains("0 total");
            } catch (EslClient.Unavailable | EslClient.Timeout e) {
                return false;
            }
        }, 120_000, "freeswitch kept channels");
    }

    @Test
    @Order(1)
    void restartRebuildsOneHundredCalls() throws Exception {
        calls.begin();
        long t0 = System.nanoTime();
        List<LiveCalls.Inbound> live = connect(100);
        assertEquals(100, live.size(), "not every call was set up");
        REPORT.put("rebuild: 100 calls set up in (ms)", Long.toString((System.nanoTime() - t0) / 1_000_000));
        // The restart comes at once: the last calls' loopback pairs may not be torn down yet.
        String before = target.node().esl().api("show channels count").trim();
        REPORT.put("rebuild: freeswitch channels at the restart", before);
        REPORT.put("rebuild: legs ended before the restart (role/cause/by/status)", endedLegs());

        long rebuildMs = target.restartProcess(LiveCalls.config(UuidV7.create()));
        calls.rewatch();
        REPORT.put("rebuild: new ESL connection to engine ready (ms)", Long.toString(rebuildMs));
        REPORT.put("rebuild: legs the new node adopted at once", Integer.toString(target.node().channels().size()));
        // freeswitch's listing lags its channels: the node looks again at 1 s and 3 s (R1-14).
        Thread.sleep(4_000);
        REPORT.put("rebuild: legs the node holds 4 s later", Integer.toString(target.node().channels().size()));
        int whole = 0;
        List<String> broken = new ArrayList<>();
        for (LiveCalls.Inbound c : live) {
            Session s = calls.session(c);
            long answered = s.getLegsList().stream().map(LegSnapshot::getState)
                    .filter(st -> st == LegState.LEG_STATE_ANSWERED).count();
            if (answered == 2 && s.getOwnerEpoch() == c.epoch) {
                whole++;
            } else {
                broken.add(s.getState() + " epoch " + s.getOwnerEpoch() + "/" + c.epoch + " legs "
                        + s.getLegsList().stream().map(l -> l.getRole() + ":" + l.getState()).toList());
            }
        }
        REPORT.put("rebuild: sessions back with both legs answered and their fence", whole + " / 100");
        REPORT.put("rebuild: the others", broken.toString());
        String after = target.node().esl().api("show channels count").trim();
        REPORT.put("rebuild: freeswitch channels 4 s after (a loopback pair TC died before tearing down stays)", after);
        REPORT.put("rebuild: legs ended after the restart (role/cause/by/status)", endedLegs());
        assertEquals(100, whole);
        assertTrue(Integer.parseInt(after.split(" ", -1)[0]) >= 200, "a call died in the restart");

        // Still in control: one customer hangs up and the hook ends its agent.
        LiveCalls.Inbound first = live.get(0);
        TestSipAgent.Call carrier = target.agent().call(first.sipCallId);
        carrier.bye();
        LiveCalls.await(() -> calls.session(first).getLegsList().stream()
                .allMatch(l -> l.getState() == LegState.LEG_STATE_ENDED), 10_000, "the session didn't end after the restart");
        REPORT.put("rebuild: a hangup after the restart ended its session", "yes");
        clear();
    }

    /** The ENDED leg updates on the stream so far, counted by role, cause, who and SIP status. */
    private static String endedLegs() {
        Map<String, Integer> n = new java.util.TreeMap<>();
        for (parity.contracts.telephony.v1.WatchResponse r : calls.events()) {
            if (r.hasLeg() && r.getLeg().getState() == LegState.LEG_STATE_ENDED) {
                n.merge(r.getLeg().getRole() + "/" + r.getLeg().getCause() + "/" + r.getLeg().getEndedBy() + "/"
                        + r.getLeg().getSipStatus(), 1, Integer::sum);
            }
        }
        return n.toString();
    }

    /** freeswitch's threads, resident memory and CPU over 10 s, from the host's /proc. */
    private static String sample() throws IOException, InterruptedException {
        String pid = Freeswitch.pid();
        long[] c0 = cpuTicks(pid);
        Thread.sleep(10_000);
        long[] c1 = cpuTicks(pid);
        Map<String, String> st = new LinkedHashMap<>();
        for (String line : Files.readAllLines(Path.of("/proc/" + pid + "/status"))) {
            if (line.startsWith("Threads:") || line.startsWith("VmRSS:")) {
                String[] f = line.split("\\s+", -1);
                st.put(f[0], f[1]);
            }
        }
        double cpuPct = (c1[0] - c0[0]) / 10.0; // 100 ticks a second: ticks per 10 s / 10 = % of one core
        return "threads " + st.get("Threads:") + ", RSS " + Long.parseLong(st.get("VmRSS:")) / 1024 + " MiB, CPU "
                + String.format(java.util.Locale.ROOT, "%.1f", cpuPct) + "% of a core";
    }

    private static long[] cpuTicks(String pid) throws IOException {
        String stat = Files.readString(Path.of("/proc/" + pid + "/stat"));
        String[] f = stat.substring(stat.lastIndexOf(')') + 2).split(" ", -1);
        return new long[] {Long.parseLong(f[11]) + Long.parseLong(f[12])};
    }

    @Test
    @Order(2)
    void loopbackCostG3() throws Exception {
        for (boolean keep : new boolean[] {false, true}) {
            calls.begin();
            target.node().setKeepLoopback(keep);
            String mode = keep ? "kept" : "torn down";
            Thread.sleep(3_000);
            REPORT.put("G-3 idle (" + mode + " run)", sample());
            int total = 0;
            for (int n : new int[] {250, 500}) {
                long t0 = System.nanoTime();
                connect(n - total);
                total = n;
                long setupMs = (System.nanoTime() - t0) / 1_000_000;
                Thread.sleep(5_000);
                String channels = target.node().esl().api("show channels count").trim();
                REPORT.put("G-3 " + n + " agent calls, loopback " + mode, sample() + "; " + channels + "; set up in "
                        + setupMs + " ms; failed set-ups " + FAILED.getAndSet(0) + "; legs ended " + endedLegs());
                // Recorded, not asserted: G-3 is a measurement (one run).
            }
            clear();
            target.node().setKeepLoopback(false);
        }
    }
}
