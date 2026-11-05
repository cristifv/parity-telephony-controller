package parity.telephonycontroller.scripts;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import parity.contracts.telephony.v1.Endpoint;
import parity.contracts.telephony.v1.LegState;
import parity.telephonycontroller.node.ParityMsClient;
import parity.telephonycontroller.realnode.BrowserAgent;
import parity.telephonycontroller.realnode.ParityMsProcess;
import parity.telephonycontroller.realnode.RtpPeer;
import parity.telephonycontroller.realnode.TestSipAgent;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Step 3's browser evidence (TC ADR 0002 §10, done-when), one run: an inbound call answered by an
 * agent in a browser (WebRTC through parity-ms, freeswitch in bypass), held past two minutes (the
 * old 120 s session timer would have ended it), with audio both ways: the customer's 440 Hz tone
 * heard by the browser, the browser's 880 Hz heard by the customer. Run with
 * {@code -Dtc.evidence=true}; results in {@code target/tc-evidence/real-browser.txt}.
 */
@EnabledIfSystemProperty(named = "tc.evidence", matches = "true")
class RealNodeBrowserIT {

    private static ParityMsProcess parityMs;
    private static ParityMsClient media;
    private static RealNodeTarget target;
    private static LiveCalls calls;
    private static final Map<String, String> REPORT = new LinkedHashMap<>();
    private static final JsonMapper JSON = JsonMapper.builder().build();

    @BeforeAll
    static void start() throws Exception {
        parityMs = ParityMsProcess.start();
        media = new ParityMsClient(new ParityMsClient.Config("127.0.0.1", ParityMsProcess.CONTROL_PORT, Duration.ofMillis(500), 4));
        media.start();
        target = new RealNodeTarget(media, "127.0.0.1");
        calls = new LiveCalls(target);
    }

    @AfterAll
    static void stop() throws IOException {
        Path out = Path.of("target/tc-evidence");
        Files.createDirectories(out);
        StringBuilder b = new StringBuilder("# TC step 3: the browser agent (one run)\n");
        REPORT.forEach((k, v) -> b.append(k).append(": ").append(v).append('\n'));
        Files.writeString(out.resolve("real-browser.txt"), b.toString(), StandardCharsets.UTF_8);
        System.out.print(b);
        if (calls != null) {
            calls.close();
        }
        if (target != null) {
            target.close();
        }
        if (media != null) {
            media.close();
        }
        if (parityMs != null) {
            parityMs.close();
        }
    }

    @Test
    void browserAgentPastTwoMinutes() throws Exception {
        calls.begin();
        try (BrowserAgent browser = BrowserAgent.start("agent1-test", 400_000);
                RtpPeer customer = new RtpPeer(0, (byte) 0x61, 440)) {
            LiveCalls.await(browser::registered, 60_000, "the browser never registered");
            LiveCalls.Inbound c = calls.arrive(customer.sdp(1));
            calls.answer(c);
            TestSipAgent.Call carrier = target.agent().call(c.sipCallId);
            LiveCalls.await(() -> carrier.finalStatus == 200, 5_000, "the carrier got no 200");
            customer.sendTo(RealNodeMediaIT.rtpAddress(carrier.remoteSdp));
            String leg = calls.addAgent(c, "sip:agent1@127.0.0.1", Endpoint.ENDPOINT_WEBRTC);
            calls.awaitState(leg, LegState.LEG_STATE_ANSWERED, 30_000);
            long t0 = System.currentTimeMillis();
            LiveCalls.sleep(Duration.ofSeconds(10));
            REPORT.put("customer: packets from the browser after 10 s", Long.toString(customer.receivedTotal()));
            REPORT.put("customer: 880 Hz ratio after 10 s", ratio(customer.toneRatio(880, 440, 660, 1000)));
            REPORT.put("freeswitch UDP sockets (local ports)", RealNodeMediaIT.freeswitchUdpPorts(target.freeswitch()).toString());
            REPORT.put("freeswitch channels with the call up", target.node().esl().api("show channels count").trim());
            LiveCalls.sleep(Duration.ofSeconds(125));
            long heldS = (System.currentTimeMillis() - t0) / 1000;
            long before = customer.receivedTotal();
            LiveCalls.sleep(Duration.ofSeconds(2));
            long lastTwo = customer.receivedTotal() - before;
            REPORT.put("call held (s)", Long.toString(heldS));
            REPORT.put("customer: packets from the browser in the last 2 s", Long.toString(lastTwo));
            REPORT.put("customer: 880 Hz ratio at the end", ratio(customer.toneRatio(880, 440, 660, 1000)));
            REPORT.put("agent leg state at the end", calls.state(leg).name());
            REPORT.put("canary", Long.toString(target.node().canaryPortsOpen()));
            assertEquals(LegState.LEG_STATE_ANSWERED, calls.state(leg), "the browser leg ended before two minutes");
            assertTrue(lastTwo > 80, "the browser's audio stopped");
            assertTrue(customer.toneRatio(880, 440, 660, 1000) > 10, "the customer doesn't hear the browser's tone");

            carrier.bye();
            LiveCalls.await(() -> {
                try {
                    return browser.exited();
                } catch (IOException e) {
                    return false;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }, 30_000, "the browser never saw the hangup");
            Path result = browser.out().resolve("browser-result.json");
            assertTrue(Files.exists(result), "the browser wrote no result: " + browser.logs());
            JsonNode meta = JSON.readTree(Files.readString(result));
            JsonNode in = meta.path("last_stats").path("inbound").path(0);
            REPORT.put("browser: packets received / lost / codec", in.path("packetsReceived").asLong() + " / "
                    + in.path("packetsLost").asLong() + " / " + in.path("codec").asString(""));
            REPORT.put("browser: DTLS / SRTP / selected pair", meta.path("last_stats").path("dtlsState").asString("") + " / "
                    + meta.path("last_stats").path("srtpCipher").asString("") + " / "
                    + meta.path("last_stats").path("remote").path("port").asLong());
            double heard = wavToneRatio(browser.out().resolve("browser-recv.wav"), 440, 880, 660, 1000);
            REPORT.put("browser: 440 Hz ratio in what it heard (middle 10 s)", ratio(heard));
            String jsonl = Files.readString(browser.out().resolve("browser.jsonl"));
            REPORT.put("browser: calls received / WS disconnects", count(jsonl, "\"call_received\"") + " / "
                    + count(jsonl, "\"ws_disconnected\""));
            int sessionExpiresAt = jsonl.indexOf("\"session_expires\":");
            REPORT.put("browser: Session-Expires on the INVITE", sessionExpiresAt < 0 ? "?"
                    : jsonl.substring(sessionExpiresAt + 18, Math.min(jsonl.length(), sessionExpiresAt + 30)).split(",", -1)[0]);
            assertTrue(in.path("packetsReceived").asLong() > 6_000, "the browser received too little audio");
            assertTrue(heard > 10, "the browser doesn't hear the customer's tone");
        }
    }

    private static int count(String s, String what) {
        int n = 0;
        for (int i = s.indexOf(what); i >= 0; i = s.indexOf(what, i + 1)) {
            n++;
        }
        return n;
    }

    private static String ratio(double r) {
        return String.format(java.util.Locale.ROOT, "%.1f", r);
    }

    /** Goertzel power at {@code hz} over the mean at {@code others}, in 10 s from the middle of a 16-bit mono WAV. */
    static double wavToneRatio(Path wav, int hz, int... others) throws IOException {
        byte[] b = Files.readAllBytes(wav);
        ByteBuffer bb = ByteBuffer.wrap(b).order(ByteOrder.LITTLE_ENDIAN);
        int rate = bb.getInt(24);
        int n = (b.length - 44) / 2;
        int len = Math.min(n, rate * 10);
        int from = Math.max(0, n / 2 - len / 2);
        double want = goertzel(bb, from, len, rate, hz);
        double rest = 0;
        for (int o : others) {
            rest += goertzel(bb, from, len, rate, o);
        }
        return want / Math.max(1e-9, rest / others.length);
    }

    private static double goertzel(ByteBuffer bb, int from, int len, int rate, int hz) {
        double c = 2 * Math.cos(2 * Math.PI * hz / rate);
        double s1 = 0;
        double s2 = 0;
        for (int i = 0; i < len; i++) {
            double s0 = bb.getShort(44 + 2 * (from + i)) + c * s1 - s2;
            s2 = s1;
            s1 = s0;
        }
        return s1 * s1 + s2 * s2 - c * s1 * s2;
    }
}
