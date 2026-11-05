package parity.telephonycontroller.scripts;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import parity.contracts.telephony.v1.Endpoint;
import parity.contracts.telephony.v1.LegState;
import parity.telephonycontroller.esl.EslClient;
import parity.telephonycontroller.node.ParityMsClient;
import parity.telephonycontroller.realnode.ParityMsProcess;
import parity.telephonycontroller.realnode.RtpPeer;
import parity.telephonycontroller.realnode.TestSipAgent;

/**
 * Step 3's media and fault evidence (TC ADR 0002 §10, done-when), one run each: real freeswitch,
 * real parity-ms (its control API as built, through {@link ParityMsClient}), the SIP agent with RTP at
 * both ends. Run with {@code -Dtc.evidence=true}; the results go to
 * {@code target/tc-evidence/real-media.txt}.
 *
 * <ol>
 *   <li>A desk-phone agent call: audio both ways through parity-ms; the loopback pair torn down;
 *       no re-INVITE to the agent; freeswitch's UDP sockets are SIP only; the canary stays 0; the
 *       call outlives half the session timeout on freeswitch's UPDATE refresh; the customer's
 *       re-INVITE to a new port moves the media (R2-6); the customer's BYE ends the agent (the hook).
 *   <li>ESL faults on a live call: a 3 s {@code SIGSTOP} (no reconnect, the call lives, commands
 *       fail fast), and a dropped ESL ({@code reload mod_event_socket}: one reconnect, the call lives).
 * </ol>
 */
@EnabledIfSystemProperty(named = "tc.evidence", matches = "true")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RealNodeMediaIT {

    private static ParityMsProcess parityMs;
    private static ParityMsClient media;
    private static RealNodeTarget target;
    private static LiveCalls calls;
    private static final Map<String, String> REPORT = new LinkedHashMap<>();

    @BeforeAll
    static void start() throws Exception {
        parityMs = ParityMsProcess.start();
        media = client();
        media.start();
        target = new RealNodeTarget(media, null);
        calls = new LiveCalls(target);
    }

    @AfterAll
    static void stop() throws IOException {
        Path out = Path.of("target/tc-evidence");
        Files.createDirectories(out);
        StringBuilder b = new StringBuilder("# TC step 3: real media and ESL faults (one run)\n");
        REPORT.forEach((k, v) -> b.append(k).append(": ").append(v).append('\n'));
        Files.writeString(out.resolve("real-media.txt"), b.toString(), StandardCharsets.UTF_8);
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

    /** A 5 s sweep grace (production: minutes), so the sweep runs within these tests; the null recording sink. */
    private static ParityMsClient client() {
        return new ParityMsClient(new ParityMsClient.Config("127.0.0.1", ParityMsProcess.CONTROL_PORT, Duration.ofMillis(500), 4,
                Duration.ofSeconds(5), true));
    }

    /** A customer and a desk-phone agent, talking through parity-ms. */
    private record Live(LiveCalls.Inbound call, TestSipAgent.Call carrier, TestSipAgent.Call agent, String agentLeg,
            RtpPeer customerRtp, RtpPeer agentRtp) {}

    private static Live connect(byte customerMark, byte agentMark) throws Exception {
        RtpPeer customer = new RtpPeer(0, customerMark);
        LiveCalls.Inbound c = calls.arrive(customer.sdp(1));
        calls.answer(c);
        TestSipAgent.Call carrier = target.agent().call(c.sipCallId);
        LiveCalls.await(() -> carrier.finalStatus == 200 && carrier.remoteSdp != null, 5_000, "the carrier got no 200");
        customer.sendTo(rtpAddress(carrier.remoteSdp));
        return addDeskPhone(c, carrier, customer, agentMark);
    }

    /** A desk-phone agent joins the customer's call (parity-ms's leg B), answered with RTP of its own. */
    private static Live addDeskPhone(LiveCalls.Inbound c, TestSipAgent.Call carrier, RtpPeer customer, byte agentMark)
            throws Exception {
        RtpPeer agentRtp = new RtpPeer(0, agentMark);
        String agentLeg = calls.addAgent(c, "sip:agent1@acme.agents.example", Endpoint.ENDPOINT_SIP);
        TestSipAgent.Call agent = target.agent().awaitIncoming(m -> agentLeg.equals(m.h("x-test-leg-id")),
                Duration.ofSeconds(10));
        assertTrue(agent != null, "no INVITE reached the desk phone");
        agentRtp.sendTo(rtpAddress(agent.remoteSdp));
        agent.ring();
        Map<String, String> timer = new LinkedHashMap<>();
        String se = agent.invite.h("session-expires");
        if (se != null) {
            timer.put("Session-Expires", se.contains("refresher") ? se : se + ";refresher=uac");
            timer.put("Require", "timer");
        }
        agent.answer(agentRtp.sdp(1), timer);
        calls.awaitState(agentLeg, LegState.LEG_STATE_ANSWERED, 10_000);
        return new Live(c, carrier, agent, agentLeg, customer, agentRtp);
    }

    @Test
    @Order(1)
    void deskPhoneCallThroughParityMs() throws Exception {
        calls.begin();
        Live l = connect((byte) 0x11, (byte) 0x22);
        REPORT.put("desk-phone INVITE Session-Expires", String.valueOf(l.agent().invite.h("session-expires")));
        LiveCalls.sleep(Duration.ofSeconds(5));
        int toAgent = l.agentRtp().receivedWith((byte) 0x11);
        int toCustomer = l.customerRtp().receivedWith((byte) 0x22);
        REPORT.put("media after 5 s (frames customer->agent / agent->customer)", toAgent + " / " + toCustomer);
        assertTrue(toAgent > 150 && toCustomer > 150, "audio didn't flow both ways through parity-ms");

        // The loopback pair is gone: freeswitch holds the customer and the agent only.
        String count = target.node().esl().api("show channels count").trim();
        REPORT.put("freeswitch channels with the call up", count);
        assertTrue(count.contains("2 total"), "the loopback pair is still up");
        List<String> ports = freeswitchUdpPorts(target.freeswitch());
        REPORT.put("freeswitch UDP sockets (local ports)", ports.toString());
        assertTrue(ports.stream().allMatch(p -> List.of("5080", "5090", "5092").contains(p)),
                "freeswitch holds a non-SIP UDP socket");

        // Past half the desk phone's session timeout (90 s): freeswitch refreshes by UPDATE, and
        // nothing re-INVITEs the agent.
        long t0 = System.currentTimeMillis();
        LiveCalls.sleep(Duration.ofSeconds(65));
        REPORT.put("call held (s)", Long.toString((System.currentTimeMillis() - t0) / 1000 + 5));
        REPORT.put("agent dialog log (SIP methods only)", l.agent().log.toString());
        REPORT.put("carrier dialog log (SIP methods only)", l.carrier().log.toString());
        assertEquals(1, l.agent().count("INVITE"), "the agent was re-INVITEd");
        assertTrue(l.agent().count("UPDATE") >= 1, "freeswitch never refreshed the desk phone's session by UPDATE");
        assertEquals(LegState.LEG_STATE_ANSWERED, calls.state(l.agentLeg()));
        int toAgent2 = l.agentRtp().receivedWith((byte) 0x11);
        LiveCalls.sleep(Duration.ofSeconds(2));
        assertTrue(l.agentRtp().receivedWith((byte) 0x11) - toAgent2 > 60, "audio stopped during the hold");
        REPORT.put("canary (RTP ports freeswitch bound in 61000-61099)", Long.toString(target.node().canaryPortsOpen()));
        assertEquals(0, target.node().canaryPortsOpen());

        // R2-6: the customer moves to a new port; parity-ms follows TC's update.
        RtpPeer moved = new RtpPeer(0, (byte) 0x33);
        moved.sendTo(rtpAddress(l.carrier().remoteSdp));
        l.customerRtp().sendTo(null);
        int before = target.node().mediaReports(l.call().customerLeg);
        l.carrier().reinvite(moved.sdp(2));
        LiveCalls.await(() -> target.node().mediaReports(l.call().customerLeg) > before, 5_000, "TC never saw the re-INVITE");
        LiveCalls.sleep(Duration.ofSeconds(3));
        int movedIn = moved.receivedWith((byte) 0x22);
        int agentFromMoved = l.agentRtp().receivedWith((byte) 0x33);
        REPORT.put("after the customer's move (frames to the new port / agent from the new port)",
                movedIn + " / " + agentFromMoved);
        assertTrue(movedIn > 60 && agentFromMoved > 60, "the media didn't follow the customer's re-INVITE");

        // R1-6, after the teardown: the desk phone's own re-INVITE to a new port is answered by
        // (c+) and reaches TC under the agent's leg id (the hook names it literally), so parity-ms
        // follows the agent too.
        RtpPeer agentMoved = new RtpPeer(0, (byte) 0x44);
        agentMoved.sendTo(rtpAddress(l.agent().remoteSdp));
        l.agentRtp().sendTo(null);
        int agentBefore = target.node().mediaReports(l.agentLeg());
        l.agent().reinvite(agentMoved.sdp(2));
        LiveCalls.await(() -> target.node().mediaReports(l.agentLeg()) > agentBefore, 5_000,
                "TC never saw the agent's re-INVITE under its leg id");
        LiveCalls.sleep(Duration.ofSeconds(3));
        int agentMovedIn = agentMoved.receivedWith((byte) 0x33);
        int customerFromAgentMoved = moved.receivedWith((byte) 0x44);
        REPORT.put("after the agent's own re-INVITE (frames to its new port / customer from it / 200 OK)",
                agentMovedIn + " / " + customerFromAgentMoved + " / " + l.agent().received("200 INVITE"));
        assertTrue(l.agent().received("200 INVITE"), "(c+) didn't answer the agent's re-INVITE");
        assertTrue(agentMovedIn > 60 && customerFromAgentMoved > 60, "the media didn't follow the agent's re-INVITE");

        // Step 4: the agent leaves; parity-ms's leg B is detached, and a second agent is attached
        // to the same call (a fresh B), the customer's answer unchanged.
        l.agent().bye();
        calls.awaitState(l.agentLeg(), LegState.LEG_STATE_ENDED, 5_000);
        agentMoved.sendTo(null);
        Live second = addDeskPhone(l.call(), l.carrier(), moved, (byte) 0x55);
        LiveCalls.sleep(Duration.ofSeconds(3));
        int toSecond = second.agentRtp().receivedWith((byte) 0x33);
        int fromSecond = moved.receivedWith((byte) 0x55);
        REPORT.put("a second agent after the first left (frames to it / from it; carrier re-INVITEd?)",
                toSecond + " / " + fromSecond + "; " + (l.carrier().count("INVITE") > 0));
        assertTrue(toSecond > 60 && fromSecond > 60, "the second agent's audio didn't flow");

        // The customer hangs up: the hook ends the agent, and parity-ms's call goes.
        l.carrier().bye();
        LiveCalls.await(() -> second.agent().received("BYE"), 5_000, "the agent leg outlived the customer");
        REPORT.put("customer BYE -> agent BYE", "yes");
        LiveCalls.await(() -> !parityMs.has(l.call().interactionId), 5_000, "parity-ms kept the call");
        REPORT.put("parity-ms holds the call after the hangup", "no");
        second.agentRtp().close();
        agentMoved.close();
        moved.close();
        l.customerRtp().close();
        l.agentRtp().close();
    }

    @Test
    @Order(2)
    void eslStallOfThreeSeconds() throws Exception {
        calls.begin();
        Live l = connect((byte) 0x41, (byte) 0x42);
        EslClient esl = target.node().esl();
        long reconnects = esl.reconnects();
        List<String> during = new ArrayList<>();
        Thread stall = Thread.ofVirtual().start(() -> {
            try {
                target.freeswitch().stall(3_000);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        Thread.sleep(300);
        long until = System.currentTimeMillis() + 2_400;
        while (System.currentTimeMillis() < until) {
            long t = System.nanoTime();
            String outcome;
            try {
                esl.api("status");
                outcome = "ok";
            } catch (EslClient.Timeout e) {
                outcome = "timeout";
            } catch (EslClient.Unavailable e) {
                outcome = "unavailable";
            }
            during.add(outcome + " " + (System.nanoTime() - t) / 1_000_000 + "ms");
        }
        stall.join();
        LiveCalls.sleep(Duration.ofSeconds(8));
        REPORT.put("stall: commands during the 3 s stop (outcome, latency)", summarize(during));
        REPORT.put("stall: reconnects", Long.toString(esl.reconnects() - reconnects));
        REPORT.put("stall: late replies counted", Long.toString(esl.lateReplies()));
        assertEquals(reconnects, esl.reconnects(), "a 3 s stall made TC reconnect");
        assertEquals(LegState.LEG_STATE_ANSWERED, calls.state(l.agentLeg()));
        assertFalse(l.agent().received("BYE"), "the call died in the stall");
        assertTrue(esl.api("show channels count").contains("2 total"));
        l.carrier().bye();
        LiveCalls.await(() -> l.agent().received("BYE"), 5_000, "the agent leg outlived the customer");
        LiveCalls.await(() -> !parityMs.has(l.call().interactionId), 5_000, "parity-ms kept the call after the stall");
        l.customerRtp().close();
        l.agentRtp().close();
    }

    @Test
    @Order(3)
    void eslConnectionDropped() throws Exception {
        calls.begin();
        Live l = connect((byte) 0x51, (byte) 0x52);
        EslClient esl = target.node().esl();
        long reconnects = esl.reconnects();
        String core = esl.coreUuid();
        long t0 = System.nanoTime();
        esl.api("bgapi reload mod_event_socket");
        LiveCalls.await(() -> esl.reconnects() > reconnects && esl.live(), 30_000, "ESL never came back");
        long back = (System.nanoTime() - t0) / 1_000_000;
        LiveCalls.sleep(Duration.ofSeconds(3));
        REPORT.put("drop: reconnects / back after (ms) / same Core-UUID", (esl.reconnects() - reconnects) + " / " + back
                + " / " + core.equals(esl.coreUuid()));
        assertEquals(LegState.LEG_STATE_ANSWERED, calls.state(l.agentLeg()));
        assertFalse(l.agent().received("BYE"), "the call died with the ESL connection");
        int toAgent = l.agentRtp().receivedWith((byte) 0x51);
        LiveCalls.sleep(Duration.ofSeconds(1));
        assertTrue(l.agentRtp().receivedWith((byte) 0x51) > toAgent, "audio stopped");
        // TC still controls the call: the customer's BYE still ends the agent.
        l.carrier().bye();
        LiveCalls.await(() -> l.agent().received("BYE"), 5_000, "the agent leg outlived the customer");
        REPORT.put("drop: the call lived and was still controlled", "yes");
        LiveCalls.await(() -> !parityMs.has(l.call().interactionId), 5_000, "parity-ms kept the call after the drop");
        l.customerRtp().close();
        l.agentRtp().close();
    }

    /** R1-4: a claimed, unanswered arrival still gets its 503 from the backstop when TC dies. */
    @Test
    @Order(4)
    void claimedArrivalGets503AfterTcDies() throws Exception {
        calls.begin();
        try (RtpPeer customer = new RtpPeer(0, (byte) 0x71)) {
            LiveCalls.Inbound c = calls.arrive(customer.sdp(1));
            calls.ring(c);
            TestSipAgent.Call carrier = target.agent().call(c.sipCallId);
            LiveCalls.await(() -> carrier.received("180 INVITE"), 5_000, "no 180 for the claimed arrival");
            long t0 = System.currentTimeMillis();
            target.restartProcess(LiveCalls.config(parity.telephonycontroller.model.UuidV7.create()));
            calls.rewatch();
            LiveCalls.await(() -> carrier.finalStatus != 0, 75_000, "the claimed arrival was never ended");
            REPORT.put("claimed arrival, TC restarted: final status / after (s)", carrier.finalStatus + " / "
                    + (System.currentTimeMillis() - t0) / 1000);
            assertEquals(503, carrier.finalStatus);
        }
    }

    /** Step 4 (TC-007): after a TC restart the new process still controls the surviving call's media. */
    @Test
    @Order(5)
    void tcRestartKeepsMediaControl() throws Exception {
        calls.begin();
        Live l = connect((byte) 0x61, (byte) 0x62);
        ParityMsClient fresh = client();
        target.restartProcess(LiveCalls.config(parity.telephonycontroller.model.UuidV7.create()), fresh);
        calls.rewatch();
        Thread.sleep(1_500);
        REPORT.put("restart: the new client knows the call's media", Boolean.toString(fresh.epoch(l.call().interactionId) >= 0));
        RtpPeer moved = new RtpPeer(0, (byte) 0x63);
        moved.sendTo(rtpAddress(l.carrier().remoteSdp));
        l.customerRtp().sendTo(null);
        l.carrier().reinvite(moved.sdp(2));
        LiveCalls.sleep(Duration.ofSeconds(3));
        int movedIn = moved.receivedWith((byte) 0x62);
        REPORT.put("restart: frames to the customer's new port after a re-INVITE", Integer.toString(movedIn));
        assertTrue(movedIn > 60, "after the restart the media didn't follow the customer");
        l.carrier().bye();
        LiveCalls.await(() -> l.agent().received("BYE"), 5_000, "the agent leg outlived the customer");
        LiveCalls.await(() -> !parityMs.has(l.call().interactionId), 5_000, "parity-ms kept the call after the restart");
        REPORT.put("restart: parity-ms holds the call after the hangup", "no");
        moved.close();
        l.customerRtp().close();
        l.agentRtp().close();
    }

    /** Step 4's media commands on a real call: a prompt, digits under a collection, mute, hold. */
    @Test
    @Order(6)
    void mediaCommandsOnARealCall() throws Exception {
        parityMs.prompt("acme", "welcome", 660, 2_000);
        parityMs.prompt("acme", "music", 1_000, 1_000);
        calls.begin();
        Live l = connect((byte) 0x71, (byte) 0x72);
        String c = l.call().customerLeg;
        String play = parity.telephonycontroller.model.UuidV7.create();
        calls.play(l.call(), c, play, "welcome", false);
        LiveCalls.sleep(Duration.ofMillis(1_200));
        double heard = l.customerRtp().toneRatio(660, 440, 880, 1000);
        parity.contracts.telephony.v1.WatchResponse done = calls.awaitEvent(
                r -> r.hasPlayFinished() && r.getPlayFinished().getPlayId().equals(play), 5_000, "no PlayFinished");
        REPORT.put("play: the customer heard the 660 Hz prompt (ratio) / PlayFinished",
                String.format(java.util.Locale.ROOT, "%.1f", heard) + " / " + done.getPlayFinished().getReason()
                        + " after " + done.getPlayFinished().getPlayedMs() + " ms");
        assertTrue(heard > 10, "the customer didn't hear the prompt");
        assertEquals(parity.contracts.telephony.v1.PlayEndReason.PLAY_END_REASON_COMPLETED, done.getPlayFinished().getReason());

        String collection = parity.telephonycontroller.model.UuidV7.create();
        l.customerRtp().sendDigit(5);
        LiveCalls.sleep(Duration.ofMillis(500));
        calls.digits(l.call(), c, parity.contracts.telephony.v1.DigitReporting.DIGIT_REPORTING_ON, collection);
        l.customerRtp().sendDigit(9);
        parity.contracts.telephony.v1.WatchResponse d = calls.awaitEvent(r -> r.hasDtmf(), 5_000, "no DtmfReceived");
        long dtmfs = calls.events().stream().filter(parity.contracts.telephony.v1.WatchResponse::hasDtmf).count();
        REPORT.put("digits: before the collection / under it (digit matches, collection id matches)",
                "none / " + dtmfs + " (" + d.getDtmf().getDigit().equals("9") + ", "
                        + d.getDtmf().getCollectionId().equals(collection) + ")");
        assertEquals("9", d.getDtmf().getDigit());
        assertEquals(1, dtmfs, "a digit outside the collection was reported");

        int before = l.agentRtp().receivedWith((byte) 0x71);
        assertTrue(calls.mute(l.call(), c, true));
        LiveCalls.sleep(Duration.ofSeconds(1));
        int during = l.agentRtp().receivedWith((byte) 0x71);
        LiveCalls.sleep(Duration.ofSeconds(1));
        int stillMuted = l.agentRtp().receivedWith((byte) 0x71) - during;
        calls.mute(l.call(), c, false);
        LiveCalls.sleep(Duration.ofSeconds(1));
        int after = l.agentRtp().receivedWith((byte) 0x71) - during - stillMuted;
        REPORT.put("mute: the customer's frames reaching the agent in 1 s muted / after unmute", stillMuted + " / " + after);
        REPORT.put("mute: the customer's frames reaching the agent in the first second muted", Integer.toString(during - before));
        assertTrue(stillMuted < 5 && after > 30, "mute didn't silence the customer");

        assertTrue(calls.hold(l.call(), true, "music"));
        LiveCalls.sleep(Duration.ofMillis(1_500));
        int agentHears = l.agentRtp().receivedWith((byte) 0x71);
        double music = l.customerRtp().toneRatio(1000, 440, 660, 880);
        LiveCalls.sleep(Duration.ofSeconds(1));
        int agentHearsLater = l.agentRtp().receivedWith((byte) 0x71) - agentHears;
        calls.hold(l.call(), false, "");
        REPORT.put("hold: customer frames to the agent in 1 s / the customer's music (1 kHz ratio)",
                agentHearsLater + " / " + String.format(java.util.Locale.ROOT, "%.1f", music));
        assertTrue(agentHearsLater < 5 && music > 10, "hold didn't mute or play the music");
        l.carrier().bye();
        LiveCalls.await(() -> l.agent().received("BYE"), 5_000, "the agent leg outlived the customer");
        l.customerRtp().close();
        l.agentRtp().close();
    }

    /**
     * Step 4's rest on a real call: an agent's DTMF action code matched by parity-ms, the
     * customer's own hold by re-INVITE, media stopping and coming back, and the sweep deleting a
     * parity-ms call TC doesn't know.
     */
    @Test
    @Order(7)
    void dtmfActionsHoldActivityAndTheSweep() throws Exception {
        calls.begin();
        String orphan = parity.telephonycontroller.model.UuidV7.create();
        parityMs.openOrphan(orphan, TestSipAgent.sdp(39_000, 1, "0"));
        Live l = connect((byte) 0x81, (byte) 0x82);
        calls.dtmfActions(l.call(), l.agentLeg(), "*1", parity.contracts.telephony.v1.DtmfActionKind.DTMF_ACTION_KIND_MUTE_TOGGLE);
        l.agentRtp().sendDigit(10);
        LiveCalls.sleep(Duration.ofMillis(300));
        l.agentRtp().sendDigit(1);
        parity.contracts.telephony.v1.WatchResponse a = calls.awaitEvent(r -> r.hasDtmfAction(), 5_000, "no DtmfAction");
        LiveCalls.sleep(Duration.ofSeconds(1));
        int before = l.customerRtp().receivedWith((byte) 0x82);
        LiveCalls.sleep(Duration.ofSeconds(1));
        int agentHeard = l.customerRtp().receivedWith((byte) 0x82) - before;
        REPORT.put("DTMF action *1 (mute toggle) on the agent: action / result / muted / agent frames heard in 1 s after",
                a.getDtmfAction().getAction() + " / " + a.getDtmfAction().getResult() + " / "
                        + a.getDtmfAction().getState().getMuted() + " / " + agentHeard);
        assertEquals(parity.contracts.telephony.v1.DtmfActionResult.DTMF_ACTION_RESULT_APPLIED, a.getDtmfAction().getResult());
        assertTrue(a.getDtmfAction().getState().getMuted() && agentHeard < 5, "the action code didn't mute the agent");

        String held = l.carrier().localSdp().replaceFirst("o=test 1000 \\d+", "o=test 1000 2").replace("a=sendrecv", "a=sendonly");
        l.carrier().reinvite(held);
        parity.contracts.telephony.v1.WatchResponse h = calls.awaitEvent(r -> r.hasRemoteHold(), 5_000, "no RemoteHold");
        REPORT.put("the customer's own hold by re-INVITE -> RemoteHold on", Boolean.toString(h.getRemoteHold().getOn()));

        l.customerRtp().sendTo(null);
        parity.contracts.telephony.v1.WatchResponse quiet = calls.awaitEvent(r -> r.hasMediaActivity(), 30_000,
                "no MediaActivity after the customer went quiet");
        l.customerRtp().sendTo(rtpAddress(l.carrier().remoteSdp));
        parity.contracts.telephony.v1.WatchResponse back = calls.awaitEvent(
                r -> r.hasMediaActivity() && r.getMediaActivity().getActive(), 20_000, "no MediaActivity when it came back");
        REPORT.put("media activity: the customer quiet / back", quiet.getMediaActivity().getActive() + " / "
                + back.getMediaActivity().getActive());

        LiveCalls.await(() -> !parityMs.has(orphan), 30_000, "the sweep left the orphaned call");
        REPORT.put("the sweep: a parity-ms call TC doesn't know was deleted; the live call kept",
                Boolean.toString(parityMs.has(l.call().interactionId)));
        assertTrue(parityMs.has(l.call().interactionId));
        l.carrier().bye();
        LiveCalls.await(() -> l.agent().received("BYE"), 5_000, "the agent leg outlived the customer");
        l.customerRtp().close();
        l.agentRtp().close();
    }

    /**
     * Must-fix 1: with 50 live calls, TC restarts and its first three channel snapshots time out.
     * The node stays not ready until a full one succeeds; no session is unknown afterwards, and the
     * media sweep (5 s grace here) deletes nothing, nor during an ESL reconnect.
     */
    @Test
    @Order(8)
    void aFailedSnapshotAtStartLosesNoCallAndTheSweepDeletesNothing() throws Exception {
        calls.begin();
        List<LiveCalls.Inbound> live = new java.util.concurrent.CopyOnWriteArrayList<>();
        try (java.util.concurrent.ExecutorService ex = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            List<java.util.concurrent.Future<?>> all = new ArrayList<>();
            java.util.concurrent.Semaphore slots = new java.util.concurrent.Semaphore(10);
            for (int i = 0; i < 50; i++) {
                slots.acquire();
                int port = 30_000 + 2 * i;
                all.add(ex.submit(() -> {
                    try {
                        LiveCalls.Inbound c = calls.arrive(TestSipAgent.sdp(port, 1, "0"));
                        calls.answer(c);
                        String leg = calls.addAgent(c, "sip:agent1@acme.agents.example", Endpoint.ENDPOINT_SIP);
                        TestSipAgent.Call a = target.agent().awaitIncoming(m -> leg.equals(m.h("x-test-leg-id")),
                                Duration.ofSeconds(20));
                        a.answer(TestSipAgent.sdp(port + 10_000, 1, "0"));
                        calls.awaitState(leg, LegState.LEG_STATE_ANSWERED, 20_000);
                        live.add(c);
                        return null;
                    } finally {
                        slots.release();
                    }
                }));
            }
            for (java.util.concurrent.Future<?> fu : all) {
                fu.get();
            }
        }
        assertEquals(50, live.size());
        Thread.sleep(2_000);
        ParityMsClient fresh = client();
        target.restartProcess(LiveCalls.config(parity.telephonycontroller.model.UuidV7.create()), fresh, 3);
        calls.rewatch();
        long retries = target.node().snapshotRetries();
        LiveCalls.sleep(Duration.ofSeconds(25));
        int whole = 0;
        int inParityMs = 0;
        for (LiveCalls.Inbound c : live) {
            parity.contracts.telephony.v1.Session s = calls.session(c);
            if (s.getLegsList().stream().filter(x -> x.getState() == LegState.LEG_STATE_ANSWERED).count() == 2) {
                whole++;
            }
            if (parityMs.has(c.interactionId)) {
                inParityMs++;
            }
        }
        REPORT.put("snapshot timeouts at start: retries / sessions whole / calls kept in parity-ms 25 s later",
                retries + " / " + whole + " / " + inParityMs);
        assertTrue(retries >= 3, "the failed snapshots weren't retried");
        assertEquals(50, whole, "a session lost legs after the failed snapshots");
        assertEquals(50, inParityMs, "the sweep deleted live calls");

        parity.telephonycontroller.esl.EslClient esl = target.node().esl();
        long reconnects = esl.reconnects();
        esl.api("bgapi reload mod_event_socket");
        LiveCalls.await(() -> esl.reconnects() > reconnects && esl.live(), 30_000, "ESL never came back");
        LiveCalls.sleep(Duration.ofSeconds(20));
        int kept = 0;
        for (LiveCalls.Inbound c : live) {
            if (parityMs.has(c.interactionId)) {
                kept++;
            }
        }
        REPORT.put("ESL reconnect with the sweep on: calls kept in parity-ms 20 s later", Integer.toString(kept));
        assertEquals(50, kept, "the sweep deleted live calls around an ESL reconnect");
        target.agent().hangUpAll();
    }

    private static String summarize(List<String> outcomes) {
        Map<String, Integer> n = new LinkedHashMap<>();
        long max = 0;
        for (String o : outcomes) {
            String[] f = o.split(" ", -1);
            n.merge(f[0], 1, Integer::sum);
            max = Math.max(max, Long.parseLong(f[1].replace("ms", "")));
        }
        return n + ", slowest " + max + " ms";
    }

    /** The local ports of freeswitch's own UDP sockets (its fds matched to the host's socket table). */
    static List<String> freeswitchUdpPorts(parity.telephonycontroller.realnode.Freeswitch fs)
            throws IOException, InterruptedException {
        String out = fs.exec("sh", "-c",
                "for f in /proc/1/fd/*; do readlink $f; done | sed -n 's/^socket:\\[\\([0-9]*\\)\\]$/\\1/p' > /tmp/inodes;"
                        + " cat /proc/1/net/udp /proc/1/net/udp6 | awk 'NR==FNR{i[$1]=1;next} ($10 in i){print $2}' /tmp/inodes -");
        TreeSet<String> ports = new TreeSet<>();
        for (String line : out.lines().toList()) {
            int colon = line.lastIndexOf(':');
            if (colon > 0) {
                ports.add(Integer.toString(Integer.parseInt(line.substring(colon + 1).trim(), 16)));
            }
        }
        return List.copyOf(ports);
    }

    /**
     * Part A of R7/P5 on a real call (media-r7-p5): hold music loops in parity-ms with no silent
     * frame between repeats until StopPlay (then STOPPED once, and again already_gone); a corrupt
     * prompt fails only its Play; the agent's AddLeg codes pause and resume the customer's recording
     * on the null sink (no file written) with no DtmfReceived for them, even while reporting is on;
     * the customer's BYE ends the recording (its pause listed) before the customer's leg.
     */
    @Test
    @Order(9)
    void r7p5LoopingHoldMusicAndDtmfRecordingPauseOnTheNullSink() throws Exception {
        parityMs.prompt("acme", "loopmusic", 1_000, 450);
        parityMs.corruptPrompt("acme", "broken");
        calls.begin();
        RtpPeer customer = new RtpPeer(0, (byte) 0x91);
        LiveCalls.Inbound c = calls.arrive(customer.sdp(1));
        calls.answer(c);
        TestSipAgent.Call carrier = target.agent().call(c.sipCallId);
        LiveCalls.await(() -> carrier.finalStatus == 200 && carrier.remoteSdp != null, 5_000, "the carrier got no 200");
        customer.sendTo(rtpAddress(carrier.remoteSdp));
        parity.contracts.telephony.v1.DtmfActions codes = parity.contracts.telephony.v1.DtmfActions.newBuilder()
                .addBindings(binding("*1", parity.contracts.telephony.v1.DtmfActionKind.DTMF_ACTION_KIND_RECORDING_PAUSE))
                .addBindings(binding("*2", parity.contracts.telephony.v1.DtmfActionKind.DTMF_ACTION_KIND_RECORDING_RESUME))
                .addBindings(binding("*3", parity.contracts.telephony.v1.DtmfActionKind.DTMF_ACTION_KIND_MUTE_TOGGLE))
                .setInterDigitTimeoutMs(2_000).build();
        RtpPeer agentRtp = new RtpPeer(0, (byte) 0x92);
        String agentLeg = calls.addAgent(c, "sip:agent1@acme.agents.example", Endpoint.ENDPOINT_SIP, codes);
        TestSipAgent.Call agent = target.agent().awaitIncoming(m -> agentLeg.equals(m.h("x-test-leg-id")),
                Duration.ofSeconds(10));
        assertTrue(agent != null, "no INVITE reached the desk phone");
        agentRtp.sendTo(rtpAddress(agent.remoteSdp));
        agent.ring();
        agent.answer(agentRtp.sdp(1), Map.of());
        calls.awaitState(agentLeg, LegState.LEG_STATE_ANSWERED, 10_000);

        // R7: hold music, looped by parity-ms (450 ms: the repeats meet mid-frame).
        String music = parity.telephonycontroller.model.UuidV7.create();
        calls.play(c, c.customerLeg, music, "loopmusic", true);
        LiveCalls.sleep(Duration.ofMillis(400));
        customer.resetQuiet();
        LiveCalls.sleep(Duration.ofSeconds(3));
        int quiet = customer.quietPackets();
        double tone = customer.toneRatio(1000, 440, 660, 880);
        boolean finishedEarly = calls.events().stream().anyMatch(r -> r.hasPlayFinished());
        REPORT.put("R7 loop: silent frames heard by the customer in 3 s of a 450 ms prompt looping / 1 kHz ratio / PlayFinished before StopPlay",
                quiet + " / " + String.format(java.util.Locale.ROOT, "%.1f", tone) + " / " + finishedEarly);
        assertEquals(0, quiet, "a gap between the loop's repeats");
        assertTrue(tone > 10 && !finishedEarly, "the music didn't loop");
        assertFalse(calls.stopPlay(c, c.customerLeg, music));
        parity.contracts.telephony.v1.WatchResponse stopped = calls.awaitEvent(
                r -> r.hasPlayFinished() && r.getPlayFinished().getPlayId().equals(music), 5_000, "no PlayFinished");
        boolean again = calls.stopPlay(c, c.customerLeg, music);
        REPORT.put("R7 StopPlay: reason / played_ms / a second StopPlay already_gone",
                stopped.getPlayFinished().getReason() + " / " + stopped.getPlayFinished().getPlayedMs() + " / " + again);
        assertEquals(parity.contracts.telephony.v1.PlayEndReason.PLAY_END_REASON_STOPPED, stopped.getPlayFinished().getReason());
        assertTrue(stopped.getPlayFinished().getPlayedMs() >= 3_000 && again);

        // A corrupt prompt fails only its Play: the call goes on.
        String code;
        try {
            calls.play(c, c.customerLeg, parity.telephonycontroller.model.UuidV7.create(), "broken", true);
            code = "accepted";
        } catch (io.grpc.StatusRuntimeException e) {
            code = e.getStatus().getCode() + " / " + e.getTrailers().get(
                    io.grpc.Metadata.Key.of("parity-error", io.grpc.Metadata.ASCII_STRING_MARSHALLER));
        }
        int before = agentRtp.receivedWith((byte) 0x91);
        LiveCalls.sleep(Duration.ofSeconds(1));
        int heard = agentRtp.receivedWith((byte) 0x91) - before;
        REPORT.put("R7 corrupt prompt: Play's status / customer frames reaching the agent in the next second", code + " / " + heard);
        assertEquals("FAILED_PRECONDITION / prompt_unknown", code);
        assertTrue(heard > 30, "the call suffered from a corrupt prompt");

        // P5 on the null sink: the customer's leg recorded; the agent's codes (from AddLeg) pause and resume it.
        String coll = parity.telephonycontroller.model.UuidV7.create();
        calls.digits(c, agentLeg, parity.contracts.telephony.v1.DigitReporting.DIGIT_REPORTING_ON, coll);
        String rec = parity.telephonycontroller.model.UuidV7.create();
        calls.startRecording(c, c.customerLeg, rec);
        calls.awaitEvent(r -> r.hasRecordingStarted(), 5_000, "no RecordingStarted");
        agentRtp.sendDigit(10);
        LiveCalls.sleep(Duration.ofMillis(300));
        agentRtp.sendDigit(1);
        parity.contracts.telephony.v1.WatchResponse paused = calls.awaitEvent(r -> r.hasDtmfAction(), 5_000, "no DtmfAction");
        LiveCalls.sleep(Duration.ofMillis(1_500));
        agentRtp.sendDigit(10);
        LiveCalls.sleep(Duration.ofMillis(300));
        agentRtp.sendDigit(2);
        parity.contracts.telephony.v1.WatchResponse resumed = calls.awaitEvent(r -> r.hasDtmfAction()
                && r.getDtmfAction().getAction() == parity.contracts.telephony.v1.DtmfActionKind.DTMF_ACTION_KIND_RECORDING_RESUME,
                5_000, "no resume");
        LiveCalls.sleep(Duration.ofMillis(500));
        long digits = calls.events().stream().filter(parity.contracts.telephony.v1.WatchResponse::hasDtmf).count();
        REPORT.put("P5 *1 / *2 on the agent: action result recording-state; DtmfReceived for them (reporting ON)",
                paused.getDtmfAction().getAction() + " " + paused.getDtmfAction().getResult() + " "
                        + paused.getDtmfAction().getState().getRecording() + " / " + resumed.getDtmfAction().getResult() + " "
                        + resumed.getDtmfAction().getState().getRecording() + "; " + digits);
        assertEquals(parity.contracts.telephony.v1.DtmfActionResult.DTMF_ACTION_RESULT_APPLIED, paused.getDtmfAction().getResult());
        assertEquals(parity.contracts.telephony.v1.RecordingState.RECORDING_STATE_PAUSED, paused.getDtmfAction().getState().getRecording());
        assertEquals(parity.contracts.telephony.v1.RecordingState.RECORDING_STATE_ACTIVE, resumed.getDtmfAction().getState().getRecording());
        assertEquals(0, digits, "a matched code reached IE as digits");

        carrier.bye();
        parity.contracts.telephony.v1.WatchResponse done = calls.awaitEvent(r -> r.hasRecordingStopped(), 10_000, "no RecordingStopped");
        calls.awaitEvent(r -> r.hasSessionEnded(), 10_000, "no SessionEnded");
        List<parity.contracts.telephony.v1.WatchResponse> evs = calls.events();
        int stoppedAt = -1;
        int customerEnded = -1;
        for (int i = 0; i < evs.size(); i++) {
            if (evs.get(i).hasRecordingStopped()) {
                stoppedAt = i;
            }
            if (evs.get(i).hasLeg() && evs.get(i).getLeg().getLegId().equals(c.customerLeg)
                    && evs.get(i).getLeg().getState() == LegState.LEG_STATE_ENDED) {
                customerEnded = i;
            }
        }
        parity.contracts.telephony.v1.RecordingStopped rs = done.getRecordingStopped();
        long files = parityMs.recordingFiles();
        REPORT.put("P5 RecordingStopped: duration_ms / pauses (ms, by_dtmf, role) / path / size / before the customer's ENDED / files written",
                rs.getDurationMs() + " / " + rs.getPausesList().stream().map(p -> (p.getEndMs() - p.getStartMs()) + " " + p.getByDtmf()
                        + " " + p.getRole()).toList() + " / '" + rs.getPath() + "' / " + rs.getSizeBytes() + " / "
                        + (stoppedAt >= 0 && stoppedAt < customerEnded) + " / " + files);
        assertEquals(1, rs.getPausesCount());
        assertTrue(rs.getPauses(0).getByDtmf() && rs.getPauses(0).getEndMs() - rs.getPauses(0).getStartMs() >= 1_000);
        assertTrue(rs.getPath().isEmpty() && rs.getSizeBytes() == 0 && files == 0, "the null sink wrote something");
        assertTrue(stoppedAt >= 0 && stoppedAt < customerEnded, "RecordingStopped after the customer's end");
        LiveCalls.await(() -> agent.received("BYE"), 5_000, "the agent leg outlived the customer");
        customer.close();
        agentRtp.close();
    }

    private static parity.contracts.telephony.v1.DtmfActionBinding binding(String seq,
            parity.contracts.telephony.v1.DtmfActionKind action) {
        return parity.contracts.telephony.v1.DtmfActionBinding.newBuilder().setDtmfSequence(seq).setAction(action).build();
    }

    static InetSocketAddress rtpAddress(String sdp) {
        String addr = "127.0.0.1";
        int port = -1;
        for (String line : sdp.lines().toList()) {
            if (line.startsWith("c=IN IP4 ")) {
                addr = line.substring(9).trim();
            } else if (line.startsWith("m=audio ")) {
                port = Integer.parseInt(line.split(" ", -1)[1]);
            }
        }
        return new InetSocketAddress(addr, port);
    }
}
