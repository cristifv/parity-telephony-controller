package parity.telephonycontroller.session;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static parity.telephonycontroller.session.EngineFixture.caller;

import java.util.List;
import org.junit.jupiter.api.Test;
import parity.telephonycontroller.model.Kinds.Endpoint;
import parity.telephonycontroller.model.Kinds.LegState;
import parity.telephonycontroller.model.Kinds.Role;
import parity.telephonycontroller.node.NodePort;

/**
 * Part A of R7/P5 in the engine, on the simulated node: a loop is one looped play; AddLeg carries
 * an agent's DTMF codes and locks to its media before it rings (the session's recording lock kept
 * while any leg holds it); recordings on the null sink, refused without it; a DTMF pause and resume
 * reported with the session's recording state; a leg's recording ends before the leg does.
 */
class EngineRecordingTest {

    private static final String I = "0192f6a1-7c3e-7b21-9a4d-5e6f70819203";
    private static final String C = "0192f6a1-8000-7000-8000-000000000001";
    private static final String A = "0192f6a1-8000-7000-8000-0000000000a1";
    private static final String S = "0192f6a1-8000-7000-8000-0000000000b1";
    private static final String R1 = "0192f6a1-c000-7000-8000-000000000001";
    private static final String R2 = "0192f6a1-c000-7000-8000-000000000002";
    private static final List<NodePort.DtmfBinding> AGENT_CODES = List.of(
            new NodePort.DtmfBinding("*1", "RECORDING_PAUSE"), new NodePort.DtmfBinding("*2", "RECORDING_RESUME"),
            new NodePort.DtmfBinding("*3", "MUTE_TOGGLE"));

    private final EngineFixture f = new EngineFixture(TelephonyNode.Limits.DEFAULT);
    private final EngineFixture.RecordingSink sink = new EngineFixture.RecordingSink();
    private int n;

    private Requests.CommandKey k() {
        n++;
        return EngineFixture.key("r" + n);
    }

    private Requests.Owner o() {
        return EngineFixture.owner(I, 1042, "r1");
    }

    private void answered() throws Exception {
        f.engine().watch(caller(), "r1", "bin-a", sink);
        f.arrive(I, C);
        f.claim(I, 1042, "r1");
        f.engine().answer(caller(), o(), k(), C);
    }

    private Views.LegResult addLeg(String leg, Role role, Requests.Dtmf dtmf) throws Exception {
        return f.engine().addLeg(caller(), o(), k(), new Requests.AddLeg(leg, role, Endpoint.WEBRTC, 30, "fp-" + leg,
                "sip:agent-7f3a@acme.agents.example", null, null, "", dtmf));
    }

    private void agentAnswered(Requests.Dtmf dtmf) throws Exception {
        addLeg(A, Role.AGENT, dtmf);
        f.node.remoteAnswer(A);
    }

    /** Each event once (the clock's advances redeliver the unacknowledged ones every 2 s). */
    private List<Events.Event> all() {
        java.util.Map<Long, Events.Event> bySeq = new java.util.TreeMap<>();
        sink.sessionEvents().forEach(d -> bySeq.putIfAbsent(d.seq(), d.event()));
        return List.copyOf(bySeq.values());
    }

    private <E extends Events.Event> List<E> events(Class<E> type) {
        return all().stream().filter(type::isInstance).map(type::cast).toList();
    }

    private void start(String recordingId, String leg, String kek) throws Exception {
        f.engine().startRecording(caller(), o(), k(), leg, recordingId, "", kek);
    }

    @Test
    void aLoopIsOneLoopedPlayAndHoldMusicLoopsToo() throws Exception {
        answered();
        f.engine().play(caller(), o(), k(), C, R1, "music", true);
        assertThat(f.node.lastPlayLooped(C)).isTrue();
        f.engine().play(caller(), o(), k(), C, R2, "welcome", false);
        assertThat(f.node.lastPlayLooped(C)).isFalse();
        f.engine().setHold(caller(), o(), k(), true, "music");
        assertThat(f.node.lastPlayLooped(C)).as("hold music is a loop").isTrue();
    }

    @Test
    void addLegCarriesTheAgentsCodesToItsMediaBeforeItRingsAndNeverACustomers() throws Exception {
        answered();
        assertThat(addLeg(A, Role.AGENT, new Requests.Dtmf(AGENT_CODES, List.of("RECORDING"), 0)).state())
                .isEqualTo(LegState.ORIGINATING);
        NodePort.DtmfSetup setup = f.node.dtmfSetup(A);
        assertThat(setup).as("set with the originate, before any ring").isNotNull();
        assertThat(setup.role()).isEqualTo("agent");
        assertThat(setup.bindings()).extracting(NodePort.DtmfBinding::action)
                .containsExactly("RECORDING_PAUSE", "RECORDING_RESUME", "MUTE_TOGGLE");
        assertThat(setup.interDigitMs()).isEqualTo(2_000);
        assertThat(setup.lock()).containsExactly("RECORDING");
        assertThat(setup.unlock()).containsExactlyInAnyOrder("MUTE", "MODE");
        assertThat(setup.toString()).as("PCI: the codes' digits are never printed").doesNotContain("*1");

        String x = "0192f6a1-8000-7000-8000-0000000000c1";
        assertThatThrownBy(() -> addLeg(x, Role.EXTERNAL, new Requests.Dtmf(
                List.of(new NodePort.DtmfBinding("*5", "MUTE")), List.of(), 0)))
                .as("no codes on a customer's or external leg")
                .isInstanceOfSatisfying(TelephonyException.class, e -> assertThat(e.code()).isEqualTo(Code.INVALID_REQUEST));
        assertThatThrownBy(() -> addLeg(S, Role.SUPERVISOR, new Requests.Dtmf(
                List.of(new NodePort.DtmfBinding("*1", "RECORDING_PAUSE")), List.of(), 0)))
                .as("a supervisor can't pause the recording")
                .isInstanceOfSatisfying(TelephonyException.class, e -> assertThat(e.code()).isEqualTo(Code.INVALID_REQUEST));
    }

    @Test
    void theSessionsRecordingLockHoldsWhileAnyLegAsksForIt() throws Exception {
        answered();
        agentAnswered(new Requests.Dtmf(AGENT_CODES, List.of("RECORDING"), 2_000));
        addLeg(S, Role.SUPERVISOR, new Requests.Dtmf(List.of(new NodePort.DtmfBinding("*7", "MUTE_TOGGLE")), List.of(), 0));
        assertThat(f.node.dtmfSetup(S).lock()).as("the agent's consent lock is the session's").containsExactly("RECORDING");
        f.node.remoteAnswer(S);
        f.engine().setDtmfActions(caller(), o(), k(), S, List.of(), List.of("MUTE"), 0);
        assertThat(f.node.dtmfSetup(S).lock()).containsExactlyInAnyOrder("MUTE", "RECORDING");
        // The agent's own codes without the lock: no leg asks for it any more, so it is freed.
        f.engine().setDtmfActions(caller(), o(), k(), A, AGENT_CODES, List.of(), 0);
        assertThat(f.node.dtmfSetup(A).unlock()).contains("RECORDING");
    }

    @Test
    void aNullSinkRecordingPausesByDtmfAndEndsBeforeItsLegAndTheSession() throws Exception {
        answered();
        agentAnswered(new Requests.Dtmf(AGENT_CODES, List.of(), 2_000));
        start(R1, C, TelephonyNode.NULL_SINK_KEK);
        assertThat(f.engine().startRecording(caller(), o(), k(), C, R1, "", TelephonyNode.NULL_SINK_KEK))
                .as("repeat-safe by recording id").isTrue();
        assertThat(events(Events.RecordingStarted.class)).singleElement()
                .isEqualTo(new Events.RecordingStarted(R1, C));
        f.clock.advance(1_000);
        f.node.dtmfActionMatch(A, "RECORDING_PAUSE", "APPLIED");
        f.clock.advance(2_000);
        f.node.dtmfActionMatch(A, "RECORDING_RESUME", "APPLIED");
        f.node.dtmfActionMatch(A, "MUTE_TOGGLE", "APPLIED");
        f.clock.advance(1_000);
        assertThat(events(Events.DtmfAction.class)).extracting(Events.DtmfAction::recording)
                .containsExactly("PAUSED", "ACTIVE", "ACTIVE");
        assertThat(events(Events.DtmfAction.class).get(2).muted()).isTrue();

        f.node.remoteEnd(C, parity.telephonycontroller.model.Kinds.EndCause.NORMAL, 0);
        List<Events.Event> all = all();
        int stopped = indexOf(all, Events.RecordingStopped.class);
        int customerEnded = -1;
        for (int i = 0; i < all.size(); i++) {
            if (all.get(i) instanceof Events.LegUpdate u && u.legId().equals(C) && u.state() == LegState.ENDED) {
                customerEnded = i;
            }
        }
        assertThat(stopped).as("the recording's end before its leg's").isBetween(0, customerEnded - 1);
        assertThat(indexOf(all, Events.SessionEnded.class)).isGreaterThan(customerEnded);
        Events.RecordingStopped r = (Events.RecordingStopped) all.get(stopped);
        assertThat(r.durationMs()).isEqualTo(4_000);
        assertThat(r.pauses()).containsExactly(new NodePort.RecordingPause(EngineFixture.START + 1_000,
                EngineFixture.START + 3_000, Role.AGENT, true));
        assertThat(r.path()).isEmpty();
        assertThat(r.sizeBytes()).isZero();
        assertThat(r.kekId()).isEqualTo(TelephonyNode.NULL_SINK_KEK);
    }

    @Test
    void withoutTheNullSinkOrWithAKeyForARealSinkStartRecordingIsRefused() throws Exception {
        answered();
        long refused = TelephonyNode.recordingsRefused();
        assertThatThrownBy(() -> start(R1, C, "kek-2026-10"))
                .as("a key meant for a real sink is never 'recorded' into the null one")
                .isInstanceOfSatisfying(TelephonyException.class, e -> assertThat(e.code()).isEqualTo(Code.INVALID_STATE));
        f.node.setRecordingNullSink(false);
        assertThatThrownBy(() -> start(R1, C, TelephonyNode.NULL_SINK_KEK))
                .as("no sink on this node (production until R9)")
                .isInstanceOfSatisfying(TelephonyException.class, e -> assertThat(e.code()).isEqualTo(Code.INVALID_STATE));
        assertThat(TelephonyNode.recordingsRefused() - refused).isEqualTo(2);
        assertThat(events(Events.RecordingStarted.class)).isEmpty();
    }

    @Test
    void stopRecordingIsRepeatSafeAndOneRecordingPerLeg() throws Exception {
        answered();
        start(R1, C, TelephonyNode.NULL_SINK_KEK);
        assertThatThrownBy(() -> start(R2, C, TelephonyNode.NULL_SINK_KEK))
                .as("one open recording per leg")
                .isInstanceOfSatisfying(TelephonyException.class, e -> assertThat(e.code()).isEqualTo(Code.INVALID_STATE));
        f.clock.advance(500);
        Views.Gone g = f.engine().stopRecording(caller(), o(), k(), R1);
        assertThat(g.alreadyGone()).isFalse();
        assertThat(events(Events.RecordingStopped.class)).singleElement()
                .satisfies(r -> assertThat(r.durationMs()).isEqualTo(500));
        assertThat(f.engine().stopRecording(caller(), o(), k(), R1).alreadyGone()).isTrue();
        assertThat(f.engine().stopRecording(caller(), o(), k(), R2).alreadyGone()).as("never started here").isTrue();
        assertThat(events(Events.RecordingStopped.class)).hasSize(1);
    }

    @Test
    void theRecordingStateIsUnsetWhileTheSessionNeverRecorded() throws Exception {
        answered();
        agentAnswered(new Requests.Dtmf(AGENT_CODES, List.of(), 0));
        f.node.dtmfActionMatch(A, "MUTE_TOGGLE", "APPLIED");
        assertThat(events(Events.DtmfAction.class)).singleElement()
                .satisfies(a -> assertThat(a.recording()).as("as before recordings existed (N-1 scripts)").isEmpty());
    }

    private static int indexOf(List<Events.Event> all, Class<?> type) {
        for (int i = 0; i < all.size(); i++) {
            if (type.isInstance(all.get(i))) {
                return i;
            }
        }
        return -1;
    }
}
