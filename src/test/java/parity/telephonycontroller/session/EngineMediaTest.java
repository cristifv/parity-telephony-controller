package parity.telephonycontroller.session;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static parity.telephonycontroller.session.EngineFixture.caller;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Step 4's media commands in the engine (TC ADR 0003 D6), on the simulated node: plays end in
 * order and a loop plays again; StopPlay; mute; digit collections (masked, on, off at their
 * deadline); hold.
 */
class EngineMediaTest {

    private static final String I = "0192f6a1-7c3e-7b21-9a4d-5e6f70819203";
    private static final String C = "0192f6a1-8000-7000-8000-000000000001";
    private static final String P1 = "0192f6a1-9000-7000-8000-000000000001";
    private static final String P2 = "0192f6a1-9000-7000-8000-000000000002";
    private static final String COLL = "0192f6a1-a000-7000-8000-000000000001";

    private final EngineFixture f = new EngineFixture(TelephonyNode.Limits.DEFAULT);
    private final EngineFixture.RecordingSink sink = new EngineFixture.RecordingSink();
    private int n;

    private Requests.CommandKey k() {
        n++;
        return EngineFixture.key("m" + n);
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

    private <E extends Events.Event> List<E> events(Class<E> type) {
        return sink.sessionEvents().stream().map(Events.Delivery::event).filter(type::isInstance).map(type::cast).toList();
    }

    @Test
    void playsEndInOrderAndALoopPlaysAgainUntilStopped() throws Exception {
        f.node.setAsyncPlays(false); // a parity-ms without play_async (the play saga's fallback)
        answered();
        assertThat(f.engine().play(caller(), o(), k(), C, P1, "welcome", false)).isFalse();
        assertThat(f.engine().play(caller(), o(), k(), C, P1, "welcome", false)).as("repeat-safe by play id").isTrue();
        f.engine().play(caller(), o(), k(), C, P2, "music", true);
        f.node.playDone(C, "replaced", 300);
        assertThat(events(Events.PlayFinished.class)).singleElement().satisfies(p -> {
            assertThat(p.playId()).isEqualTo(P1);
            assertThat(p.reason()).isEqualTo("STOPPED");
        });
        f.node.playDone(C, "completed", 5_000);
        assertThat(events(Events.PlayFinished.class)).as("a loop plays again").hasSize(1);
        TelephonyNode.Stopped st = f.engine().stopPlay(caller(), o(), k(), C, P2);
        assertThat(st.alreadyGone()).isFalse();
        f.node.playDone(C, "stopped", 1_200);
        assertThat(events(Events.PlayFinished.class)).last().satisfies(p -> {
            assertThat(p.playId()).isEqualTo(P2);
            assertThat(p.reason()).isEqualTo("STOPPED");
        });
        assertThat(f.engine().stopPlay(caller(), o(), k(), C, P2).alreadyGone()).isTrue();
    }

    /**
     * MS-042 / TC-050: a play TC gave up on (its send failed; parity-ms may start it later) ends under
     * its own id, which TC doesn't track: ignored, never taken for the retry that replaced it.
     */
    @Test
    void aPlayTcGaveUpOnNeverEndsTheRetryThatReplacedIt() throws Exception {
        f.node.setAsyncPlays(false);
        answered();
        f.node.setLoopsNatively(true);
        f.node.setParityMsUp(false);
        assertThatThrownBy(() -> f.engine().play(caller(), o(), k(), C, P1, "music", true))
                .isInstanceOf(TelephonyException.class);
        String gaveUp = f.node.lastPlayId(C);
        f.node.setParityMsUp(true);
        assertThat(f.engine().play(caller(), o(), k(), C, P1, "music", true)).as("the retry runs").isFalse();
        String retry = f.node.lastPlayId(C);
        assertThat(retry).isNotEqualTo(gaveUp).startsWith("tc-");
        // The play TC gave up on started after all and is replaced by the retry: its end is ignored.
        f.node.playDone(C, "replaced", 900, gaveUp);
        assertThat(events(Events.PlayFinished.class)).isEmpty();
        assertThat(f.engine().stopPlay(caller(), o(), k(), C, P1).alreadyGone()).as("the retry still plays").isFalse();
        f.node.playDone(C, "stopped", 1_200, retry);
        assertThat(events(Events.PlayFinished.class)).singleElement().satisfies(p -> {
            assertThat(p.playId()).isEqualTo(P1);
            assertThat(p.reason()).isEqualTo("STOPPED");
        });
    }

    /** TC-051: a loop the media loops itself is never sent again when it reports {@code completed}. */
    @Test
    void aLoopTheMediaLoopsItselfIsNeverSentAgain() throws Exception {
        f.node.setAsyncPlays(false);
        answered();
        f.node.setLoopsNatively(true);
        f.engine().play(caller(), o(), k(), C, P2, "music", true);
        String sent = f.node.lastPlayId(C);
        f.node.playDone(C, "completed", 0, sent);
        assertThat(f.node.lastPlayId(C)).as("not sent again").isEqualTo(sent);
        assertThat(events(Events.PlayFinished.class)).singleElement().satisfies(p -> {
            assertThat(p.playId()).isEqualTo(P2);
            assertThat(p.reason()).isEqualTo("COMPLETED");
        });
        // A media that plays it once (older): sent again, under a new id, and IE hears nothing yet.
        f.node.setLoopsNatively(false);
        f.engine().play(caller(), o(), k(), C, P1, "music", true);
        String once = f.node.lastPlayId(C);
        f.node.playDone(C, "completed", 5_000, once);
        assertThat(f.node.lastPlayId(C)).isNotEqualTo(once);
        assertThat(events(Events.PlayFinished.class)).hasSize(1);
    }

    @Test
    void muteChangesOnce() throws Exception {
        answered();
        assertThat(f.engine().setMute(caller(), o(), k(), C, true)).isTrue();
        assertThat(f.engine().setMute(caller(), o(), k(), C, true)).isFalse();
        assertThat(f.engine().setMute(caller(), o(), k(), C, false)).isTrue();
    }

    @Test
    void digitsReachIeOnlyUnderACollectionMaskedOrNotAndStopAtItsDeadline() throws Exception {
        answered();
        f.node.remoteDigit(C, "5", "rfc4733");
        assertThat(events(Events.DtmfReceived.class)).as("reporting is off").isEmpty();
        f.engine().setDigitReporting(caller(), o(), k(), C, "MASKED", COLL, 30);
        assertThat(f.node.digitMode(C)).isEqualTo("masked");
        f.node.remoteDigit(C, "5", "rfc4733");
        assertThat(events(Events.DtmfReceived.class)).singleElement().satisfies(d -> {
            assertThat(d.digit()).as("masked").isEmpty();
            assertThat(d.collectionId()).isEqualTo(COLL);
        });
        f.engine().setDigitReporting(caller(), o(), k(), C, "ON", COLL, 30);
        f.node.remoteDigit(C, "#", "inband");
        assertThat(events(Events.DtmfReceived.class)).last().satisfies(d -> {
            assertThat(d.digit()).isEqualTo("#");
            assertThat(d.source()).isEqualTo("INBAND");
        });
        assertThat(events(Events.DtmfReceived.class).getLast().toString()).as("PCI: never printed").doesNotContain("#");
        f.clock.advance(31_000);
        assertThat(f.node.digitMode(C)).as("off at the collection's deadline").isEqualTo("off");
    }

    @Test
    void anAdopterGetsNoBufferedDigitsAndReportingIsOffEverywhere() throws Exception {
        answered();
        f.engine().setDigitReporting(caller(), o(), k(), C, "ON", COLL, 30);
        f.node.remoteDigit(C, "4", "rfc4733");
        assertThat(events(Events.DtmfReceived.class)).hasSize(1);
        assertThat(f.engine().buffered(EngineFixture.TENANT, I)).as("the digit is unacknowledged").isPositive();
        // r2 adopts: the fence rises; r2's stream gets the replay.
        EngineFixture.RecordingSink adopter = new EngineFixture.RecordingSink();
        f.engine().watch(caller(), "r2", "bin-a", adopter);
        f.claim(I, 1052, "r2");
        f.engine().claim(caller(), EngineFixture.owner(I, 1052, "r2"));
        assertThat(adopter.sessionEvents()).extracting(Events.Delivery::event)
                .noneMatch(e -> e instanceof Events.DtmfReceived);
        assertThat(f.node.digitMode(C)).as("parity-ms told: off").isEqualTo("off");
        f.node.remoteDigit(C, "4", "rfc4733");
        assertThat(adopter.sessionEvents()).extracting(Events.Delivery::event)
                .noneMatch(e -> e instanceof Events.DtmfReceived);
    }

    @Test
    void holdMutesBothAndPlaysMusicToTheCustomerOnce() throws Exception {
        answered();
        assertThat(f.engine().setHold(caller(), o(), k(), true, "music")).isTrue();
        assertThat(f.engine().setHold(caller(), o(), k(), true, "music")).isFalse();
        f.node.playDone(C, "completed", 30_000);
        assertThat(events(Events.PlayFinished.class)).as("hold music is TC's, not IE's").isEmpty();
        assertThat(f.engine().setHold(caller(), o(), k(), false, "")).isTrue();
        assertThat(f.engine().setMute(caller(), o(), k(), C, false)).as("hold off unmuted").isFalse();
    }

    @Test
    void aMissingPromptIsPromptUnknown() throws Exception {
        answered();
        f.node.promptMissing("nosuch");
        assertThatThrownBy(() -> f.engine().play(caller(), o(), k(), C, P1, "nosuch", false))
                .isInstanceOfSatisfying(TelephonyException.class, e -> assertThat(e.code()).isEqualTo(Code.PROMPT_UNKNOWN));
    }

    @Test
    void remoteHoldAndMediaActivityReachIeOncePerChange() throws Exception {
        answered();
        f.node.remoteHold(C, true);
        f.node.remoteHold(C, true);
        f.node.mediaActivity(C, false);
        f.node.mediaActivity(C, false);
        f.node.mediaActivity(C, true);
        assertThat(events(Events.RemoteHold.class)).singleElement().satisfies(h -> assertThat(h.on()).isTrue());
        assertThat(events(Events.MediaActivity.class)).extracting(Events.MediaActivity::active).containsExactly(false, true);
    }

    @Test
    void dtmfActionsAreCheckedByRoleAndReportedWithTheStateAfter() throws Exception {
        answered();
        List<parity.telephonycontroller.node.NodePort.DtmfBinding> mute =
                List.of(new parity.telephonycontroller.node.NodePort.DtmfBinding("*1", "MUTE_TOGGLE"));
        assertThatThrownBy(() -> f.engine().setDtmfActions(caller(), o(), k(), C, mute, List.of(), 0))
                .as("a customer binds none")
                .isInstanceOfSatisfying(TelephonyException.class, e -> assertThat(e.code()).isEqualTo(Code.INVALID_REQUEST));
        f.node.dtmfActionMatch(C, "MUTE_TOGGLE", "APPLIED");
        assertThat(events(Events.DtmfAction.class)).singleElement().satisfies(a -> {
            assertThat(a.muted()).isTrue();
            assertThat(a.toString()).doesNotContain("*1");
        });
        List<parity.telephonycontroller.node.NodePort.DtmfBinding> prefix = List.of(
                new parity.telephonycontroller.node.NodePort.DtmfBinding("*1", "MUTE"),
                new parity.telephonycontroller.node.NodePort.DtmfBinding("*12", "UNMUTE"));
        assertThat(prefix.toString()).as("PCI: codes never printed").doesNotContain("*1");
    }

    @Test
    void mediaCommandsNeedAnAnsweredLeg() throws Exception {
        f.engine().watch(caller(), "r1", "bin-a", sink);
        f.arrive(I, C);
        f.claim(I, 1042, "r1");
        assertThatThrownBy(() -> f.engine().play(caller(), o(), k(), C, P1, "welcome", false))
                .isInstanceOfSatisfying(TelephonyException.class, e -> assertThat(e.code()).isEqualTo(Code.INVALID_STATE));
    }
}
