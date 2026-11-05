package parity.telephonycontroller.session;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static parity.telephonycontroller.session.EngineFixture.caller;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The play saga (TC ADR 0004) on the simulated node, which plays asynchronously like a parity-ms
 * with {@code play_async}: one id per play for its whole life, outcomes by events, and a
 * compensation ({@code stop_play} by id) for every play TC no longer wants. The property under
 * test: whatever the order of the caller giving up, the stop, the prompt's load and the media's
 * answers, no play TC doesn't want is left running, and IE hears one end per play.
 */
class EnginePlaySagaTest {

    private static final String I = "0192f6a1-7c3e-7b21-9a4d-5e6f70819203";
    private static final String C = "0192f6a1-8000-7000-8000-000000000001";
    private static final String P1 = "0192f6a1-9000-7000-8000-000000000001";
    private static final String P2 = "0192f6a1-9000-7000-8000-000000000002";

    private final EngineFixture f = new EngineFixture(TelephonyNode.Limits.DEFAULT);
    private final EngineFixture.RecordingSink sink = new EngineFixture.RecordingSink();
    private int n;

    private Requests.CommandKey k() {
        n++;
        return EngineFixture.key("s" + n);
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

    /** IE's ends of play {@code playId}: PlayFinished and PlayFailed. */
    private List<Events.Event> ends(String playId) {
        return sink.sessionEvents().stream().map(Events.Delivery::event)
                .filter(e -> (e instanceof Events.PlayFinished done && done.playId().equals(playId))
                        || (e instanceof Events.PlayFailed failed && failed.playId().equals(playId)))
                .toList();
    }

    private TelephonyNode.Played play(String playId, boolean resultEvents) throws Exception {
        return f.engine().play(caller(), o(), k(), C, playId, "welcome", false, resultEvents);
    }

    @Test
    void withResultEventsTheAnswerIsAtOnceAndTheOutcomeComesAsEvents() throws Exception {
        answered();
        f.node.holdPlayStarts(true);
        TelephonyNode.Played p = play(P1, true);
        assertThat(p.started()).as("nothing to wait for").isNull();
        assertThat(events(Events.PlayStarted.class)).as("the prompt still loads").isEmpty();
        f.node.promptLoaded(C);
        assertThat(events(Events.PlayStarted.class)).singleElement().satisfies(e -> assertThat(e.playId()).isEqualTo(P1));
        assertThat(play(P1, true).existing()).as("repeat-safe by play id").isTrue();
        assertThat(f.engine().stopPlay(caller(), o(), k(), C, P1).alreadyGone()).isFalse();
        assertThat(f.node.stopsById()).singleElement().isEqualTo(f.node.lastPlayId(C));
        f.node.playDone(C, "stopped", 700);
        assertThat(ends(P1)).singleElement().isInstanceOfSatisfying(Events.PlayFinished.class,
                e -> assertThat(e.reason()).isEqualTo("STOPPED"));
        // A prompt the media doesn't have: the answer is still OK; PlayFailed(PROMPT_UNKNOWN) tells it.
        f.node.holdPlayStarts(false);
        f.node.promptMissing("nosuch");
        f.engine().play(caller(), o(), k(), C, P2, "nosuch", false, true);
        assertThat(ends(P2)).singleElement().isInstanceOfSatisfying(Events.PlayFailed.class,
                e -> assertThat(e.reason()).isEqualTo("PROMPT_UNKNOWN"));
    }

    /**
     * The reviewer's orphan, any timing: IE's StopPlay before the prompt loaded, while the stop can't
     * reach parity-ms, and after the start. Each time the media ends up playing nothing, and IE hears
     * one end (PlayFinished(STOPPED)).
     */
    @Test
    void aPlayStoppedBeforeOrAfterItsStartNeverPlaysOnWhateverTheOrder() throws Exception {
        answered();
        f.node.holdPlayStarts(true);
        // 1. Stopped while loading: cancelled in the media; the load's end starts nothing.
        play(P1, true);
        f.engine().stopPlay(caller(), o(), k(), C, P1);
        f.node.promptLoaded(C);
        assertThat(f.node.isPlaying(C)).isFalse();
        assertThat(ends(P1)).singleElement().isInstanceOfSatisfying(Events.PlayFinished.class,
                e -> assertThat(e.reason()).isEqualTo("STOPPED"));
        assertThat(events(Events.PlayStarted.class)).isEmpty();

        // 2. Stopped while parity-ms doesn't answer: the stop is lost, the prompt loads, the play
        // starts; TC stops it at once (its tombstone), and IE still hears one end.
        play(P2, true);
        String wire = f.node.lastPlayId(C);
        long late = TelephonyNode.playsCompensated("late_start");
        f.node.setParityMsUp(false);
        f.engine().stopPlay(caller(), o(), k(), C, P2);
        f.node.setParityMsUp(true);
        f.node.promptLoaded(C);
        assertThat(f.node.isPlaying(C)).as("the late start was stopped").isFalse();
        assertThat(f.node.stopsById()).last().isEqualTo(wire);
        assertThat(TelephonyNode.playsCompensated("late_start")).isEqualTo(late + 1);
        assertThat(ends(P2)).hasSize(1);
        assertThat(events(Events.PlayStarted.class)).as("a play IE stopped never 'starts' for IE").isEmpty();
    }

    /**
     * An IE without result_events waits for the start; past its deadline TC calls the play off (the
     * stop cancels the load), and a start that comes later anyway is stopped. A start that wins the
     * race answers OK.
     */
    @Test
    void aLegacyCallerThatGivesUpLeavesNoPlayRunning() throws Exception {
        answered();
        f.node.holdPlayStarts(true);
        long gaveUp = TelephonyNode.playsCompensated("caller_gave_up");
        TelephonyNode.Played p = play(P1, false);
        String wire = f.node.lastPlayId(C);
        assertThatThrownBy(() -> f.engine().awaitStart(p, o(), C, P1, 1))
                .isInstanceOfSatisfying(TelephonyException.class, e -> assertThat(e.code()).isEqualTo(Code.PARITY_MS_UNAVAILABLE));
        assertThat(TelephonyNode.playsCompensated("caller_gave_up")).isEqualTo(gaveUp + 1);
        assertThat(f.node.stopsById()).containsExactly(wire);
        f.node.promptLoaded(C);
        f.node.startedLate(C, wire);
        assertThat(f.node.isPlaying(C)).as("a start after the caller gave up is stopped").isFalse();
        assertThat(sink.sessionEvents().stream().map(Events.Delivery::event)
                .filter(e -> e instanceof Events.PlayFinished || e instanceof Events.PlayStarted)).isEmpty();

        // The start wins: answered OK.
        f.node.holdPlayStarts(false);
        TelephonyNode.Played q = play(P2, false);
        f.engine().awaitStart(q, o(), C, P2, 1);
        assertThat(f.node.isPlaying(C)).isTrue();
    }

    /**
     * A send whose answer is lost is sent again with the same id (never a new one), at most three
     * times; then TC gives up by compensating first, and only then reports PlayFailed. A start that
     * comes later is stopped.
     */
    @Test
    void anUnansweredSendIsRetriedWithTheSameIdThenGivenUpByCompensation() throws Exception {
        answered();
        f.node.setParityMsUp(false);
        int sends = f.node.playSends();
        assertThat(play(P1, true).existing()).as("TC took the play: the events tell the rest").isFalse();
        String wire = f.node.lastPlayId(C);
        f.clock.advance(3_000);
        assertThat(f.node.playSends() - sends).as("the first send and three retries").isEqualTo(4);
        assertThat(f.node.lastPlayId(C)).as("always the same id").isEqualTo(wire);
        assertThat(ends(P1)).singleElement().isInstanceOfSatisfying(Events.PlayFailed.class,
                e -> assertThat(e.reason()).isEqualTo("PARITY_MS_UNAVAILABLE"));
        f.node.setParityMsUp(true);
        f.node.startedLate(C, wire);
        assertThat(f.node.isPlaying(C)).as("a start after TC gave up is stopped").isFalse();
        assertThat(events(Events.PlayStarted.class)).isEmpty();

        // Retried successfully: started under the same id.
        f.node.setParityMsUp(false);
        play(P2, true);
        String second = f.node.lastPlayId(C);
        f.node.setParityMsUp(true);
        f.clock.advance(200);
        assertThat(f.node.lastPlayId(C)).isEqualTo(second);
        assertThat(events(Events.PlayStarted.class)).singleElement().satisfies(e -> assertThat(e.playId()).isEqualTo(P2));
    }

    /** A newer play supersedes one still loading: PlayFailed(REPLACED) once, whatever the media says after. */
    @Test
    void aNewerPlaySupersedesOneStillLoadingWithOneEnd() throws Exception {
        answered();
        f.node.holdPlayStarts(true);
        play(P1, true);
        play(P2, true);
        f.node.promptLoaded(C);
        assertThat(ends(P1)).singleElement().isInstanceOfSatisfying(Events.PlayFailed.class,
                e -> assertThat(e.reason()).isEqualTo("REPLACED"));
        assertThat(events(Events.PlayStarted.class)).singleElement().satisfies(e -> assertThat(e.playId()).isEqualTo(P2));
    }

    /** Hold music off before it started: called off (no failure counted), and a late start is stopped. */
    @Test
    void holdOffBeforeTheMusicStartedLeavesNoMusic() throws Exception {
        answered();
        f.node.holdPlayStarts(true);
        long failed = TelephonyNode.playsFailed("CANCELLED");
        assertThat(f.engine().setHold(caller(), o(), k(), true, "music")).isTrue();
        String wire = f.node.lastPlayId(C);
        assertThat(f.engine().setHold(caller(), o(), k(), false, "")).isTrue();
        assertThat(f.node.stopsById()).containsExactly(wire);
        f.node.startedLate(C, wire);
        assertThat(f.node.isPlaying(C)).isFalse();
        assertThat(TelephonyNode.playsFailed("CANCELLED")).isEqualTo(failed);
        assertThat(sink.sessionEvents().stream().map(Events.Delivery::event)
                .filter(e -> e instanceof Events.PlayFinished || e instanceof Events.PlayFailed
                        || e instanceof Events.PlayStarted)).as("hold music is TC's, not IE's").isEmpty();
    }
}
