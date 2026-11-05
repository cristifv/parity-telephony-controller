package parity.telephonycontroller.session;

import static org.assertj.core.api.Assertions.assertThat;
import static parity.telephonycontroller.session.EngineFixture.TENANT;
import static parity.telephonycontroller.session.EngineFixture.caller;
import static parity.telephonycontroller.session.EngineFixture.key;
import static parity.telephonycontroller.session.EngineFixture.owner;

import java.util.List;
import org.junit.jupiter.api.Test;
import parity.telephonycontroller.model.Kinds.Endpoint;
import parity.telephonycontroller.model.Kinds.Role;

/** TC ADR 0001 §4.4's limits on unacknowledged events, with a stream IE has stopped reading. */
class DeliveryLimitsTest {

    private static final String I = "0192f6a1-7c3e-7b21-9a4d-5e6f70819203";
    private static final String C = "0192f6a1-8000-7000-8000-000000000001";

    /**
     * An answered call with {@code agents} agent legs answered: events 1 to 2 + 2 × agents, none
     * acked; the claim withdrew event 1 (the offer), so 1 + 2 × agents are held.
     */
    private static void call(EngineFixture f, int agents) throws Exception {
        f.arrive(I, C);
        f.claim(I, 1042, "r1");
        f.engine().answer(caller(), owner(I, 1042, "r1"), key("c-answer"), C);
        for (int i = 0; i < agents; i++) {
            String leg = String.format("0192f6a1-8000-7000-8000-%012d", 100 + i);
            f.engine().addLeg(caller(), owner(I, 1042, "r1"), key("c-add-" + i),
                    new Requests.AddLeg(leg, Role.AGENT, Endpoint.WEBRTC, 30, "fp-" + i));
            f.node.remoteRing(leg);
            f.node.remoteAnswer(leg);
        }
    }

    @Test
    void aSessionKeepsItsNewestEventsAndTheReconnectShowsTheGap() throws Exception {
        EngineFixture f = new EngineFixture(new TelephonyNode.Limits(4, 600_000, 64L << 20));
        EngineFixture.RecordingSink stuck = new EngineFixture.RecordingSink();
        stuck.stuck = true;
        f.engine().watch(caller(), "r1", "bin-a", stuck);
        long before = TelephonyNode.droppedEvents();

        call(f, 3);

        long emitted = 2 + 3 * 2;
        assertThat(f.engine().buffered(TENANT, I)).isEqualTo(4);
        assertThat(TelephonyNode.droppedEvents() - before).isEqualTo(emitted - 1 - 4);
        EngineFixture.RecordingSink again = new EngineFixture.RecordingSink();
        f.engine().watch(caller(), "r1", "bin-a", again);
        List<Long> replayed = again.sessionEvents().stream().map(Events.Delivery::seq).toList();
        assertThat(replayed).containsExactly(emitted - 3, emitted - 2, emitted - 1, emitted);
    }

    @Test
    void eventsOlderThanTheAgeLimitGo() throws Exception {
        EngineFixture f = new EngineFixture(new TelephonyNode.Limits(256, 10_000, 64L << 20));
        f.engine().watch(caller(), "r1", "bin-a", new EngineFixture.RecordingSink());
        call(f, 1);
        assertThat(f.engine().buffered(TENANT, I)).isEqualTo(3);

        f.clock.advance(12_000);

        assertThat(f.engine().buffered(TENANT, I)).isZero();
        assertThat(f.engine().bufferedBytes()).isZero();
    }

    @Test
    void theNodesBudgetHoldsAcrossSessions() throws Exception {
        long budget = 20_000;
        EngineFixture f = new EngineFixture(new TelephonyNode.Limits(256, 600_000, budget));
        EngineFixture.RecordingSink stuck = new EngineFixture.RecordingSink();
        stuck.stuck = true;
        f.engine().watch(caller(), "r1", "bin-a", stuck);
        long before = TelephonyNode.droppedEvents();
        for (int i = 0; i < 500; i++) {
            f.arrive(String.format("0192f6a1-7c3e-7b21-9a4d-%012d", i), String.format("0192f6a1-8000-7000-8000-%012d", 5000 + i));
        }
        assertThat(f.engine().bufferedBytes()).isGreaterThan(budget);

        f.clock.advance(2_000);

        assertThat(f.engine().bufferedBytes()).isLessThanOrEqualTo(budget);
        assertThat(TelephonyNode.droppedEvents() - before).isPositive();
    }

    @Test
    void aStreamThatHasNotDrainedGetsNoRedelivery() throws Exception {
        EngineFixture f = new EngineFixture(TelephonyNode.Limits.DEFAULT);
        EngineFixture.RecordingSink sink = new EngineFixture.RecordingSink();
        f.engine().watch(caller(), "r1", "bin-a", sink);
        call(f, 0);
        int first = sink.sessionEvents().size();
        sink.stuck = true;

        f.clock.advance(4_000);
        assertThat(sink.sessionEvents()).hasSize(first);

        sink.stuck = false;
        f.clock.advance(2_000);
        assertThat(sink.sessionEvents()).as("the answer, still unacknowledged, again").hasSize(first + 1);
    }
}
