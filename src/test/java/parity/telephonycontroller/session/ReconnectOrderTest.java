package parity.telephonycontroller.session;

import static org.assertj.core.api.Assertions.assertThat;
import static parity.telephonycontroller.session.EngineFixture.caller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import parity.telephonycontroller.model.Kinds.RemoteMediaKind;

/**
 * A reconnecting replica never gets a session's new event before its older unacknowledged ones (the
 * bug the stress test found): the event made while the replay is still on another session waits
 * for, and follows, its own session's replay.
 */
class ReconnectOrderTest {

    private static final String I1 = "0192f6a1-7c3e-7b21-9a4d-5e6f70819203";
    private static final String I2 = "0192f6a1-7c3e-7b21-9a4d-5e6f70819204";
    private static final String C1 = "0192f6a1-8000-7000-8000-000000000001";
    private static final String C2 = "0192f6a1-8000-7000-8000-000000000002";

    @Test
    void anEventMadeDuringTheReplayFollowsItsSessionsOlderEvents() throws Exception {
        EngineFixture f = new EngineFixture(TelephonyNode.Limits.DEFAULT);
        f.engine().watch(caller(), "r1", "bin-a", new EngineFixture.RecordingSink());
        for (String[] call : new String[][] {{I1, C1}, {I2, C2}}) {
            f.arrive(call[0], call[1]);
            f.claim(call[0], call[0].equals(I1) ? 1042 : 1062, "r1");
            f.engine().answer(caller(), EngineFixture.owner(call[0], call[0].equals(I1) ? 1042 : 1062, "r1"),
                    EngineFixture.key("answer-" + call[1]), call[1]);
        }
        // Each session holds its answer, unacknowledged. The new stream's first replayed message
        // triggers a carrier re-INVITE on whichever session the replay hasn't reached yet.
        EngineFixture.RecordingSink reconnect = new EngineFixture.RecordingSink() {
            private boolean fired;

            @Override
            public boolean send(Events.Delivery d) {
                boolean ok = super.send(d);
                if (!fired && d.session() != null) {
                    fired = true;
                    String other = d.session().interactionId().equals(I1) ? C2 : C1;
                    f.node.reinvite(other, RemoteMediaKind.ADDRESS);
                }
                return ok;
            }
        };
        f.engine().watch(caller(), "r1", "bin-a", reconnect);

        Map<String, List<Long>> seqs = new HashMap<>();
        for (Events.Delivery d : reconnect.sessionEvents()) {
            seqs.computeIfAbsent(d.session().interactionId(), k -> new ArrayList<>()).add(d.seq());
        }
        assertThat(seqs.get(I1)).isSorted();
        assertThat(seqs.get(I2)).isSorted();
        assertThat(seqs.values().stream().mapToInt(List::size).sum()).as("both answers and the re-INVITE").isEqualTo(3);
    }
}
