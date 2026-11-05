package parity.telephonycontroller.session;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static parity.telephonycontroller.session.EngineFixture.TENANT;
import static parity.telephonycontroller.session.EngineFixture.caller;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import parity.telephonycontroller.journal.FileJournal;
import parity.telephonycontroller.journal.Journal;
import parity.telephonycontroller.model.Kinds.EndCause;
import parity.telephonycontroller.model.Kinds.Endpoint;
import parity.telephonycontroller.model.Kinds.LegState;
import parity.telephonycontroller.model.Kinds.Role;
import parity.telephonycontroller.node.NodePort;
import parity.telephonycontroller.node.SimulatedNode;
import parity.telephonycontroller.owner.InMemoryOwnerDirectory;
import parity.telephonycontroller.time.ManualClock;

/**
 * TC-036: a session that ends with no owner (a declined arrival, one past its 5 s deadline) keeps
 * a short-lived tombstone, capped per node; its end goes once to the offered replica and is never
 * replayed (not every 2 s, not to a restarted IE, not after a TC restart). A session that had an
 * owner still keeps its unacked tombstone the full 24 h, replayed until acked.
 */
class OwnerlessTombstoneTest {

    private static final String I = "0192f6a1-7c3e-7b21-9a4d-5e6f70819203";
    private static final String I2 = "0192f6a1-7c3e-7b21-9a4d-5e6f70819204";
    private static final String C = "0192f6a1-8000-7000-8000-000000000001";
    private static final String C2 = "0192f6a1-8000-7000-8000-000000000002";

    private static String iid(int i) {
        return String.format("0192f6a1-7c3e-7b21-9a4d-%012x", i);
    }

    private static String leg(int i) {
        return String.format("0192f6a1-8000-7000-8000-%012x", i);
    }

    private static long eventsFor(EngineFixture.RecordingSink sink, String interactionId) {
        return sink.sessionEvents().stream().filter(d -> d.session().interactionId().equals(interactionId)).count();
    }

    @Test
    void aFloodOfDeclinedAndUnclaimedArrivalsStaysBoundedAndIsReleased() throws Exception {
        int cap = 1_000;
        EngineFixture f = new EngineFixture(new TelephonyNode.Limits(256, 600_000, 64L << 20, cap, 120_000));
        EngineFixture.RecordingSink sink = new EngineFixture.RecordingSink();
        f.engine().watch(caller(), "r1", EngineFixture.BIN, sink);
        int perCycle = 250;
        int cycles = 40;
        int n = 0;
        int maxHeld = 0;
        long evictedBefore = TelephonyNode.ownerlessEvicted();
        for (int c = 0; c < cycles; c++) {
            // Spam to an unassigned number: IE declines it (404).
            for (int i = 0; i < perCycle; i++, n++) {
                f.arrive(iid(n), leg(n));
                f.engine().decline(caller(), new Requests.Decline(TENANT, iid(n), "r1", leg(n), 404));
            }
            // IE down or too slow: the arrival meets its 5 s deadline (503).
            for (int i = 0; i < perCycle; i++, n++) {
                f.arrive(iid(n), leg(n));
            }
            f.clock.advance(5_001);
            maxHeld = Math.max(maxHeld, f.engine().sessionCount());
            assertThat(f.engine().bufferedBytes()).as("nothing ownerless waits for an ack").isZero();
        }
        assertThat(n).isEqualTo(20_000);
        assertThat(maxHeld).as("capped: at most the cap plus one cycle's arrivals").isLessThanOrEqualTo(cap + perCycle);
        assertThat(TelephonyNode.ownerlessEvicted() - evictedBefore).isGreaterThan(0);
        // Each session's end went once, and nothing comes again on the 2 s redelivery.
        long sent = sink.sessionEvents().size();
        f.clock.advance(10_000);
        assertThat(sink.sessionEvents()).hasSize((int) sent);
        assertThat(sink.sessionEvents().stream().filter(d -> d.event() instanceof Events.SessionEnded).count())
                .isEqualTo(n);
        // The short life: two minutes after its end, every tombstone (and its journal records) is gone.
        f.clock.advance(120_000);
        assertThat(f.engine().sessionCount()).isZero();
        Journal.Contents j = f.journal.load();
        assertThat(j.released().stream().map(Journal.Released::interactionId).distinct().count()).isEqualTo(n);
    }

    @Test
    void anOwnerlessEndIsSentOnceAndNeverReplayedNotToARestartedReplicaNorAfterATcRestart() throws Exception {
        EngineFixture f = new EngineFixture(TelephonyNode.Limits.DEFAULT);
        EngineFixture.RecordingSink first = new EngineFixture.RecordingSink();
        f.engine().watch(caller(), "r1", EngineFixture.BIN, first);
        f.arrive(I, C);
        f.engine().decline(caller(), new Requests.Decline(TENANT, I, "r1", C, 404));
        assertThat(first.sessionEvents().stream().filter(d -> d.event() instanceof Events.SessionEnded)).hasSize(1);
        long before = eventsFor(first, I);
        f.clock.advance(10_000);
        assertThat(eventsFor(first, I)).as("no 2 s redelivery").isEqualTo(before);
        // IE restarted: a new stream for the same replica gets nothing of it.
        EngineFixture.RecordingSink restarted = new EngineFixture.RecordingSink();
        f.engine().watch(caller(), "r1", EngineFixture.BIN, restarted);
        f.clock.advance(4_000);
        assertThat(eventsFor(restarted, I)).isZero();
        // The tombstone answers a late GetSession while it lives.
        assertThat(f.engine().get(caller(), TENANT, I).state()).isEqualTo(parity.telephonycontroller.model.Kinds.SessionState.ENDED);
        // TC restarted: the rebuild doesn't send it again either, and still releases it on time.
        f.holder.restart("inc-2", 0);
        EngineFixture.RecordingSink afterTc = new EngineFixture.RecordingSink();
        f.engine().watch(caller(), "r1", EngineFixture.BIN, afterTc);
        f.clock.advance(4_000);
        assertThat(eventsFor(afterTc, I)).isZero();
        f.clock.advance(TelephonyNode.OWNERLESS_TOMBSTONE_MS);
        assertThatThrownBy(() -> f.engine().get(caller(), TENANT, I))
                .isInstanceOfSatisfying(TelephonyException.class, e -> assertThat(e.code()).isEqualTo(Code.SESSION_UNKNOWN));
    }

    @Test
    void anOwnedSessionsUnackedEndIsStillReplayedAndKeptUntilAcked() throws Exception {
        EngineFixture f = new EngineFixture(TelephonyNode.Limits.DEFAULT);
        EngineFixture.RecordingSink sink = new EngineFixture.RecordingSink();
        f.engine().watch(caller(), "r1", EngineFixture.BIN, sink);
        f.arrive(I2, C2);
        f.claim(I2, 1042, "r1");
        f.engine().answer(caller(), EngineFixture.owner(I2, 1042, "r1"), EngineFixture.key("a1"), C2);
        f.node.remoteEnd(C2, EndCause.NORMAL, 0);
        f.clock.advance(TelephonyNode.OWNERLESS_TOMBSTONE_MS + 1);
        // IE restarted before acking: the end comes again (an adopter's, step 8), the tombstone stays.
        EngineFixture.RecordingSink restarted = new EngineFixture.RecordingSink();
        f.engine().watch(caller(), "r1", EngineFixture.BIN, restarted);
        List<Events.Delivery> ends = restarted.sessionEvents().stream()
                .filter(d -> d.event() instanceof Events.SessionEnded).toList();
        assertThat(ends).hasSize(1);
        assertThat(f.engine().sessionCount()).isEqualTo(1);
        // Acked: it goes 10 minutes later, as before.
        f.engine().acknowledge(caller(), List.of(new Requests.Owner(TENANT, I2, 1042, "r1", ends.get(0).seq(), "inc-1")));
        f.clock.advance(TelephonyNode.TOMBSTONE_AFTER_ACK_MS + 1);
        assertThat(f.engine().sessionCount()).isZero();
    }

    /** Rebuilt from CDR files alone (its journal record lost), a tombstone's owner is unknown: 24 h. */
    @Test
    void aTombstoneRebuiltWithoutItsJournalRecordKeepsTheFullDay() throws Exception {
        EngineFixture f = new EngineFixture(TelephonyNode.Limits.DEFAULT);
        f.engine().watch(caller(), "r1", EngineFixture.BIN, new EngineFixture.RecordingSink());
        f.arrive(I2, C2);
        f.claim(I2, 1042, "r1");
        f.engine().answer(caller(), EngineFixture.owner(I2, 1042, "r1"), EngineFixture.key("a1"), C2);
        f.node.remoteEnd(C2, EndCause.NORMAL, 0);
        f.journal.clear();
        f.holder.restart("inc-2", 0);
        assertThat(f.engine().sessionCount()).as("rebuilt from the node's CDR").isEqualTo(1);
        f.clock.advance(TelephonyNode.OWNERLESS_TOMBSTONE_MS + 1);
        assertThat(f.engine().sessionCount()).as("not taken for ownerless").isEqualTo(1);
    }

    /** TC-041: a claim that raised an ownerless tombstone's fence keeps its short life across a restart. */
    @Test
    void aClaimRaisingAnOwnerlessTombstonesFenceKeepsItsShortLifeAcrossARestart() throws Exception {
        EngineFixture f = new EngineFixture(TelephonyNode.Limits.DEFAULT);
        f.engine().watch(caller(), "r1", EngineFixture.BIN, new EngineFixture.RecordingSink());
        f.arrive(I, C);
        f.clock.advance(5_001);
        f.claim(I, 1042, "r1");
        assertThat(f.engine().claim(caller(), EngineFixture.owner(I, 1042, "r1")).session().state())
                .isEqualTo(parity.telephonycontroller.model.Kinds.SessionState.ENDED);
        f.holder.restart("inc-2", 0);
        EngineFixture.RecordingSink after = new EngineFixture.RecordingSink();
        f.engine().watch(caller(), "r1", EngineFixture.BIN, after);
        f.clock.advance(TelephonyNode.OWNERLESS_TOMBSTONE_MS);
        assertThat(eventsFor(after, I)).as("not replayed after the restart").isZero();
        assertThatThrownBy(() -> f.engine().get(caller(), TENANT, I))
                .isInstanceOfSatisfying(TelephonyException.class, e -> assertThat(e.code()).isEqualTo(Code.SESSION_UNKNOWN));
    }

    /** The journal's live records (what its next compaction keeps) hold nothing of a released flood. */
    @Test
    void theJournalKeepsNothingOfAReleasedFloodButTheReleaseMarkers(@TempDir Path dir) throws Exception {
        ManualClock clock = new ManualClock(EngineFixture.START);
        SimulatedNode node = new SimulatedNode(clock, 1_000_000);
        InMemoryOwnerDirectory owners = new InMemoryOwnerDirectory();
        owners.register(EngineFixture.BIN, "r1");
        try (FileJournal journal = new FileJournal(dir)) {
            EngineHolder holder = new EngineHolder(new TelephonyNode.Config("media-1", "test", Set.of(EngineFixture.BIN),
                    "inc-1", List.of(), TelephonyNode.Limits.DEFAULT), clock, owners, journal, node);
            holder.start();
            holder.get().watch(caller(), "r1", EngineFixture.BIN, new EngineFixture.RecordingSink());
            int n = 2_000;
            for (int i = 0; i < n; i++) {
                node.park(new NodePort.Channel(leg(i), TENANT, iid(i), EngineFixture.BIN, Role.CUSTOMER, Endpoint.SIP,
                        leg(i) + "@carrier", true, clock.nowMs(), "+447700900100", "+447700900123", "carrier-a",
                        List.of(), null, LegState.PARKED));
                holder.get().decline(caller(), new Requests.Decline(TENANT, iid(i), "r1", leg(i), 404));
            }
            journal.flush();
            assertThat(journal.load().sessions()).hasSize(n);
            clock.advance(TelephonyNode.OWNERLESS_TOMBSTONE_MS + 1);
            journal.flush();
            Journal.Contents live = journal.load();
            assertThat(live.legs()).isEmpty();
            assertThat(live.sessions()).isEmpty();
            assertThat(live.released()).hasSize(n);
            assertThat(holder.get().sessionCount()).isZero();
            try (var files = Files.list(dir)) {
                assertThat(files.count()).isPositive();
            }
        }
    }
}
