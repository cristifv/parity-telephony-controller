package parity.telephonycontroller.session;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static parity.telephonycontroller.session.EngineFixture.TENANT;
import static parity.telephonycontroller.session.EngineFixture.caller;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import parity.telephonycontroller.model.Kinds.EndCause;
import parity.telephonycontroller.model.Kinds.Endpoint;
import parity.telephonycontroller.model.Kinds.LegState;
import parity.telephonycontroller.model.Kinds.Role;
import parity.telephonycontroller.model.Kinds.SessionState;

/**
 * The engine's step-3 rules (TC ADR 0002 §2–§3): provisional sessions instead of the node-wide
 * creation lock; an interaction id that already has a session; a dropped SessionEnded that counts
 * as acknowledged; attempts past their expiry doing nothing.
 */
class EngineStep3Test {

    private static final String I = "0192f6a1-7c3e-7b21-9a4d-5e6f70819203";
    private static final String I2 = "0192f6a1-7c3e-7b21-9a4d-5e6f70819204";
    private static final String C = "0192f6a1-8000-7000-8000-000000000001";
    private static final String C2 = "0192f6a1-8000-7000-8000-000000000002";
    private static final String A = "0192f6a1-8000-7000-8000-0000000000a1";

    private static Requests.AddLeg agent(String leg) {
        return new Requests.AddLeg(leg, Role.AGENT, Endpoint.WEBRTC, 30, "fp-" + leg);
    }

    @Test
    void twoCreatorsOfOneSessionQueueInOneMailboxAndOriginateOnce() throws Exception {
        EngineFixture f = new EngineFixture(TelephonyNode.Limits.DEFAULT);
        f.claim(I2, 1042, "r1");
        CountDownLatch go = new CountDownLatch(1);
        List<AtomicReference<Object>> results = new ArrayList<>();
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            AtomicReference<Object> r = new AtomicReference<>();
            results.add(r);
            String cmd = "create-" + i;
            threads.add(Thread.ofVirtual().start(() -> {
                try {
                    go.await();
                    r.set(f.engine().addLeg(caller(), EngineFixture.owner(I2, 1042, "r1"), EngineFixture.key(cmd),
                            new Requests.AddLeg(A, Role.CUSTOMER, Endpoint.SIP, 30, "fp")));
                } catch (Exception e) {
                    r.set(e);
                }
            }));
        }
        go.countDown();
        for (Thread t : threads) {
            t.join();
        }
        List<Boolean> existing = results.stream().map(r -> ((Views.LegResult) r.get()).existing()).toList();
        assertThat(existing).containsExactlyInAnyOrder(false, true);
        assertThat(f.node.invariantViolations()).isEmpty();
        assertThat(f.engine().get(caller(), TENANT, I2).legs()).hasSize(1);
    }

    @Test
    void aCreatorThatFailsRetiresItsProvisionalSession() throws Exception {
        EngineFixture f = new EngineFixture(TelephonyNode.Limits.DEFAULT);
        f.claim(I2, 1042, "r1");
        f.owners.setAvailable(false);
        assertThatThrownBy(() -> f.engine().addLeg(caller(), EngineFixture.owner(I2, 1042, "r1"),
                EngineFixture.key("c1"), new Requests.AddLeg(A, Role.CUSTOMER, Endpoint.SIP, 30, "fp")))
                .isInstanceOfSatisfying(TelephonyException.class, e -> assertThat(e.code()).isEqualTo(Code.OWNER_CHECK_UNAVAILABLE));
        assertThatThrownBy(() -> f.engine().get(caller(), TENANT, I2))
                .isInstanceOfSatisfying(TelephonyException.class, e -> assertThat(e.code()).isEqualTo(Code.SESSION_UNKNOWN));
        f.owners.setAvailable(true);
        Views.LegResult r = f.engine().addLeg(caller(), EngineFixture.owner(I2, 1042, "r1"), EngineFixture.key("c1"),
                new Requests.AddLeg(A, Role.CUSTOMER, Endpoint.SIP, 30, "fp"));
        assertThat(r.existing()).isFalse();
        assertThat(r.state()).isEqualTo(LegState.ORIGINATING);
    }

    @Test
    void anotherLegForAnInteractionWithASessionGets503AndTheSameLegChangesNothing() throws Exception {
        EngineFixture f = new EngineFixture(TelephonyNode.Limits.DEFAULT);
        EngineFixture.RecordingSink sink = new EngineFixture.RecordingSink();
        f.engine().watch(caller(), "r1", "bin-a", sink);
        long before = TelephonyNode.duplicateArrivals();
        f.arrive(I, C);
        f.arrive(I, C);
        f.arrive(I, C2);
        assertThat(TelephonyNode.duplicateArrivals()).isEqualTo(before + 1);
        assertThat(f.node.cdrs(0)).singleElement().satisfies(e -> {
            assertThat(e.legId()).isEqualTo(C2);
            assertThat(e.sipStatus()).isEqualTo(503);
        });
        assertThat(f.engine().get(caller(), TENANT, I).legs()).extracting(Views.LegView::legId).containsExactly(C);
        assertThat(sink.sessionEvents()).hasSize(1);
    }

    @Test
    void anArrivalForAnUnknownBinGets503AndNoSession() throws Exception {
        EngineFixture f = new EngineFixture(TelephonyNode.Limits.DEFAULT);
        f.node.park(new parity.telephonycontroller.node.NodePort.Channel(C, TENANT, I, "bin-x", Role.CUSTOMER,
                Endpoint.SIP, "x@carrier", true, f.clock.nowMs(), "+447700900100", "+447700900123", "carrier-a",
                List.of(), null, LegState.PARKED));
        assertThat(f.node.cdrs(0)).singleElement().satisfies(e -> assertThat(e.sipStatus()).isEqualTo(503));
        assertThatThrownBy(() -> f.engine().get(caller(), TENANT, I)).isInstanceOf(TelephonyException.class);
    }

    @Test
    void anAcceptedArrivalCarriesTheSessionsToken() throws Exception {
        EngineFixture f = new EngineFixture(TelephonyNode.Limits.DEFAULT);
        f.arrive(I, C);
        String token = f.node.channel(C).orElseThrow().sessionToken;
        assertThat(token).isNotBlank();
        assertThat(parity.telephonycontroller.model.UuidV7.unixMs(token)).isPositive();
    }

    @Test
    void aDroppedSessionEndedCountsAsAcknowledgedOnceTheAckPassesIt() throws Exception {
        // Events older than 1 s are dropped; nothing is acknowledged until after the end.
        EngineFixture f = new EngineFixture(new TelephonyNode.Limits(256, 1_000, 64L << 20));
        EngineFixture.RecordingSink sink = new EngineFixture.RecordingSink();
        f.engine().watch(caller(), "r1", "bin-a", sink);
        f.arrive(I, C);
        f.claim(I, 1042, "r1");
        f.engine().answer(caller(), EngineFixture.owner(I, 1042, "r1"), EngineFixture.key("a"), C);
        f.node.remoteEnd(C, EndCause.NORMAL, 0);
        long endSeq = sink.sessionEvents().stream().filter(d -> d.event() instanceof Events.SessionEnded)
                .mapToLong(Events.Delivery::seq).max().orElseThrow();
        f.clock.advance(3_000);
        assertThat(f.engine().buffered(TENANT, I)).as("the end was dropped by age").isZero();
        // The owner acknowledges the end's seq (it saw it before the drop); the tombstone goes 10 min later.
        f.engine().acknowledge(caller(), List.of(new Requests.Owner(TENANT, I, 1042, "r1", endSeq, "inc-1")));
        f.clock.advance(TelephonyNode.TOMBSTONE_AFTER_ACK_MS + 1);
        assertThatThrownBy(() -> f.engine().get(caller(), TENANT, I))
                .isInstanceOfSatisfying(TelephonyException.class, e -> assertThat(e.code()).isEqualTo(Code.SESSION_UNKNOWN));
    }

    @Test
    void anAttemptPastItsExpiryDoesNothing() throws Exception {
        EngineFixture f = new EngineFixture(TelephonyNode.Limits.DEFAULT);
        f.arrive(I, C);
        f.claim(I, 1042, "r1");
        f.engine().answer(caller(), EngineFixture.owner(I, 1042, "r1"), EngineFixture.key("a"), C);
        long now = f.clock.nowMs();
        // The header's expiry, less the skew budget, has passed.
        Requests.CommandKey late = new Requests.CommandKey("add", "AddLeg", "fp",
                new Requests.Attempt(now + 200, Long.MAX_VALUE));
        assertThatThrownBy(() -> f.engine().addLeg(caller(), EngineFixture.owner(I, 1042, "r1"), late, agent(A)))
                .isInstanceOfSatisfying(TelephonyException.class, e -> assertThat(e.code()).isEqualTo(Code.ATTEMPT_EXPIRED));
        // gRPC's own deadline has passed.
        Requests.CommandKey overdue = new Requests.CommandKey("add", "AddLeg", "fp",
                new Requests.Attempt(0, System.nanoTime() - 1));
        assertThatThrownBy(() -> f.engine().addLeg(caller(), EngineFixture.owner(I, 1042, "r1"), overdue, agent(A)))
                .isInstanceOfSatisfying(TelephonyException.class, e -> assertThat(e.code()).isEqualTo(Code.ATTEMPT_EXPIRED));
        assertThat(f.node.channel(A)).as("never originated").isEmpty();
        // The retry (same command id, fresh expiry) runs.
        Requests.CommandKey fresh = new Requests.CommandKey("add", "AddLeg", "fp",
                new Requests.Attempt(now + 1_000, Long.MAX_VALUE));
        assertThat(f.engine().addLeg(caller(), EngineFixture.owner(I, 1042, "r1"), fresh, agent(A)).existing()).isFalse();
        assertThat(f.node.channel(A)).isPresent();
        assertThat(f.engine().get(caller(), TENANT, I).state()).isEqualTo(SessionState.ACTIVE);
    }

    @Test
    void aLateReinviteOnAnEndedLegEmitsNothingAfterSessionEnded() throws Exception {
        EngineFixture f = new EngineFixture(TelephonyNode.Limits.DEFAULT);
        EngineFixture.RecordingSink sink = new EngineFixture.RecordingSink();
        f.engine().watch(caller(), "r1", "bin-a", sink);
        f.arrive(I, C);
        f.claim(I, 1042, "r1");
        f.engine().answer(caller(), EngineFixture.owner(I, 1042, "r1"), EngineFixture.key("a"), C);
        f.node.remoteEnd(C, EndCause.NORMAL, 0);
        int before = sink.sessionEvents().size();
        assertThat(sink.sessionEvents().get(before - 1).event()).isInstanceOf(Events.SessionEnded.class);
        f.node.reinvite(C, parity.telephonycontroller.model.Kinds.RemoteMediaKind.ADDRESS);
        assertThat(sink.sessionEvents()).hasSize(before);
    }

    @Test
    void aTombstonesRaisedFenceSurvivesARestart() throws Exception {
        EngineFixture f = new EngineFixture(TelephonyNode.Limits.DEFAULT);
        f.arrive(I, C);
        f.claim(I, 1042, "r1");
        f.engine().answer(caller(), EngineFixture.owner(I, 1042, "r1"), EngineFixture.key("a"), C);
        f.node.remoteEnd(C, EndCause.NORMAL, 0);
        // r2 adopts the ended session: the claim answers its tombstone and raises the fence.
        f.claim(I, 1052, "r2");
        assertThat(f.engine().claim(caller(), EngineFixture.owner(I, 1052, "r2")).session().state())
                .isEqualTo(SessionState.ENDED);
        f.holder.restart("inc-2", 0);
        // The older owner's no-op command must not commit after the restart.
        assertThatThrownBy(() -> f.engine().removeLeg(caller(), EngineFixture.owner(I, 1042, "r1"),
                EngineFixture.key("late"), new Requests.RemoveLeg(C, EndCause.NORMAL)))
                .isInstanceOfSatisfying(TelephonyException.class, e -> assertThat(e.code()).isEqualTo(Code.STALE_OWNER));
    }

    @Test
    void aSessionThatEndsWhileTheRebuildFinishesSendsOneEnd() throws Exception {
        EngineFixture f = new EngineFixture(TelephonyNode.Limits.DEFAULT);
        f.arrive(I, C);
        f.claim(I, 1042, "r1");
        f.engine().answer(caller(), EngineFixture.owner(I, 1042, "r1"), EngineFixture.key("a"), C);
        f.holder.restart("inc-2", 500);
        EngineFixture.RecordingSink sink = new EngineFixture.RecordingSink();
        f.engine().watch(caller(), "r1", "bin-a", sink);
        // The caller hangs up after the snapshot, before the rebuild has finished.
        f.node.remoteEnd(C, EndCause.NORMAL, 0);
        f.clock.advance(600);
        long ends = sink.sessionEvents().stream().filter(d -> d.event() instanceof Events.SessionEnded).count();
        assertThat(ends).isEqualTo(1);
    }
}
