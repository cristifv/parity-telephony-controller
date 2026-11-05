package parity.telephonycontroller.session;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static parity.telephonycontroller.session.EngineFixture.caller;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.RepetitionInfo;
import parity.telephonycontroller.model.Kinds.EndCause;
import parity.telephonycontroller.model.Kinds.Endpoint;
import parity.telephonycontroller.model.Kinds.RemoteMediaKind;
import parity.telephonycontroller.model.Kinds.SessionState;
import parity.telephonycontroller.model.Kinds.Role;

/**
 * The per-session single writer under real concurrency (TC ADR 0001 §11, step 5 fix 1): races
 * X9–X12 run truly at once against one engine. In each round, six threads run at the same time:
 * the first owner's commands; an adopter that claims a higher epoch and commands; the far ends
 * (ring, answer, hangup, re-INVITE); the clock (timers, redelivery); Watch streams opening and
 * dropping (attach, supersede); and restarts (rebuild, finishRebuild). Two sessions share one pool
 * of leg ids.
 *
 * <p>After each round: no deadlock; one owner (per session, the epochs of successful commands in
 * the writer's order never go down); events in order per stream and session (rising seqs, a replay
 * identical, the fence they carry never going down); each leg's events in one session only; nothing
 * after a session's SessionEnded; no leg originated twice; one fence on every live leg, in the media
 * and in the engine.
 *
 * <p>TC ADR 0002 §9 (X11): the far ends also hang up the customers now and then, and the adopter
 * claims sessions that may have ended, which must answer with their tombstones.
 */
class SessionWriterStressTest {

    private static final String I1 = "0192f6a1-7c3e-7b21-9a4d-5e6f70819203";
    private static final String I2 = "0192f6a1-7c3e-7b21-9a4d-5e6f70819204";
    private static final String C1 = "0192f6a1-8000-7000-8000-000000000001";
    private static final String C2 = "0192f6a1-8000-7000-8000-000000000002";
    private static final List<String> POOL = List.of(
            "0192f6a1-8000-7000-8000-0000000000a1", "0192f6a1-8000-7000-8000-0000000000a2",
            "0192f6a1-8000-7000-8000-0000000000a3", "0192f6a1-8000-7000-8000-0000000000a4",
            "0192f6a1-8000-7000-8000-0000000000a5", "0192f6a1-8000-7000-8000-0000000000a6");
    private static final int OPS = 300;

    @RepeatedTest(40)
    void interleavingsKeepOneOwnerOneFenceAndOrder(RepetitionInfo info) throws Exception {
        long seed = new Random().nextLong() ^ info.getCurrentRepetition();
        try {
            round(seed);
        } catch (AssertionError | Exception e) {
            throw new AssertionError("seed " + seed + ": " + e.getMessage(), e);
        }
    }

    private void round(long seed) throws Exception {
        Random master = new Random(seed);
        EngineFixture f = new EngineFixture(TelephonyNode.Limits.DEFAULT);
        Map<String, List<Long>> committed = new HashMap<>();
        f.holder.observe((interaction, epoch, rpc) -> {
            synchronized (committed) {
                committed.computeIfAbsent(interaction, k -> new ArrayList<>()).add(epoch);
            }
        });
        List<EngineFixture.RecordingSink> sinks = Collections.synchronizedList(new ArrayList<>());
        f.engine().watch(caller(), "r1", "bin-a", sink(sinks));
        // Two answered calls owned by r1.
        answered(f, I1, C1, 1042);
        answered(f, I2, C2, 1062);

        AtomicInteger restarts = new AtomicInteger();
        CountDownLatch go = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(6);
        List<Future<?>> work = new ArrayList<>();
        work.add(pool.submit(actor(go, master.nextLong(), r -> owner(f, r, "r1", 1042, 1062))));
        work.add(pool.submit(actor(go, master.nextLong(), r -> adopter(f, r))));
        work.add(pool.submit(actor(go, master.nextLong(), r -> farEnds(f, r))));
        work.add(pool.submit(actor(go, master.nextLong(), r -> f.clock.advance(r.nextInt(2500)))));
        work.add(pool.submit(actor(go, master.nextLong(), r -> streams(f, r, sinks))));
        work.add(pool.submit(actor(go, master.nextLong(), r -> {
            if (r.nextInt(40) == 0) {
                f.holder.restart("inc-" + restarts.incrementAndGet() + "-" + r.nextInt(1000), 0);
            }
        })));
        go.countDown();
        pool.shutdown();
        if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
            StringBuilder dump = new StringBuilder();
            for (ThreadInfo t : ManagementFactory.getThreadMXBean().dumpAllThreads(true, true)) {
                dump.append(t);
            }
            pool.shutdownNow();
            fail("deadlock or livelock:\n" + dump);
        }
        for (Future<?> w : work) {
            w.get();
        }

        // One owner: per session, epochs of successful commands never go down in the writer's order.
        synchronized (committed) {
            committed.forEach((interaction, epochs) -> {
                for (int i = 1; i < epochs.size(); i++) {
                    assertThat(epochs.get(i)).as("%s: command %d after a higher epoch's", interaction, i)
                            .isGreaterThanOrEqualTo(epochs.get(i - 1));
                }
            });
        }
        // Events in order, the fence never going down, each leg in one session.
        Map<String, String> legSession = new HashMap<>();
        for (EngineFixture.RecordingSink s : List.copyOf(sinks)) {
            Map<String, Map<Long, Events.Delivery>> seen = new HashMap<>();
            Map<String, Long> fence = new HashMap<>();
            for (Events.Delivery d : s.sessionEvents()) {
                String key = d.incarnation() + "/" + d.session().interactionId();
                Map<Long, Events.Delivery> bySeq = seen.computeIfAbsent(key, k -> new HashMap<>());
                Events.Delivery earlier = bySeq.get(d.seq());
                if (earlier != null) {
                    assertThat(d).as("a replay of %s/%d is identical", key, d.seq()).isEqualTo(earlier);
                    continue;
                }
                long max = bySeq.keySet().stream().mapToLong(Long::longValue).max().orElse(0);
                assertThat(d.seq()).as("%s: delivery_seq rises", key).isGreaterThan(max);
                boolean afterEnd = bySeq.values().stream().anyMatch(e -> e.event() instanceof Events.SessionEnded);
                assertThat(afterEnd).as("%s: nothing after SessionEnded (seq %d: %s; before it: %s)", key, d.seq(),
                        d.event(), bySeq.values().stream().map(e -> e.seq() + "=" + e.event()).toList()).isFalse();
                bySeq.put(d.seq(), d);
                long f0 = fence.getOrDefault(key, 0L);
                assertThat(d.session().ownerEpoch()).as("%s: the fence an event carries", key).isGreaterThanOrEqualTo(f0);
                fence.put(key, d.session().ownerEpoch());
                if (d.event() instanceof Events.LegUpdate u) {
                    String other = legSession.putIfAbsent(u.legId(), d.session().interactionId());
                    assertThat(other == null ? d.session().interactionId() : other)
                            .as("leg %s's events in one session", u.legId()).isEqualTo(d.session().interactionId());
                }
            }
        }
        assertThat(f.node.invariantViolations()).as("no leg originated twice").isEmpty();
        assertThat(f.engine().invariantViolations()).as("one fence on every live leg and the media").isEmpty();
        f.engine().close();
    }

    private static EngineFixture.RecordingSink sink(List<EngineFixture.RecordingSink> sinks) {
        EngineFixture.RecordingSink s = new EngineFixture.RecordingSink();
        sinks.add(s);
        return s;
    }

    private static void answered(EngineFixture f, String interaction, String leg, long epoch) throws Exception {
        f.arrive(interaction, leg);
        f.claim(interaction, epoch, "r1");
        f.engine().answer(caller(), EngineFixture.owner(interaction, epoch, "r1"), EngineFixture.key("answer-" + leg), leg);
    }

    private static Runnable actor(CountDownLatch go, long seed, IntConsumerWithRandom op) {
        return () -> {
            Random r = new Random(seed);
            try {
                go.await();
                for (int i = 0; i < OPS; i++) {
                    op.accept(r);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };
    }

    @FunctionalInterface
    private interface IntConsumerWithRandom {
        void accept(Random r);
    }

    private static final AtomicInteger COMMANDS = new AtomicInteger();

    /** The first owner: AddLeg and RemoveLeg on both sessions, with the pool's leg ids. */
    private static void owner(EngineFixture f, Random r, String replica, long e1, long e2) {
        String interaction = r.nextBoolean() ? I1 : I2;
        long epoch = interaction.equals(I1) ? e1 : e2;
        command(f, r, interaction, epoch, replica);
    }

    /**
     * The adopter: once, it takes I1 over (E 1052, r2); then it commands with its epoch. Now and then
     * it claims I2 too (E 1072), which may have ended: a tombstone's claim answers ENDED.
     */
    private static void adopter(EngineFixture f, Random r) {
        if (r.nextInt(40) == 0) {
            f.claim(I2, 1072, "r2");
            try {
                Views.Claimed c = f.engine().claim(caller(), EngineFixture.owner(I2, 1072, "r2"));
                assertThat(c.session().state()).isIn(SessionState.ACTIVE, SessionState.ENDED);
            } catch (TelephonyException e) {
                // rebuilding, stale (a restart re-read an older record), or released: tried again later.
            }
            return;
        }
        if (r.nextInt(30) == 0) {
            f.claim(I1, 1052, "r2");
            try {
                f.engine().claim(caller(), EngineFixture.owner(I1, 1052, "r2"));
            } catch (TelephonyException e) {
                // rebuilding, or etcd's record replaced meanwhile: tried again later.
            }
        } else {
            command(f, r, I1, 1052, "r2");
        }
    }

    private static void command(EngineFixture f, Random r, String interaction, long epoch, String replica) {
        String leg = POOL.get(r.nextInt(POOL.size()));
        Requests.Owner o = EngineFixture.owner(interaction, epoch, replica);
        try {
            if (r.nextInt(3) > 0) {
                f.engine().addLeg(caller(), o, EngineFixture.key("cmd-" + COMMANDS.incrementAndGet()),
                        new Requests.AddLeg(leg, Role.AGENT, Endpoint.WEBRTC, 30, "fp-" + leg));
            } else {
                f.engine().removeLeg(caller(), o, EngineFixture.key("cmd-" + COMMANDS.incrementAndGet()),
                        new Requests.RemoveLeg(leg, EndCause.NORMAL));
            }
        } catch (TelephonyException e) {
            // stale_owner, owner_unverified, leg_conflict, leg_unknown, rebuilding...: expected here.
        }
    }

    /** The far ends of the agents' legs, and a carrier's re-INVITE. */
    private static void farEnds(EngineFixture f, Random r) {
        if (r.nextInt(60) == 0) {
            f.node.remoteEnd(r.nextBoolean() ? C1 : C2, EndCause.NORMAL, 0);
            return;
        }
        String leg = POOL.get(r.nextInt(POOL.size()));
        switch (r.nextInt(4)) {
            case 0 -> f.node.remoteRing(leg);
            case 1 -> f.node.remoteAnswer(leg);
            case 2 -> f.node.remoteEnd(leg, EndCause.NORMAL, 0);
            default -> f.node.reinvite(r.nextBoolean() ? C1 : C2, RemoteMediaKind.ADDRESS);
        }
    }

    /** Streams: a new one (superseding the replica's older one, replaying), or one dropped. */
    private static void streams(EngineFixture f, Random r, List<EngineFixture.RecordingSink> sinks) {
        String replica = r.nextBoolean() ? "r1" : "r2";
        try {
            if (r.nextInt(3) > 0) {
                f.engine().watch(caller(), replica, "bin-a", sink(sinks));
            } else {
                f.engine().dropStream(replica);
            }
        } catch (TelephonyException e) {
            // rebuilding
        }
    }
}
