package parity.telephonycontroller.esl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The ESL client against fake-freeswitch (TC ADR 0002 §1, R1-3): the cycle and subscription, replies
 * matched in order, jobs, a timed-out command's slot, fail-fast when busy, a stall shorter than the
 * probe deadline (no reconnect), a dead connection and a freeswitch restart (reconnect, new
 * {@code Core-UUID}). Timings are scaled down: probes every 100 ms, presumed dead after 1 s.
 */
class EslClientTest {

    private FakeFreeswitch fs;
    private EslClient client;
    private final Recorder handler = new Recorder();

    /** What the client reported. */
    static final class Recorder implements EslClient.Handler {
        final List<String> calls = new CopyOnWriteArrayList<>();
        final List<NodeEvent> events = new CopyOnWriteArrayList<>();
        final AtomicInteger ups = new AtomicInteger();

        @Override
        public void connecting() {
            calls.add("connecting");
        }

        @Override
        public void up(String coreUuid) {
            calls.add("up " + coreUuid);
            ups.incrementAndGet();
        }

        @Override
        public void event(NodeEvent e) {
            events.add(e);
        }

        @Override
        public void down() {
            calls.add("down");
        }
    }

    @BeforeEach
    void start() throws Exception {
        fs = new FakeFreeswitch("secret");
        client = new EslClient(config(32));
        client.start(handler);
        assertThat(client.awaitLive(Duration.ofSeconds(5))).isTrue();
    }

    private EslClient.Config config(int pipeline) {
        return new EslClient.Config("127.0.0.1", fs.port(), "secret", 2, pipeline, Duration.ofSeconds(2),
                Duration.ofMillis(300), Duration.ofMillis(100), Duration.ofSeconds(1), Duration.ofMillis(20),
                Duration.ofMillis(200));
    }

    @AfterEach
    void stop() {
        client.close();
        fs.close();
    }

    @Test
    void aCycleSubscribesBeforeItIsUpAndReportsTheCoreUuid() {
        List<String> seen = fs.commandsSeen();
        assertThat(seen).first().isEqualTo("auth ***");
        assertThat(seen).anyMatch(l -> l.startsWith("event json CHANNEL_PARK"));
        assertThat(seen).contains("filter variable_cc_managed true", "filter Application -set");
        assertThat(seen.indexOf("api global_getvar core_uuid"))
                .isGreaterThan(seen.indexOf("filter variable_cc_managed true"));
        assertThat(handler.calls).containsExactly("connecting", "up " + fs.coreUuid());
        assertThat(client.coreUuid()).isEqualTo(fs.coreUuid());
    }

    @Test
    void apiAndSendmsgReplies() throws Exception {
        fs.onApi(cmd -> cmd.equals("uuid_exists x") ? "true" : "-ERR no");
        assertThat(client.api("uuid_exists x")).isEqualTo("true");
        List<String> bodies = new ArrayList<>();
        fs.onSendmsg((uuid, c) -> {
            bodies.add(uuid + "|" + c.header("execute-app-name") + "|" + c.bodyText());
            return "+OK";
        });
        assertThat(client.execute("u1", "set", "switch_r_sdp=v=0\r\no=- 1 2 IN IP4 127.0.0.1\r\n")).isEqualTo("+OK");
        assertThat(bodies).containsExactly("u1|set|switch_r_sdp=v=0\r\no=- 1 2 IN IP4 127.0.0.1\r\n");
    }

    @Test
    void aJobCompletesFromItsEventEvenWhenTheEventOvertakesTheReply() throws Exception {
        fs.jobEventBeforeReply(true);
        fs.onApi(cmd -> cmd.startsWith("originate") ? "+OK 0192f6a1-0000-7000-8000-00000000000a" : "-ERR");
        CompletableFuture<String> job = client.bgapi("originate loopback/x &park()");
        assertThat(job.get(2, TimeUnit.SECONDS)).isEqualTo("+OK 0192f6a1-0000-7000-8000-00000000000a");
    }

    @Test
    void aTimedOutCommandKeepsItsSlotAndItsLateReplyIsNotTheNextCommands() throws Exception {
        fs.onApi(cmd -> cmd.equals("slow") ? "slow-answer" : "fast-answer");
        fs.delayReplies("api slow", 600);
        assertThatThrownBy(() -> client.api("slow")).isInstanceOf(EslClient.Timeout.class);
        // Every command connection gets one (the slow one's queue is the least loaded only once).
        List<String> answers = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            answers.add(client.api("fast"));
        }
        assertThat(answers).containsOnly("fast-answer");
        Thread.sleep(800);
        // The slow reply, and any probe held behind it on the same connection, came late.
        assertThat(client.lateReplies()).isGreaterThanOrEqualTo(1);
        assertThat(client.reconnects()).isZero();
        assertThat(client.api("fast")).isEqualTo("fast-answer");
    }

    @Test
    void everyConnectionAtItsLimitFailsAtOnce() throws Exception {
        client.close();
        client = new EslClient(config(1));
        client.start(handler);
        assertThat(client.awaitLive(Duration.ofSeconds(5))).isTrue();
        fs.onApi(cmd -> "x");
        fs.delayReplies("api hold", 2_000);
        // Two connections, one slot each: two holds fill them.
        Thread a = Thread.ofVirtual().start(() -> swallow(() -> client.api("hold")));
        Thread b = Thread.ofVirtual().start(() -> swallow(() -> client.api("hold")));
        Thread.sleep(150);
        long t0 = System.nanoTime();
        assertThatThrownBy(() -> client.api("other")).isInstanceOf(EslClient.Unavailable.class);
        assertThat(Duration.ofNanos(System.nanoTime() - t0)).isLessThan(Duration.ofMillis(100));
        a.join();
        b.join();
    }

    @Test
    void aStallShorterThanTheProbeDeadlineReconnectsNothing() throws Exception {
        fs.onApi(cmd -> "ok");
        fs.stall();
        Thread.sleep(600);
        assertThatThrownBy(() -> client.api("during")).isInstanceOf(EslClient.Timeout.class);
        fs.resume();
        Thread.sleep(400);
        assertThat(client.reconnects()).isZero();
        assertThat(handler.calls).doesNotContain("down");
        assertThat(client.api("after")).isEqualTo("ok");
        // The command and the probes sent during the stall: answered late, consumed in their slots.
        assertThat(client.lateReplies()).isGreaterThanOrEqualTo(1);
    }

    @Test
    void aStallPastTheDeadlineIsADeadConnection() throws Exception {
        fs.stall();
        Thread.sleep(1_600);
        fs.resume();
        assertThat(waitFor(() -> handler.ups.get() >= 2, 5_000)).isTrue();
        assertThat(handler.calls).containsSubsequence("up " + fs.coreUuid(), "down", "connecting", "up " + fs.coreUuid());
        assertThat(client.reconnects()).isGreaterThanOrEqualTo(1);
    }

    @Test
    void aRestartGivesANewCoreUuid() throws Exception {
        String before = fs.coreUuid();
        fs.restart();
        assertThat(waitFor(() -> handler.ups.get() >= 2, 5_000)).isTrue();
        assertThat(client.coreUuid()).isNotEqualTo(before).isEqualTo(fs.coreUuid());
    }

    @Test
    void eventsArriveInOrderAndProbesAndJobsAreNotPassedOn() throws Exception {
        for (int i = 0; i < 50; i++) {
            fs.fire(Map.of("Event-Name", "CHANNEL_PARK", "Unique-ID", "u" + i, "variable_cc_managed", "true"));
        }
        assertThat(waitFor(() -> handler.events.size() >= 50, 3_000)).isTrue();
        Thread.sleep(300);
        List<String> ids = handler.events.stream().map(NodeEvent::uuid).toList();
        assertThat(ids).hasSize(50);
        for (int i = 0; i < 50; i++) {
            assertThat(ids.get(i)).isEqualTo("u" + i);
        }
        assertThat(handler.events).noneMatch(e -> e.subclass().equals("parity::probe"));
        assertThat(client.probeLagNanos()).isPositive();
    }

    @Test
    void aWrongPasswordNeverGoesLive() throws Exception {
        client.close();
        client = new EslClient(new EslClient.Config("127.0.0.1", fs.port(), "wrong", 1, 4, Duration.ofSeconds(1),
                Duration.ofMillis(300), Duration.ofMillis(100), Duration.ofSeconds(1), Duration.ofMillis(20),
                Duration.ofMillis(100)));
        client.start(handler);
        assertThat(client.awaitLive(Duration.ofMillis(800))).isFalse();
        assertThatThrownBy(() -> client.api("x")).isInstanceOf(EslClient.Unavailable.class);
    }

    interface Action {
        void run() throws Exception;
    }

    private static void swallow(Action a) {
        try {
            a.run();
        } catch (Exception e) {
            // expected for the hold commands
        }
    }

    private static boolean waitFor(java.util.function.BooleanSupplier c, long ms) throws InterruptedException {
        long until = System.currentTimeMillis() + ms;
        while (System.currentTimeMillis() < until) {
            if (c.getAsBoolean()) {
                return true;
            }
            Thread.sleep(20);
        }
        return c.getAsBoolean();
    }
}
