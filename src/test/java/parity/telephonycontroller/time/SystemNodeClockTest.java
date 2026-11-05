package parity.telephonycontroller.time;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** TC-039: a cancelled timer leaves the scheduler at once, so it no longer holds what it captured. */
class SystemNodeClockTest {

    @Test
    void cancelledTimersLeaveTheQueueAtOnceAndLiveOnesStillRun() throws Exception {
        try (SystemNodeClock clock = new SystemNodeClock()) {
            List<NodeClock.Timer> timers = new ArrayList<>();
            long day = clock.nowMs() + 86_400_000;
            for (int i = 0; i < 10_000; i++) {
                byte[] held = new byte[64];
                timers.add(clock.schedule(day, () -> held[0]++));
            }
            assertThat(clock.pending()).isEqualTo(10_000);
            timers.forEach(NodeClock.Timer::cancel);
            assertThat(clock.pending()).as("a tombstone's cancelled 24 h release is not kept").isZero();
            CountDownLatch ran = new CountDownLatch(1);
            clock.schedule(clock.nowMs() + 10, ran::countDown);
            assertThat(ran.await(5, TimeUnit.SECONDS)).isTrue();
        }
    }
}
