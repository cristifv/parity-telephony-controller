package parity.telephonycontroller.session;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * A session's mailbox (TC ADR 0002 §2.1): one turn at a time, in queue order; the caller drains an
 * idle mailbox itself and hands the rest on; a nested call runs at once; bounded turns are refused
 * when full, the engine's own never are.
 */
class MailboxTest {

    @Test
    void anIdleMailboxRunsTheTurnOnTheCallersThread() throws Exception {
        Mailbox m = new Mailbox();
        Thread caller = Thread.currentThread();
        Thread ran = m.call(Thread::currentThread, true);
        assertThat(ran).isSameAs(caller);
    }

    @Test
    void turnsNeverOverlapAndKeepEachProducersOrder() throws Exception {
        Mailbox m = new Mailbox();
        AtomicInteger inside = new AtomicInteger();
        AtomicBoolean overlap = new AtomicBoolean();
        List<String> order = Collections.synchronizedList(new ArrayList<>());
        int producers = 8;
        int each = 200;
        CountDownLatch go = new CountDownLatch(1);
        List<Thread> threads = new ArrayList<>();
        for (int p = 0; p < producers; p++) {
            int id = p;
            threads.add(Thread.ofVirtual().start(() -> {
                try {
                    go.await();
                    for (int i = 0; i < each; i++) {
                        int n = i;
                        Mailbox.Turn<Void> t = () -> {
                            if (inside.incrementAndGet() > 1) {
                                overlap.set(true);
                            }
                            order.add(id + ":" + n);
                            inside.decrementAndGet();
                            return null;
                        };
                        if (n % 2 == 0) {
                            m.call(t, false);
                        } else {
                            m.post(t, false);
                        }
                    }
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }));
        }
        go.countDown();
        for (Thread t : threads) {
            t.join();
        }
        long until = System.currentTimeMillis() + 5_000;
        while (order.size() < producers * each && System.currentTimeMillis() < until) {
            Thread.sleep(10);
        }
        assertThat(overlap).isFalse();
        assertThat(order).hasSize(producers * each);
        for (int p = 0; p < producers; p++) {
            String prefix = p + ":";
            List<Integer> mine = order.stream().filter(s -> s.startsWith(prefix))
                    .map(s -> Integer.parseInt(s.substring(prefix.length()))).toList();
            assertThat(mine).as("producer %d's turns in its order", p).isSorted();
        }
    }

    @Test
    void aNestedCallRunsAtOnce() throws Exception {
        Mailbox m = new Mailbox();
        List<String> order = new ArrayList<>();
        m.call(() -> {
            order.add("outer");
            try {
                m.call(() -> order.add("inner"), true);
            } catch (Mailbox.Busy e) {
                throw new IllegalStateException(e);
            }
            order.add("outer again");
            return null;
        }, true);
        assertThat(order).containsExactly("outer", "inner", "outer again");
    }

    @Test
    void aFullMailboxRefusesBoundedTurnsOnly() throws Exception {
        Mailbox m = new Mailbox();
        CountDownLatch inTurn = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        m.post(() -> {
            inTurn.countDown();
            await(release);
            return null;
        }, false);
        assertThat(inTurn.await(2, TimeUnit.SECONDS)).isTrue();
        m.capacity(2);
        m.post(() -> null, true);
        m.post(() -> null, true);
        assertThatThrownBy(() -> m.post(() -> null, true)).isInstanceOf(Mailbox.Busy.class);
        assertThatThrownBy(() -> m.call(() -> null, true)).isInstanceOf(Mailbox.Busy.class);
        m.post(() -> null, false);
        assertThat(m.queued()).isEqualTo(3);
        release.countDown();
        long until = System.currentTimeMillis() + 2_000;
        while (m.queued() > 0 && System.currentTimeMillis() < until) {
            Thread.sleep(5);
        }
        assertThat(m.queued()).isZero();
        m.capacity(Mailbox.CAPACITY);
        assertThat(m.call(() -> "served", true)).isEqualTo("served");
    }

    @Test
    void aCallerServesItsOwnTurnThenHandsTheRestOn() throws Exception {
        Mailbox m = new Mailbox();
        List<Thread> ranOn = Collections.synchronizedList(new ArrayList<>());
        Thread me = Thread.currentThread();
        // The caller drains the idle mailbox; its turn queues 50 more; it returns once its own is done.
        m.call(() -> {
            for (int i = 0; i < 50; i++) {
                try {
                    m.post(() -> {
                        ranOn.add(Thread.currentThread());
                        return null;
                    }, false);
                } catch (Mailbox.Busy e) {
                    throw new IllegalStateException(e);
                }
            }
            return null;
        }, true);
        long until = System.currentTimeMillis() + 2_000;
        while (ranOn.size() < 50 && System.currentTimeMillis() < until) {
            Thread.sleep(5);
        }
        assertThat(ranOn).hasSize(50).doesNotContain(me);
    }

    @Test
    void aTurnsExceptionReachesItsCaller() {
        Mailbox m = new Mailbox();
        assertThatThrownBy(() -> m.call(() -> {
            throw new TelephonyException(Code.LEG_UNKNOWN);
        }, true)).isInstanceOf(TelephonyException.class);
        assertThatThrownBy(() -> m.call(() -> {
            throw new IllegalStateException("x");
        }, true)).isInstanceOf(IllegalStateException.class);
    }

    private static void await(CountDownLatch l) {
        try {
            l.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
