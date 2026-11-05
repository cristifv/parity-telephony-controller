package parity.telephonycontroller.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Incarnations are UUIDv7 (telephony.proto, {@code WatchResponse.incarnation}): a later run's sorts
 * higher, even when the earlier one drew the largest random bits and the later one the smallest.
 * Random UUIDv4s, which TC made before, sort at random.
 */
class UuidV7Test {

    @Test
    void itIsVersion7WithTheRfcVariantAndCarriesItsMilliseconds() {
        long ms = 1_793_000_000_123L;
        String id = UuidV7.create(ms, new SplittableRandom(1));
        UUID u = UUID.fromString(id);
        assertThat(u.version()).isEqualTo(7);
        assertThat(u.variant()).isEqualTo(2);
        assertThat(UuidV7.unixMs(id)).isEqualTo(ms);
        assertThat(id).isEqualTo(id.toLowerCase(java.util.Locale.ROOT)).hasSize(36);
    }

    @Test
    void aLaterMillisecondSortsHigherWhateverTheRandomBits() {
        long ms = 1_793_000_000_000L;
        String earlierWithAllOnes = UuidV7.create(ms, new FixedRandom(-1L));
        String laterWithAllZeros = UuidV7.create(ms + 1, new FixedRandom(0L));
        assertThat(laterWithAllZeros).isGreaterThan(earlierWithAllOnes);
    }

    @Test
    void textOrderIsTimeOrderAcrossManyRuns() {
        SplittableRandom r = new SplittableRandom(7);
        List<String> ids = new ArrayList<>();
        long ms = 1_700_000_000_000L;
        for (int i = 0; i < 2_000; i++) {
            ms += 1 + r.nextInt(5_000);
            ids.add(UuidV7.create(ms, r));
        }
        assertThat(ids).isSorted();
    }

    @Test
    void theProductionGeneratorMakesUuidV7ForNow() {
        long before = System.currentTimeMillis();
        String id = UuidV7.create();
        long after = System.currentTimeMillis();
        assertThat(UuidV7.unixMs(id)).isBetween(before, after);
    }

    @Test
    void otherVersionsAndOutOfRangeTimesAreRefused() {
        assertThatThrownBy(() -> UuidV7.unixMs(UUID.randomUUID().toString()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> UuidV7.create(-1, new SplittableRandom(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> UuidV7.create(1L << 48, new SplittableRandom(1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** Every draw the same value: the extremes of the random bits. */
    private static final class FixedRandom implements java.util.random.RandomGenerator {
        private final long value;

        FixedRandom(long value) {
            this.value = value;
        }

        @Override
        public long nextLong() {
            return value;
        }
    }
}
