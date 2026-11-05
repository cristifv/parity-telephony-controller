package parity.telephonycontroller.model;

import java.security.SecureRandom;
import java.util.UUID;
import java.util.random.RandomGenerator;

/**
 * UUIDv7 (RFC 9562 §5.7): 48 bits of Unix epoch milliseconds, then random bits. Its canonical
 * lowercase text sorts by time, which is what {@code telephony.v1}'s incarnation promises ("a
 * UUIDv7 made at start"): a later run's incarnation sorts higher than an earlier one's, unless the
 * wall clock went back between the two ({@link #after} adds the floor for that, TC ADR 0002 §2.6).
 */
public final class UuidV7 {

    private static final long MAX_MS = (1L << 48) - 1;
    private static final RandomGenerator RANDOM = new SecureRandom();

    private UuidV7() {}

    /** A UUIDv7 for now (the system's wall clock). */
    public static String create() {
        return create(System.currentTimeMillis(), RANDOM);
    }

    /**
     * A UUIDv7 that sorts after {@code previous} (a UUIDv7, or null) even if the wall clock went
     * back: its time is the later of now and {@code previous}'s + 1 ms (TC ADR 0002 §2.6).
     */
    public static String after(String previous) {
        long now = System.currentTimeMillis();
        if (previous != null) {
            try {
                now = Math.max(now, unixMs(previous) + 1);
            } catch (IllegalArgumentException notV7) {
                // an older run's random UUID: no floor
            }
        }
        return create(now, RANDOM);
    }

    /** A UUIDv7 for {@code unixMs}, its random bits from {@code random}. */
    public static String create(long unixMs, RandomGenerator random) {
        if (unixMs < 0 || unixMs > MAX_MS) {
            throw new IllegalArgumentException("a UUIDv7 needs a time within 48 bits of milliseconds");
        }
        long msb = (unixMs << 16) | 0x7000L | (random.nextLong() & 0x0FFFL);
        long lsb = (random.nextLong() & 0x3FFF_FFFF_FFFF_FFFFL) | 0x8000_0000_0000_0000L;
        return new UUID(msb, lsb).toString();
    }

    /** The milliseconds a UUIDv7 carries; refuses any other version. */
    public static long unixMs(String uuid) {
        UUID u = UUID.fromString(uuid);
        if (u.version() != 7 || u.variant() != 2) {
            throw new IllegalArgumentException("not a UUIDv7");
        }
        return u.getMostSignificantBits() >>> 16;
    }
}
