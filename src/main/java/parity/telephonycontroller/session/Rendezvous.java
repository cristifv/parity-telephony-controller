package parity.telephonycontroller.session;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

/**
 * Rendezvous hashing for arrivals (TC ADR 0001 §4.5): each replica scores
 * SHA-256({@code interaction_id + "/" + replica_id}) (its first 8 bytes, unsigned); the highest
 * score is offered first, then the next.
 */
public final class Rendezvous {

    private Rendezvous() {}

    public static List<String> order(String interactionId, Collection<String> replicaIds) {
        return replicaIds.stream()
                .sorted(Comparator.comparing((String r) -> score(interactionId, r), Long::compareUnsigned)
                        .reversed().thenComparing(Comparator.naturalOrder()))
                .toList();
    }

    static long score(String interactionId, String replicaId) {
        try {
            byte[] h = MessageDigest.getInstance("SHA-256")
                    .digest((interactionId + "/" + replicaId).getBytes(StandardCharsets.UTF_8));
            return ByteBuffer.wrap(h, 0, 8).getLong();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
