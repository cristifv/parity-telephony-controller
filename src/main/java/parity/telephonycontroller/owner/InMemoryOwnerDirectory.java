package parity.telephonycontroller.owner;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The owner directory of {@code fake-telephony-controller} and of tests: records are put with the
 * epoch a script names as their create revision, and the whole directory can be made unavailable
 * (a script's {@code etcd_down}).
 */
public final class InMemoryOwnerDirectory implements OwnerDirectory, OwnerWriter {

    private final ConcurrentHashMap<String, OwnerRecord> owners = new ConcurrentHashMap<>();
    private final Set<String> replicas = ConcurrentHashMap.newKeySet();
    private volatile boolean available = true;

    @Override
    public Optional<OwnerRecord> owner(String interactionId) throws OwnerDirectoryUnavailable {
        check();
        return Optional.ofNullable(owners.get(interactionId));
    }

    @Override
    public boolean replicaRegistered(String bin, String replicaId) throws OwnerDirectoryUnavailable {
        check();
        return replicas.contains(bin + "/" + replicaId);
    }

    @Override
    public long claim(String interactionId, long epoch, OwnerRecord value) {
        owners.put(interactionId, new OwnerRecord(epoch, value.tenantId(), value.bin(), value.replicaId(), value.nodeId()));
        return epoch;
    }

    @Override
    public void delete(String interactionId) {
        owners.remove(interactionId);
    }

    @Override
    public void register(String bin, String replicaId) {
        replicas.add(bin + "/" + replicaId);
    }

    @Override
    public void setAvailable(boolean up) {
        available = up;
    }

    @Override
    public void clear() {
        owners.clear();
        replicas.clear();
        available = true;
    }

    private void check() throws OwnerDirectoryUnavailable {
        if (!available) {
            throw new OwnerDirectoryUnavailable(null);
        }
    }
}
