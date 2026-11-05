package parity.telephonycontroller.session;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import parity.telephonycontroller.journal.InMemoryJournal;
import parity.telephonycontroller.model.Kinds.Endpoint;
import parity.telephonycontroller.model.Kinds.LegState;
import parity.telephonycontroller.model.Kinds.Role;
import parity.telephonycontroller.node.SimulatedNode;
import parity.telephonycontroller.owner.InMemoryOwnerDirectory;
import parity.telephonycontroller.owner.OwnerRecord;
import parity.telephonycontroller.time.ManualClock;

/** An engine on the simulated node with in-memory owners and journal, for engine-level tests. */
final class EngineFixture {

    static final long START = 1_792_800_000_000L;
    static final String TENANT = "acme";
    static final String BIN = "bin-a";

    final ManualClock clock = new ManualClock(START);
    final InMemoryOwnerDirectory owners = new InMemoryOwnerDirectory();
    final InMemoryJournal journal = new InMemoryJournal();
    final SimulatedNode node = new SimulatedNode(clock, 1_000_000);
    final EngineHolder holder;

    EngineFixture(TelephonyNode.Limits limits) {
        owners.register(BIN, "r1");
        owners.register(BIN, "r2");
        node.setRecordingNullSink(true);
        holder = new EngineHolder(new TelephonyNode.Config("media-1", "test", Set.of(BIN), "inc-1", List.of(), limits),
                clock, owners, journal, node);
        holder.start();
    }

    TelephonyNode engine() {
        return holder.get();
    }

    static Requests.Caller caller() {
        return new Requests.Caller(BIN);
    }

    /** A parked inbound call. */
    void arrive(String interactionId, String legId) {
        node.park(new parity.telephonycontroller.node.NodePort.Channel(legId, TENANT, interactionId, BIN, Role.CUSTOMER, Endpoint.SIP,
                legId + "@carrier", true, clock.nowMs(), "+447700900100", "+447700900123", "carrier-a", List.of(), null,
                LegState.PARKED));
    }

    void claim(String interactionId, long epoch, String replica) {
        owners.delete(interactionId);
        owners.claim(interactionId, epoch, new OwnerRecord(epoch, TENANT, BIN, replica, "media-1"));
    }

    static Requests.Owner owner(String interactionId, long epoch, String replica) {
        return new Requests.Owner(TENANT, interactionId, epoch, replica, 0, "");
    }

    static Requests.CommandKey key(String id) {
        return new Requests.CommandKey(id, "test", id);
    }

    /** A sink that records what it is sent; {@code stuck} makes it report itself not drained. */
    static class RecordingSink implements StreamSink {
        final List<Events.Delivery> sent = Collections.synchronizedList(new ArrayList<>());
        volatile boolean stuck;
        volatile Code closedWith;
        volatile boolean dropped;

        @Override
        public boolean send(Events.Delivery d) {
            if (closedWith != null || dropped) {
                return false;
            }
            sent.add(d);
            return true;
        }

        @Override
        public boolean drained() {
            return !stuck;
        }

        @Override
        public void close(Code code) {
            closedWith = code;
        }

        @Override
        public void drop() {
            dropped = true;
        }

        List<Events.Delivery> sessionEvents() {
            synchronized (sent) {
                return sent.stream().filter(d -> d.session() != null).toList();
            }
        }
    }
}
