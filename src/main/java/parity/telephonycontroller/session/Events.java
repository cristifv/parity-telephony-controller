package parity.telephonycontroller.session;

import java.util.List;
import parity.telephonycontroller.model.Kinds.EndCause;
import parity.telephonycontroller.model.Kinds.EndedBy;
import parity.telephonycontroller.model.Kinds.LegState;
import parity.telephonycontroller.model.Kinds.RemoteMediaKind;
import parity.telephonycontroller.model.Kinds.Role;
import parity.telephonycontroller.model.Kinds.SessionEndReason;
import parity.telephonycontroller.obs.Pii;

/** What TC streams to IE (TC ADR 0001 §4.4): the events of 0.2.0's scripts, and the envelope. */
public final class Events {

    private Events() {}

    /** One event. */
    public sealed interface Event
            permits Arrived, LegUpdate, RemoteMediaChanged, SessionEnded, NodeStatus, Heartbeat, PlayFinished,
            PlayStarted, PlayFailed, DtmfReceived, RemoteHold, MediaActivity, DtmfAction, RecordingStarted,
            RecordingStopped {}

    /** A far end held or resumed (step 4). */
    public record RemoteHold(String legId, boolean on) implements Event {}

    /** Media stopped ({@code active} false) or came back on a leg (step 4). */
    public record MediaActivity(String legId, boolean active) implements Event {}

    /**
     * A DTMF action code matched (step 4; P5): its action and result without their prefixes; the
     * leg's mute after it, and the session's recording (ACTIVE, PAUSED, STOPPED). Never the digits.
     */
    public record DtmfAction(String legId, Role role, String action, String result, boolean muted, String recording)
            implements Event {}

    /** A recording started (the null sink until R9: nothing is written). */
    public record RecordingStarted(String recordingId, String legId) implements Event {}

    /**
     * A recording finished: the null sink has no path, size or hash; the wrapped key (base64) and
     * the key's id as supplied (never the key itself); its pauses (silence).
     */
    public record RecordingStopped(String recordingId, String legId, String path, long sizeBytes, long durationMs,
            String sha256, String wrappedDekBase64, String kekId,
            List<parity.telephonycontroller.node.NodePort.RecordingPause> pauses) implements Event {}

    /** A play ended (step 4). {@code reason}: COMPLETED, STOPPED, FAILED or LEG_GONE. */
    public record PlayFinished(String legId, String playId, String reason, long playedMs) implements Event {}

    /** A play asked with {@code result_events} started (the play saga, contracts 0.17.0). */
    public record PlayStarted(String legId, String playId) implements Event {}

    /**
     * A play asked with {@code result_events} ended without starting (0.17.0); {@code reason}:
     * PROMPT_UNKNOWN, PARITY_MS_UNAVAILABLE, LEG_GONE, REPLACED, CANCELLED.
     */
    public record PlayFailed(String legId, String playId, String reason) implements Event {}

    /**
     * A digit from a leg under a collection (step 4). PCI: {@code digit} is a card digit possibly;
     * empty when masked. {@code source}: RFC4733 or INBAND. Never logged.
     */
    public record DtmfReceived(String legId, String digit, int durationMs, String source, String collectionId)
            implements Event {
        @Override
        public String toString() {
            return "DtmfReceived[" + legId + "]";
        }
    }

    /** An allow-listed SIP header (the default list is empty). */
    public record Header(String name, String value) {
        @Override
        public String toString() {
            return "Header[" + name + "]";
        }
    }

    /** A parked call offered to a replica. Its numbers never reach a log line: see toString. */
    public record Arrived(String bin, String legId, String sipCallId, String calledNumber, String callerNumber,
            List<Header> headers, int attempt, String redirectedFromBin, long deadlineMs, String trunkId)
            implements Event {
        @Override
        public String toString() {
            return "Arrived[" + legId + " attempt " + attempt + " called " + Pii.maskPhone(calledNumber) + "]";
        }
    }

    /** A leg's new state. */
    public record LegUpdate(String legId, Role role, LegState state, EndCause cause, int sipStatus, EndedBy endedBy,
            long answeredAtMs) implements Event {}

    /** What TC did with a far end's re-INVITE. */
    public record RemoteMediaChanged(String legId, RemoteMediaKind kind) implements Event {}

    /** A session's last event. */
    public record SessionEnded(SessionEndReason reason, int sipStatus) implements Event {}

    /** The node, on every stream. */
    public record NodeStatus(int sessions, int freeLegs, boolean draining, boolean rebuilding, String version,
            List<String> capabilities) implements Event {}

    /** The stream is alive. */
    public record Heartbeat() implements Event {}

    /** The session an event is about, and the fence TC held when it was made. */
    public record SessionRef(String tenantId, String interactionId, long ownerEpoch) {}

    /** One message on a stream; {@code session} null and {@code seq} 0 for node-level ones. */
    public record Delivery(String incarnation, String nodeId, long seq, SessionRef session, long atMs, Event event) {

        /** About what the message costs in memory and on the wire, for the node's buffer budget. */
        public long estimatedBytes() {
            long n = 160 + len(incarnation) + len(nodeId);
            if (session != null) {
                n += len(session.tenantId()) + len(session.interactionId());
            }
            if (event instanceof Arrived a) {
                n += len(a.bin()) + len(a.legId()) + len(a.sipCallId()) + len(a.calledNumber())
                        + len(a.callerNumber()) + len(a.trunkId()) + len(a.redirectedFromBin());
                for (Header h : a.headers()) {
                    n += len(h.name()) + len(h.value());
                }
            } else if (event instanceof LegUpdate u) {
                n += len(u.legId());
            } else if (event instanceof RemoteMediaChanged r) {
                n += len(r.legId());
            }
            return n;
        }

        private static long len(String s) {
            return s == null ? 0 : 2L * s.length();
        }
    }
}
