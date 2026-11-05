package parity.telephonycontroller.session;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import parity.telephonycontroller.model.Kinds.EndCause;
import parity.telephonycontroller.model.Kinds.EndedBy;
import parity.telephonycontroller.model.Kinds.Endpoint;
import parity.telephonycontroller.model.Kinds.LegState;
import parity.telephonycontroller.model.Kinds.MediaKind;
import parity.telephonycontroller.model.Kinds.Role;
import parity.telephonycontroller.model.Kinds.SessionEndReason;
import parity.telephonycontroller.model.Kinds.SessionState;
import parity.telephonycontroller.time.NodeClock;

/**
 * One interaction on this node. Everything here is touched only inside a turn of its
 * {@link #mailbox}: a session's commands, events and timers run one at a time, in the order they
 * were queued (the single writer the fence relies on; TC ADR 0002 §2.1).
 */
final class Session {

    final Mailbox mailbox = new Mailbox();
    final String tenantId;
    final String interactionId;
    SessionState state;
    MediaKind media = MediaKind.NONE;
    boolean onHold;

    long fence;
    String ownerReplica;
    String ownerBin;

    // The arrival (an inbound session until it is claimed).
    String arrivalBin;
    Events.Arrived arrivalTemplate;
    long parkedAtMs;
    long deadlineMs;
    String offeredReplica;
    int attempt;
    long arrivedSeq;
    boolean arrivalAcked;
    boolean claimed;
    String declinedBy;
    int declinedStatus;
    NodeClock.Timer reofferTimer;
    NodeClock.Timer deadlineTimer;

    final Map<String, Leg> legs = new LinkedHashMap<>();

    // Recordings (null sink until R9) and the session's DTMF pause: one for all its recordings (PCI).
    final Map<String, Recording> recordings = new LinkedHashMap<>();
    boolean recordingPaused;
    long recordingPausedAtMs;
    Role recordingPausedBy;
    /** The session's finished pauses, for recordings whose media can't report their own. */
    final java.util.List<parity.telephonycontroller.node.NodePort.RecordingPause> recordingPauses = new java.util.ArrayList<>();

    /** True if any recording of the session is open. */
    boolean recording() {
        return recordings.values().stream().anyMatch(r -> r.open);
    }

    // Delivery: per incarnation, from 1.
    long nextSeq = 1;
    final ArrayDeque<Pending> unacked = new ArrayDeque<>();
    final Map<String, Cached> commands = new HashMap<>();

    // The end (a tombstone).
    SessionEndReason endReason;
    int endSipStatus;
    long endedAtMs;
    boolean endAcked;
    NodeClock.Timer releaseTimer;
    /** TC-036: it ended before any owner: a short-lived tombstone, never replayed. */
    boolean endedOwnerless;

    /** TC's token for the session's legs and its hangup hook (TC ADR 0002 R1-1): a UUIDv7. */
    String token;

    /**
     * Created by its first command and not yet committed (TC ADR 0002 §2.2): invisible to every
     * other command, Watch and GetSession; retired (removed, its queued turns re-dispatched) if
     * that command fails.
     */
    volatile boolean provisional;
    volatile boolean retired;

    /** A node fact was dropped (a full mailbox): the next turn re-reads the legs first. */
    volatile boolean needsResync;

    /** A SessionEnded dropped by the delivery limits: an ack at or past it counts as its ack. */
    long droppedEndSeq;

    Session(String tenantId, String interactionId, SessionState state) {
        this.tenantId = tenantId;
        this.interactionId = interactionId;
        this.state = state;
    }

    List<Leg> liveLegs() {
        return legs.values().stream().filter(l -> l.state != LegState.ENDED).toList();
    }

    Leg customer() {
        return legs.values().stream().filter(l -> l.role == Role.CUSTOMER).findFirst().orElse(null);
    }

    /** The replica that gets this session's events: its owner's, or the one its arrival is offered to. */
    String bound() {
        return ownerReplica != null ? ownerReplica : offeredReplica;
    }

    /**
     * The stream this session's unacknowledged events were last replayed to. A send to any other
     * stream replays them all first, in order, so a stream never gets a newer event before an
     * older unacknowledged one (found by the step-5 stress test).
     */
    Object attachedStream;

    /** True once a drop has been logged for this session (one warning per session, then the counter). */
    boolean dropLogged;

    /** An event sent and not yet acknowledged, with its estimated size for the node's budget. */
    static final class Pending {
        final Events.Delivery delivery;
        final long bytes;
        long lastSentMs;

        Pending(Events.Delivery delivery, long bytes, long lastSentMs) {
            this.delivery = delivery;
            this.bytes = bytes;
            this.lastSentMs = lastSentMs;
        }
    }

    /** A command's first answer, kept 10 minutes. */
    record Cached(String rpc, String fingerprint, Object result, long atMs) {}

    /** A recording of one leg (StartRecording): its id, key reference (never the key itself), start. */
    static final class Recording {
        final String recordingId;
        final String legId;
        final long startedAtMs;
        /** The wrapped key as supplied, base64 (not secret). */
        final String wrappedDek;
        final String kekId;
        boolean open = true;

        Recording(String recordingId, String legId, long startedAtMs, String wrappedDek, String kekId) {
            this.recordingId = recordingId;
            this.legId = legId;
            this.startedAtMs = startedAtMs;
            this.wrappedDek = wrappedDek;
            this.kekId = kekId;
        }
    }

    /** A play sent to a leg: {@code loop} plays it again when it completes, until stopped. */
    static final class Play {
        final String playId;
        final String file;
        final boolean loop;
        boolean stopping;
        /**
         * TC's id for the play sent to the media, echoed in its events (MS-042, TC-050). With an
         * asynchronous media (the play saga, ADR 0004) one id for the play's whole life: every retry
         * repeats it; with an older one, new per send.
         */
        String wireId;
        /** The media said it loops this play itself (R7): a {@code completed} is then never sent again (TC-051). */
        boolean nodeLoops;

        // ---- The play saga (ADR 0004) ----
        /** Where the play is: sent (answer unknown), accepted, started; it leaves the leg's list when it ends. */
        PlayState state = PlayState.REQUESTED;
        /**
         * False once TC no longer wants it (IE stopped it, hold ended, superseded, a caller gave up,
         * TC gave up its retries): a tombstone until its end; a later start is stopped at once.
         */
        boolean wanted = true;
        /** Its one end went to IE (or isn't owed: hold music); later ends are ignored. */
        boolean reported;
        /** IE asked for PlayStarted / PlayFailed (contracts 0.17.0 {@code result_events}). */
        boolean resultEvents;
        /** Hold music (SetHold): TC's own, never reported to IE. */
        boolean hold;
        /** Sends whose answer was lost (each repeats {@link #wireId}). */
        int retries;
        NodeClock.Timer retryTimer;
        /**
         * An IE without {@code result_events} waits for the start: completed with null when it
         * started, or with the error code it gets (PROMPT_UNKNOWN, PARITY_MS_UNAVAILABLE).
         */
        java.util.concurrent.CompletableFuture<Code> started;

        Play(String playId, String file, boolean loop) {
            this.playId = playId;
            this.file = file;
            this.loop = loop;
        }
    }

    /** A play's saga state (ADR 0004). */
    enum PlayState {
        REQUESTED,
        ACCEPTED,
        STARTED
    }

    /** A leg of the session. */
    static final class Leg {
        final String legId;
        final Role role;
        final Endpoint endpoint;
        final String sipCallId;
        final boolean inbound;
        final String fingerprint;
        LegState state;
        EndCause cause;
        int sipStatus;
        EndedBy endedBy;
        long answeredAtMs;
        long endedAtMs;
        NodeClock.Timer ringTimer;
        // Step 4: the leg's media (TC ADR 0003 D6).
        /** Plays sent to parity-ms and not yet finished, oldest first (it reports them in order). */
        final java.util.ArrayDeque<Play> plays = new java.util.ArrayDeque<>();
        /** Every play id this leg was given (Play is repeat-safe by play_id). */
        final java.util.Set<String> playIds = new java.util.HashSet<>();
        /** The media ids of its last plays that ended (the play saga): a late repeat of their events is known. */
        final java.util.ArrayDeque<String> endedPlays = new java.util.ArrayDeque<>();
        boolean muted;
        /** The far end's own hold (its re-INVITE), as last reported. */
        boolean remoteHeld;
        /** No media from the leg (parity-ms's counters), as last reported. */
        boolean mediaInactive;
        /** The digit collection now (SetDigitReporting): its id, its mode, its end. */
        String collectionId = "";
        String digitMode = "OFF";
        NodeClock.Timer digitTimer;
        /** The facets pinned against this leg's DTMF codes (MUTE, RECORDING, MODE). */
        final java.util.Set<String> locks = new java.util.HashSet<>();

        Leg(String legId, Role role, Endpoint endpoint, String sipCallId, boolean inbound, String fingerprint,
                LegState state) {
            this.legId = legId;
            this.role = role;
            this.endpoint = endpoint;
            this.sipCallId = sipCallId;
            this.inbound = inbound;
            this.fingerprint = fingerprint;
            this.state = state;
        }
    }
}
