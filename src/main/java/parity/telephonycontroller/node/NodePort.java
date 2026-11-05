package parity.telephonycontroller.node;

import java.util.List;
import java.util.Optional;
import parity.telephonycontroller.journal.Journal;
import parity.telephonycontroller.model.Kinds.EndCause;
import parity.telephonycontroller.model.Kinds.EndedBy;
import parity.telephonycontroller.model.Kinds.Endpoint;
import parity.telephonycontroller.model.Kinds.LegState;
import parity.telephonycontroller.model.Kinds.RemoteMediaKind;
import parity.telephonycontroller.model.Kinds.Role;

/**
 * The media node as the engine sees it (TC ADR 0002 §2, R1-13): freeswitch's channels with their
 * {@code cc_*} variables, the CDR files freeswitch writes, and the session's media. Two
 * implementations: {@link SimulatedNode} (the fake, unit tests and the in-process script runner) and
 * {@code RealNode} (ESL and parity-ms). TC's own actions return what they did; what the far ends
 * and the node do on their own goes to the {@link Listener}.
 */
public interface NodePort {

    /**
     * Which part of the node was unavailable; {@code PROMPT}: parity-ms has no usable prompt file
     * (missing, unreadable or corrupt: PROMPT_UNKNOWN); {@code RECORDING}: the node has no recording
     * sink able to take the request (INVALID_STATE).
     */
    enum Part { FREESWITCH, PARITY_MS, PROMPT, RECORDING }

    /** freeswitch (ESL) or parity-ms can't be reached, or didn't answer: nothing is known to have changed. */
    final class Unavailable extends Exception {
        private static final long serialVersionUID = 1L;
        private final Part part;

        public Unavailable(Part part) {
            super(part + " is unavailable", null, false, false);
            this.part = part;
        }

        public Part part() {
            return part;
        }
    }

    /**
     * A leg the node won't dial as asked (TC-056: an agent contact that isn't the leg's tenant's, or
     * isn't a SIP URI). Nothing was changed, and the same request can't succeed: the engine refuses
     * the command with a code IE doesn't retry, never as a node that is unavailable.
     */
    final class Refused extends RuntimeException {
        private static final long serialVersionUID = 1L;

        /** Why, as the command's code: {@code tenant_mismatch} or {@code invalid_request}. */
        public enum Why { TENANT_MISMATCH, INVALID_TARGET }

        private final Why why;

        /** The message names the rule only (never a contact, a domain or a tenant). */
        public Refused(Why why, @com.google.errorprone.annotations.CompileTimeConstant String rule) {
            super(rule, null, false, false);
            this.why = why;
        }

        public Why why() {
            return why;
        }
    }

    /** A channel and its variables. Mutable inside a node implementation; the engine gets copies. */
    final class Channel {
        public final String legId;
        public final String tenantId;
        public final String interactionId;
        public final String bin;
        public final Role role;
        public final Endpoint endpoint;
        public final String sipCallId;
        public final boolean inbound;
        public final long createdAtMs;
        public final String calledNumber;
        public final String callerNumber;
        public final String trunkId;
        public final List<String[]> headers;
        public final String addLegFingerprint;
        public LegState state;
        public long answeredAtMs;
        public long ownerEpoch;
        public String ownerReplicaId;
        public String ownerBin;
        /** The session's token (TC ADR 0002 R1-1), when TC accepted or originated the leg. */
        public String sessionToken;

        public Channel(String legId, String tenantId, String interactionId, String bin, Role role, Endpoint endpoint,
                String sipCallId, boolean inbound, long createdAtMs, String calledNumber, String callerNumber,
                String trunkId, List<String[]> headers, String addLegFingerprint, LegState state) {
            this.legId = legId;
            this.tenantId = tenantId;
            this.interactionId = interactionId;
            this.bin = bin;
            this.role = role;
            this.endpoint = endpoint;
            this.sipCallId = sipCallId;
            this.inbound = inbound;
            this.createdAtMs = createdAtMs;
            this.calledNumber = calledNumber;
            this.callerNumber = callerNumber;
            this.trunkId = trunkId;
            this.headers = headers;
            this.addLegFingerprint = addLegFingerprint;
            this.state = state;
        }

        public Channel copy() {
            Channel c = new Channel(legId, tenantId, interactionId, bin, role, endpoint, sipCallId, inbound,
                    createdAtMs, calledNumber, callerNumber, trunkId, headers, addLegFingerprint, state);
            c.answeredAtMs = answeredAtMs;
            c.ownerEpoch = ownerEpoch;
            c.ownerReplicaId = ownerReplicaId;
            c.ownerBin = ownerBin;
            c.sessionToken = sessionToken;
            return c;
        }

        @Override
        public String toString() {
            return "Channel[" + legId + " " + role + " " + state + "]";
        }
    }

    /**
     * Where an originated leg goes ({@code AddLeg}'s destination): an agent's contact, or a number
     * through a trunk; with the session's token, so the leg carries it from its first byte.
     */
    record Dial(String agentContact, String pstnNumber, String trunkId, String callerId, int ringTimeoutS,
            String sessionToken, DtmfSetup dtmf) {

        public Dial(String agentContact, String pstnNumber, String trunkId, String callerId, int ringTimeoutS,
                String sessionToken) {
            this(agentContact, pstnNumber, trunkId, callerId, ringTimeoutS, sessionToken, null);
        }
    }

    /**
     * A leg's DTMF action codes as {@code AddLeg} gives them (P5): set on the leg's media right after
     * parity-ms attaches it and before it rings. {@code lock}/{@code unlock}: the facets (MUTE,
     * RECORDING, MODE) to pin and to free.
     */
    record DtmfSetup(String role, List<DtmfBinding> bindings, int interDigitMs, List<String> lock,
            List<String> unlock) {}

    /** A pause in a recording (silence written), as the recorder reports it. */
    record RecordingPause(long startMs, long endMs, Role role, boolean byDtmf) {}

    /**
     * A finished recording as the media layer reports it (R9's fields; the null sink: no path, 0
     * bytes, no hash). Null from a node that can't say: the engine then reports its own figures.
     */
    record RecordingResult(long durationMs, List<RecordingPause> pauses, String path, long sizeBytes, String sha256) {}

    /** A leg that ended, as TC reports it. */
    record Ended(String legId, EndCause cause, int sipStatus, EndedBy endedBy, long atMs) {}

    /** What the far ends and the node do on their own. */
    interface Listener {
        void parked(Channel channel);

        void ringing(String legId);

        void answered(String legId, long atMs);

        void ended(List<Ended> legs);

        void remoteMedia(String legId, RemoteMediaKind kind);

        void lost(Part part);

        /**
         * A live leg the node found after its first snapshot (freeswitch's listing lags its
         * channels, R1-14): the engine adds it as the rebuild would have.
         */
        default void discovered(Channel channel) {}

        /** A play on a leg ended (parity-ms's reason: completed, stopped, replaced). */
        default void playFinished(String legId, String reason, long playedMs) {}

        /**
         * The same, with the id TC gave the play ({@link NodePort#play(String, String, boolean, String, long)}),
         * echoed by the media; empty from a media that doesn't echo it (then plays end in order).
         */
        default void playFinished(String legId, String reason, long playedMs, String playId) {
            playFinished(legId, reason, playedMs);
        }

        /** The play saga (ADR 0004): the play TC sent under {@code playId} started on the leg. */
        default void playStarted(String legId, String playId) {}

        /**
         * The play saga (ADR 0004): the play TC sent under {@code playId} ended without starting;
         * parity-ms's {@code reason} (prompt_not_found, bad_prompt, busy, cancelled, replaced,
         * leg_gone, invalid_state, unavailable).
         */
        default void playFailed(String legId, String playId, String reason) {}

        /** A digit from a leg, as its reporting allows. PCI: never logged. */
        default void digit(String legId, String digit, int durationMs, String source) {}

        /** A far end held (sendonly or inactive) or resumed (sendrecv) by re-INVITE. */
        default void remoteHold(String legId, boolean on) {}

        /** Media stopped or came back on a leg (parity-ms's packet counters, ADR 0003 D6). */
        default void mediaActivity(String legId, boolean active) {}

        /**
         * A DTMF action code matched (parity-ms P5): {@code action} and {@code result} as the
         * contract names them without their prefix (MUTE_TOGGLE, APPLIED); the leg's mute after it;
         * the session's recording after it (ACTIVE, PAUSED, STOPPED), or empty if the node can't
         * say (the engine then follows it from the results).
         */
        default void dtmfAction(String legId, String action, String result, boolean muted, String recording) {}
    }

    // ---- Step 4: a leg's media (TC ADR 0003 D6); without parity-ms, unavailable -------------------

    /** Plays a prompt file into a leg (replacing one playing). */
    default void play(String legId, String file, long epoch) throws Unavailable {
        throw new Unavailable(Part.PARITY_MS);
    }

    /**
     * The same, {@code loop}: again and again until stopped (R7). True if the media loops it
     * itself (parity-ms says so); false: it plays once and the engine plays it again.
     */
    default boolean play(String legId, String file, boolean loop, long epoch) throws Unavailable {
        play(legId, file, epoch);
        return false;
    }

    /**
     * The same, under {@code playId}: TC's own id for this attempt, which the media echoes when the
     * play ends, so a play TC gave up on is never taken for the one it sent next (MS-042, TC-050).
     */
    default boolean play(String legId, String file, boolean loop, String playId, long epoch) throws Unavailable {
        return play(legId, file, loop, epoch);
    }

    /** Stops a leg's play; false if nothing was playing. */
    default boolean stopPlay(String legId, long epoch) throws Unavailable {
        throw new Unavailable(Part.PARITY_MS);
    }

    /** What the media did with an asynchronous play (the play saga, ADR 0004). */
    enum PlaySent {
        /** Taken: {@code playStarted} or {@code playFailed} follows. */
        ACCEPTED,
        /** The media had this play id already (a repeat): nothing new follows. */
        EXISTING,
        /** Refused outright (the leg can't take it, the fence): nothing follows. */
        REFUSED
    }

    /** Whether the media loops a looped play itself (R7): every parity-ms that plays asynchronously does. */
    default boolean loopsNatively() {
        return true;
    }

    /**
     * Whether plays are asynchronous (parity-ms's {@code play_async}, ADR 0004): {@link #playAsync}
     * answers at once and {@link Listener#playStarted} or {@link Listener#playFailed} follows.
     */
    default boolean asyncPlays() {
        return false;
    }

    /**
     * The play saga (ADR 0004): sends a play under {@code playId}, TC's id for the play's whole
     * life (a retry repeats it). {@link Unavailable}: no answer; the media may have it or not.
     */
    default PlaySent playAsync(String legId, String file, boolean loop, String playId, long epoch) throws Unavailable {
        throw new Unavailable(Part.PARITY_MS);
    }

    /**
     * The play saga (ADR 0004): stops {@code playId} only (a loading one is cancelled, one the media
     * never had is remembered, so it never starts); never a newer play. Repeat-safe.
     */
    default void stopPlay(String legId, String playId, long epoch) throws Unavailable {
        throw new Unavailable(Part.PARITY_MS);
    }

    /** Silences what a leg sends (or not); true if that changed it. */
    default boolean mute(String legId, boolean muted, long epoch) throws Unavailable {
        throw new Unavailable(Part.PARITY_MS);
    }

    /** Sends digits to a leg's endpoint. PCI: never logged. */
    default void sendDtmf(String legId, String digits, long epoch) throws Unavailable {
        throw new Unavailable(Part.PARITY_MS);
    }

    /** Which of a leg's digits are reported: off, masked or on. */
    default void reportDigits(String legId, String mode, long epoch) throws Unavailable {
        throw new Unavailable(Part.PARITY_MS);
    }

    /** One DTMF action code: {@code *} and 1–7 keys, and its action (MUTE_TOGGLE, …). PCI: the code is digits. */
    record DtmfBinding(String sequence, String action) {
        @Override
        public String toString() {
            return "DtmfBinding[" + action + "]";
        }
    }

    /**
     * A leg's DTMF action codes (parity-ms P5; empty turns them off), its role's name, the
     * inter-digit timeout, and the facets (MUTE, RECORDING, MODE) to pin against them and to free.
     */
    default void dtmfActions(String legId, String role, java.util.List<DtmfBinding> bindings, int interDigitMs,
            List<String> lock, List<String> unlock, long epoch) throws Unavailable {
        throw new Unavailable(Part.PARITY_MS);
    }

    /**
     * True when the node records into the null sink (nothing written: MS-012), the only sink until
     * R9's encrypted one. Explicit configuration, local test profiles only (TC wiring).
     */
    default boolean recordingNullSink() {
        return false;
    }

    /** Starts recording a leg (both directions). */
    default void startRecording(String legId, String recordingId, long epoch) throws Unavailable {
        throw new Unavailable(Part.RECORDING);
    }

    /** Stops a leg's recording: its result, or null if the node can't say. */
    default RecordingResult stopRecording(String legId, long epoch) throws Unavailable {
        throw new Unavailable(Part.RECORDING);
    }

    /**
     * A leg that ended had a recording: its result once the media let it go (a stop, a detach or
     * the call's delete), waiting a little for it; null if the node can't say.
     */
    default RecordingResult finishRecording(String legId, long epoch) {
        return null;
    }

    void listen(Listener l);

    /**
     * True when the listener may run session turns on the calling thread (the simulated node: a
     * test's thread). False for ESL: its reader never waits, so the engine hands its events off.
     */
    boolean callbacksInline();

    /** TC accepted an arrival (bin known, no duplicate, leg free): the token and the hangup hook (R1-1). */
    void accepted(String legId, String sessionToken) throws Unavailable;

    /** An arrival's first verified command: the safety hangup goes, the backstop comes (R1-4). */
    void claimArrival(String legId) throws Unavailable;

    /** 200 OK with parity-ms's answer, opening the session's media. Returns when (epoch ms). */
    long answer(String legId, long epoch) throws Unavailable;

    /** 180 Ringing on a parked arrival. */
    void ring(String legId) throws Unavailable;

    /** A final response to an unanswered arrival. */
    Ended reject(String legId, EndCause cause, int sipStatus, EndedBy by) throws Unavailable;

    /**
     * Originates a leg.
     *
     * @throws Refused the agent contact isn't one this node dials for the leg's tenant (TC-056);
     *     thrown before anything changed
     */
    void originate(Channel spec, Dial dial, long epoch) throws Unavailable, Refused;

    /** Hangs up a leg; what the node's own coupling ends with it is returned or reported. */
    List<Ended> hangup(String legId, EndCause cause, EndedBy by) throws Unavailable;

    /** {@code uuid_setvar} of the fence on one leg; false if the leg is gone. */
    boolean setOwner(String legId, long epoch, String replicaId, String bin) throws Unavailable;

    /** parity-ms's {@code claim}: raises the media's fence. */
    void mediaClaim(String interactionId, long epoch) throws Unavailable;

    /** parity-ms {@code update} after a far end moved its media (re-INVITE); the real node did it already. */
    void mediaUpdate(String interactionId, long epoch) throws Unavailable;

    /** Free capacity for new legs. */
    int freeLegs();

    /** Copies of the live channels TC manages. */
    List<Channel> channels();

    /** One live channel, re-read (a session's resync after an overflow). */
    Optional<Channel> channel(String legId);

    /**
     * The CDR files freeswitch wrote for channels that ended since {@code sinceMs} (the journal's
     * newest record: what ended while TC was down, TC ADR 0002 §2.4).
     */
    List<Journal.LegEnded> cdrs(long sinceMs);

    /** The media's fence for a session, or -1 without media. */
    long mediaEpoch(String interactionId);

    /** Invariants a script's end checks (no leg originated twice). */
    List<String> invariantViolations();
}
