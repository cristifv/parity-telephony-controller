package parity.telephonycontroller.node;

/**
 * The session's media as the real node brokers it (TC ADR 0002 §6; step 4: parity-ms's own API,
 * requirement 41). A session's media is one parity-ms call named by the interaction id: leg A is
 * the customer's (answered by {@code open}), leg B the agent's ({@code attach}, then the agent's
 * answer; {@code detach} when it ends, so another agent can follow). SDPs are opaque here and never
 * logged. Every mutating call carries the tenant and the owner's fence.
 */
public interface MediaPort {

    /** A call's two legs, as parity-ms names them. */
    enum Side {
        A,
        B
    }

    /** Before the rebuild: learn which sessions still have media (after a TC restart). */
    default void start() {}

    /** An arrival's offer: the answer to send back to its leg (leg A). */
    String open(String tenantId, String interactionId, String offerA, long epoch) throws NodePort.Unavailable;

    /** A leg TC originates while leg A is up: the offer to send it (leg B). */
    String attach(String tenantId, String interactionId, String leg, boolean webrtc, long epoch)
            throws NodePort.Unavailable;

    /** The originated leg's answer. */
    void complete(String tenantId, String interactionId, String leg, String answer, long epoch)
            throws NodePort.Unavailable;

    /**
     * The originated leg ended while leg A goes on: its stream and ports go (ok if already gone).
     * Returns its recording's result if it was recording, else null.
     */
    NodePort.RecordingResult detach(String tenantId, String interactionId, String leg, long epoch)
            throws NodePort.Unavailable;

    /** A far end moved its media (a re-INVITE answered by freeswitch, read from switch_r_sdp). */
    void updateRemote(String tenantId, String interactionId, Side side, String sdp, long epoch)
            throws NodePort.Unavailable;

    /** Raises the media's fence. */
    void claim(String tenantId, String interactionId, long epoch) throws NodePort.Unavailable;

    /**
     * The session's media goes (its last leg ended); ok if already gone. Returns the results of the
     * recordings it finished, by side.
     */
    java.util.Map<Side, NodePort.RecordingResult> end(String tenantId, String interactionId, long epoch)
            throws NodePort.Unavailable;

    /** The media's fence, or -1 without media. */
    long epoch(String interactionId);

    // ---- Step 4: what IE does with a leg's media (ADR 0003 D6) ------------------------------------

    /** Plays a prompt file (a path under parity-ms's prompt directory) into a leg; replaces one playing. */
    default void play(String tenantId, String interactionId, Side side, String file, long epoch)
            throws NodePort.Unavailable {
        throw new NodePort.Unavailable(NodePort.Part.PARITY_MS);
    }

    /** The same, {@code loop}ed (R7); true if the media loops it itself. */
    default boolean play(String tenantId, String interactionId, Side side, String file, boolean loop, long epoch)
            throws NodePort.Unavailable {
        play(tenantId, interactionId, side, file, epoch);
        return false;
    }

    /** The same, under TC's {@code playId} for this attempt (echoed in {@link Events#played}). */
    default boolean play(String tenantId, String interactionId, Side side, String file, boolean loop, String playId,
            long epoch) throws NodePort.Unavailable {
        return play(tenantId, interactionId, side, file, loop, epoch);
    }

    /** Stops what a leg plays; false if nothing was playing. */
    default boolean stopPlay(String tenantId, String interactionId, Side side, long epoch) throws NodePort.Unavailable {
        throw new NodePort.Unavailable(NodePort.Part.PARITY_MS);
    }

    /** The play saga (ADR 0004): whether {@link #playAsync} is there (parity-ms's {@code play_async}). */
    default boolean asyncPlays() {
        return false;
    }

    /** The play saga (ADR 0004): an asynchronous play under {@code playId} (see {@link NodePort#playAsync}). */
    default NodePort.PlaySent playAsync(String tenantId, String interactionId, Side side, String file, boolean loop,
            String playId, long epoch) throws NodePort.Unavailable {
        throw new NodePort.Unavailable(NodePort.Part.PARITY_MS);
    }

    /** The play saga (ADR 0004): stops {@code playId} only (see {@link NodePort#stopPlay(String, String, long)}). */
    default void stopPlay(String tenantId, String interactionId, Side side, String playId, long epoch)
            throws NodePort.Unavailable {
        throw new NodePort.Unavailable(NodePort.Part.PARITY_MS);
    }

    /** Silences (or un-silences) what a leg sends; true if that changed it. */
    default boolean mute(String tenantId, String interactionId, Side side, boolean muted, long epoch)
            throws NodePort.Unavailable {
        throw new NodePort.Unavailable(NodePort.Part.PARITY_MS);
    }

    /** Sends digits to a leg's endpoint. PCI: never logged. */
    default void sendDtmf(String tenantId, String interactionId, Side side, String digits, long epoch)
            throws NodePort.Unavailable {
        throw new NodePort.Unavailable(NodePort.Part.PARITY_MS);
    }

    /** Which of a leg's digits become events: {@code off}, {@code masked} or {@code on}. */
    default void reportDigits(String tenantId, String interactionId, Side side, String mode, long epoch)
            throws NodePort.Unavailable {
        throw new NodePort.Unavailable(NodePort.Part.PARITY_MS);
    }

    /** A leg's DTMF action codes (P5), its role, the inter-digit timeout, the facets to pin and to free. */
    default void dtmfActions(String tenantId, String interactionId, Side side, String role,
            java.util.List<NodePort.DtmfBinding> bindings, int interDigitMs, java.util.List<String> lock,
            java.util.List<String> unlock, long epoch) throws NodePort.Unavailable {
        throw new NodePort.Unavailable(NodePort.Part.PARITY_MS);
    }

    /**
     * The sink recordings go to: {@code null} (nothing written; only when TC's configuration allows
     * it and parity-ms says it runs one), or {@code none} (TC records nothing).
     */
    default String recordingSink() {
        return "none";
    }

    /** Starts recording a leg ({@code recordingId} makes the request repeat-safe). */
    default void startRecording(String tenantId, String interactionId, Side side, String recordingId, long epoch)
            throws NodePort.Unavailable {
        throw new NodePort.Unavailable(NodePort.Part.RECORDING);
    }

    /** Stops a leg's recording: its result (null if it had none). */
    default NodePort.RecordingResult stopRecording(String tenantId, String interactionId, Side side, long epoch)
            throws NodePort.Unavailable {
        throw new NodePort.Unavailable(NodePort.Part.RECORDING);
    }

    /** What parity-ms reports about a session's media (ADR 0003 D5). */
    interface Events {
        /** A play on a leg ended: {@code completed}, {@code stopped} or {@code replaced}. */
        void played(String interactionId, Side side, String reason, long playedMs);

        /** The same, with the play's id as TC gave it (empty if the media doesn't echo it). */
        default void played(String interactionId, Side side, String reason, long playedMs, String playId) {
            played(interactionId, side, reason, playedMs);
        }

        /** The play saga (ADR 0004): the play TC sent under {@code playId} started. */
        default void playStarted(String interactionId, Side side, String playId) {}

        /** The play saga (ADR 0004): the play TC sent under {@code playId} ended without starting. */
        default void playFailed(String interactionId, Side side, String playId, String reason) {}

        /** A digit from a leg, as its reporting allows ({@code *} when masked). PCI: never logged. */
        void digit(String interactionId, Side side, String digit, int durationMs, String source);

        /** parity-ms ended a call TC didn't (its idle timeout, a shutdown). */
        void ended(String interactionId, String reason);

        /**
         * A DTMF action code matched: the contract's action and result names, the leg's mute after
         * it, the session's recording after it (ACTIVE, PAUSED, STOPPED; empty if unknown).
         */
        default void dtmfAction(String interactionId, Side side, String action, String result, boolean muted,
                String recording) {}

        /** Media stopped or came back from a leg (the sweep's packet counters). */
        default void activity(String interactionId, Side side, boolean active) {}

        /** Whether TC still has a live leg in the session (the sweep's first, cheap look). */
        default boolean live(String interactionId) {
            return true;
        }

        /**
         * How long the node has been fully in sync (ready, its late snapshots done) since its last
         * start or ESL reconnect, in ms; -1 while it isn't. The sweep waits for its grace.
         */
        default long syncedForMs() {
            return -1;
        }

        /**
         * Just before a delete: true only if TC has no leg in the session and freeswitch, asked
         * now, has no channel carrying its {@code cc_interaction_id}; false if freeswitch can't say.
         */
        default boolean confirmOrphan(String interactionId) {
            return false;
        }
    }

    /** Where parity-ms's events go (one subscriber). */
    default void subscribe(Events events) {}
}
