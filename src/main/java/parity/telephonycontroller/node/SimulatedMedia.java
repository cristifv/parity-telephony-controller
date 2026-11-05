package parity.telephonycontroller.node;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Media without parity-ms (TC ADR 0002 §6): syntactically valid G.711 SDPs aimed at a port nobody
 * listens on. The scripts on real freeswitch use it (SIP endpoints that ignore media), as do the
 * fake and unit tests. It can be made unreachable, or lost, like the real one.
 */
public final class SimulatedMedia implements MediaPort {

    private final ConcurrentHashMap<String, Long> epochs = new ConcurrentHashMap<>();
    private final AtomicInteger ports = new AtomicInteger(20_000);
    private volatile boolean up = true;
    /** Recording into the null sink allowed (tests and the fake: nothing is ever written here). */
    private volatile boolean nullSink;

    public void setUp(boolean up) {
        this.up = up;
    }

    /** Test hook: the null recording sink is configured (TC's local test profiles). */
    public void setNullSink(boolean on) {
        nullSink = on;
    }

    /** parity-ms died: every session's media is gone, with its legs' mutes and digit settings. */
    public void lose() {
        epochs.clear();
        mutes.clear();
        digitModes.clear();
    }

    private void need() throws NodePort.Unavailable {
        if (!up) {
            throw new NodePort.Unavailable(NodePort.Part.PARITY_MS);
        }
    }

    private String sdp() {
        int port = ports.getAndAdd(2);
        if (port > 29_000) {
            ports.set(20_000);
        }
        return "v=0\r\no=parity-ms 1 1 IN IP4 127.0.0.1\r\ns=-\r\nc=IN IP4 127.0.0.1\r\nt=0 0\r\n"
                + "m=audio " + port + " RTP/AVP 0 101\r\na=rtpmap:0 PCMU/8000\r\n"
                + "a=rtpmap:101 telephone-event/8000\r\na=fmtp:101 0-16\r\na=sendrecv\r\n";
    }

    @Override
    public String open(String tenantId, String interactionId, String offerA, long epoch) throws NodePort.Unavailable {
        need();
        epochs.merge(interactionId, epoch, Math::max);
        return sdp();
    }

    @Override
    public String attach(String tenantId, String interactionId, String leg, boolean webrtc, long epoch)
            throws NodePort.Unavailable {
        need();
        epochs.merge(interactionId, epoch, Math::max);
        // A fresh leg B, as parity-ms makes one: not muted, no digit reporting.
        mutes.remove(leg(interactionId, Side.B));
        digitModes.remove(leg(interactionId, Side.B));
        return sdp();
    }

    @Override
    public void complete(String tenantId, String interactionId, String leg, String answer, long epoch)
            throws NodePort.Unavailable {
        need();
    }

    @Override
    public NodePort.RecordingResult detach(String tenantId, String interactionId, String leg, long epoch)
            throws NodePort.Unavailable {
        need();
        return null;
    }

    @Override
    public void updateRemote(String tenantId, String interactionId, Side side, String sdp, long epoch)
            throws NodePort.Unavailable {
        need();
    }

    @Override
    public void claim(String tenantId, String interactionId, long epoch) throws NodePort.Unavailable {
        need();
        epochs.computeIfPresent(interactionId, (k, v) -> Math.max(v, epoch));
    }

    @Override
    public java.util.Map<Side, NodePort.RecordingResult> end(String tenantId, String interactionId, long epoch) {
        epochs.remove(interactionId);
        return java.util.Map.of();
    }

    @Override
    public long epoch(String interactionId) {
        return epochs.getOrDefault(interactionId, -1L);
    }

    // ---- Step 4: media commands, simulated; the tests fire parity-ms's events ---------------------

    private volatile Events events;
    private final java.util.Set<String> missingPrompts = ConcurrentHashMap.newKeySet();
    private final ConcurrentHashMap<String, String> digitModes = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Boolean> mutes = new ConcurrentHashMap<>();

    private static String leg(String interactionId, Side side) {
        return interactionId + "/" + side;
    }

    @Override
    public void subscribe(Events e) {
        events = e;
    }

    /** Test hook: parity-ms has no such prompt (the scripts' {@code prompt_missing}). */
    public void promptMissing(String promptId) {
        missingPrompts.add(promptId);
    }

    @Override
    public void play(String tenantId, String interactionId, Side side, String file, long epoch) throws NodePort.Unavailable {
        need();
        String id = file.substring(file.lastIndexOf('/') + 1).replaceFirst("\\.wav$", "");
        if (missingPrompts.contains(id)) {
            throw new NodePort.Unavailable(NodePort.Part.PROMPT);
        }
    }

    @Override
    public boolean stopPlay(String tenantId, String interactionId, Side side, long epoch) throws NodePort.Unavailable {
        need();
        return true;
    }

    @Override
    public boolean mute(String tenantId, String interactionId, Side side, boolean muted, long epoch)
            throws NodePort.Unavailable {
        need();
        Boolean before = mutes.put(leg(interactionId, side), muted);
        return (before != null && before) != muted;
    }

    @Override
    public void sendDtmf(String tenantId, String interactionId, Side side, String digits, long epoch)
            throws NodePort.Unavailable {
        need();
    }

    @Override
    public void reportDigits(String tenantId, String interactionId, Side side, String mode, long epoch)
            throws NodePort.Unavailable {
        need();
        digitModes.put(leg(interactionId, side), mode);
    }

    @Override
    public void dtmfActions(String tenantId, String interactionId, Side side, String role,
            java.util.List<NodePort.DtmfBinding> bindings, int interDigitMs, java.util.List<String> lock,
            java.util.List<String> unlock, long epoch) throws NodePort.Unavailable {
        need();
    }

    @Override
    public String recordingSink() {
        return nullSink ? "null" : "none";
    }

    /** No media here: the engine reports its own figures (null results). */
    @Override
    public void startRecording(String tenantId, String interactionId, Side side, String recordingId, long epoch)
            throws NodePort.Unavailable {
        need();
        if (!nullSink) {
            throw new NodePort.Unavailable(NodePort.Part.RECORDING);
        }
    }

    @Override
    public NodePort.RecordingResult stopRecording(String tenantId, String interactionId, Side side, long epoch)
            throws NodePort.Unavailable {
        need();
        return null;
    }

    /** Test hook: parity-ms reports a play ended. */
    public void played(String interactionId, Side side, String reason, long playedMs) {
        Events e = events;
        if (e != null) {
            e.played(interactionId, side, reason, playedMs);
        }
    }

    /** Test hook: a digit from the leg, as its reporting allows ({@code *} masked). */
    public void digit(String interactionId, Side side, String digit, String source) {
        String mode = digitModes.getOrDefault(leg(interactionId, side), "off");
        Events e = events;
        if (e != null && !"off".equals(mode)) {
            e.digit(interactionId, side, "masked".equals(mode) ? "*" : digit, 100, source);
        }
    }

    /** Test hook: media stopped or came back from the leg. */
    public void activity(String interactionId, Side side, boolean active) {
        Events e = events;
        if (e != null) {
            e.activity(interactionId, side, active);
        }
    }

    /** Test hook: a DTMF action code matched; a mute action applied changes the leg's mute. */
    public void dtmfActionMatch(String interactionId, Side side, String action, String result) {
        String k = leg(interactionId, side);
        boolean muted = mutes.getOrDefault(k, false);
        if ("APPLIED".equals(result)) {
            muted = switch (action) {
                case "MUTE" -> true;
                case "UNMUTE" -> false;
                case "MUTE_TOGGLE" -> !muted;
                default -> muted;
            };
            mutes.put(k, muted);
        }
        Events e = events;
        if (e != null) {
            e.dtmfAction(interactionId, side, action, result, muted, "");
        }
    }
}
