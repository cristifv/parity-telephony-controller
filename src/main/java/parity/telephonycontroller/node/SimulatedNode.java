package parity.telephonycontroller.node;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import parity.telephonycontroller.journal.Journal;
import parity.telephonycontroller.model.Kinds.EndCause;
import parity.telephonycontroller.model.Kinds.EndedBy;
import parity.telephonycontroller.model.Kinds.LegState;
import parity.telephonycontroller.model.Kinds.RemoteMediaKind;
import parity.telephonycontroller.model.Kinds.Role;
import parity.telephonycontroller.time.NodeClock;

/**
 * The media node as TC sees it, simulated until steps 3 and 4 bring ESL and parity-ms: freeswitch's
 * channels with their {@code cc_*} variables (the durable copy of the fence) and the per-leg CDR
 * files freeswitch writes itself, and parity-ms's media with its own epoch fence. It outlives a
 * simulated TC restart, as the real node does. Hangup coupling is freeswitch's: when the customer's
 * leg ends, every other leg of the call ends too ({@code api_hangup_hook}).
 *
 * <p>TC's own actions return what they did; the far ends' actions (a script's {@code inject}) are
 * reported to the {@link Listener}, never while this node's lock is held.
 */
public final class SimulatedNode implements NodePort {

    private final NodeClock clock;
    private final ReentrantLock lock = new ReentrantLock();
    private final Map<String, Channel> channels = new LinkedHashMap<>();
    private final List<Journal.LegEnded> cdrs = new ArrayList<>();
    private final Map<String, Long> mediaEpochs = new LinkedHashMap<>();
    private final Map<String, Integer> originates = new LinkedHashMap<>();
    private volatile Listener listener;
    private volatile boolean freeswitchUp = true;
    private volatile boolean parityMsUp = true;
    private int freeLegs;

    public SimulatedNode(NodeClock clock, int freeLegs) {
        this.clock = clock;
        this.freeLegs = freeLegs;
    }

    @Override
    public void listen(Listener l) {
        listener = l;
    }

    @Override
    public boolean callbacksInline() {
        return true;
    }

    @Override
    public void accepted(String legId, String sessionToken) throws Unavailable {
        need(true, false);
        with(legId, c -> c.sessionToken = sessionToken);
    }

    @Override
    public void claimArrival(String legId) throws Unavailable {
        need(true, false);
    }

    // ---- TC's commands -------------------------------------------------------------------

    /** 200 OK with parity-ms's answer: opens the session's media (one parity-ms leg). */
    @Override
    public long answer(String legId, long epoch) throws Unavailable {
        need(true, true);
        lock.lock();
        try {
            Channel c = channels.get(legId);
            if (c == null) {
                return -1;
            }
            c.state = LegState.ANSWERED;
            c.answeredAtMs = clock.nowMs();
            freeLegs--;
            mediaEpochs.merge(c.interactionId, epoch, Math::max);
            return c.answeredAtMs;
        } finally {
            lock.unlock();
        }
    }

    /** 180 Ringing on a parked arrival. */
    @Override
    public void ring(String legId) throws Unavailable {
        need(true, false);
        lock.lock();
        try {
            Channel c = channels.get(legId);
            if (c != null) {
                c.state = LegState.RINGING;
            }
        } finally {
            lock.unlock();
        }
    }

    /** A final response to an unanswered arrival. */
    @Override
    public Ended reject(String legId, EndCause cause, int sipStatus, EndedBy by) throws Unavailable {
        need(true, false);
        lock.lock();
        try {
            return end(legId, cause, sipStatus, by);
        } finally {
            lock.unlock();
        }
    }

    /** Originates a leg (the loopback recipe, later); parity-ms makes its offer (one leg). */
    @Override
    public void originate(Channel spec, Dial dial, long epoch) throws Unavailable {
        need(true, true);
        AgentRegistrar r = agentRegistrar;
        if (r != null && dial.pstnNumber() == null) {
            // The real node's rule (TC-056), so engine tests see what it refuses.
            java.util.regex.Matcher m = FsText.SIP_URI.matcher(dial.agentContact() == null ? "" : dial.agentContact());
            if (!m.matches()) {
                throw new Refused(Refused.Why.INVALID_TARGET, "not an agent's SIP URI");
            }
            r.directoryDomain(spec.tenantId, m.group(2));
        }
        lock.lock();
        try {
            spec.sessionToken = dial.sessionToken();
            channels.put(spec.legId, spec);
            if (dial.dtmf() != null) {
                dtmfSetups.put(spec.legId, dial.dtmf());
            }
            originates.merge(spec.legId, 1, Integer::sum);
            freeLegs--;
            mediaEpochs.merge(spec.interactionId, epoch, Math::max);
        } finally {
            lock.unlock();
        }
    }

    /** Hangs up a leg; freeswitch's hook ends the others if it is the customer's. */
    @Override
    public List<Ended> hangup(String legId, EndCause cause, EndedBy by) throws Unavailable {
        need(true, true);
        lock.lock();
        try {
            return endWithHook(legId, cause, 0, by);
        } finally {
            lock.unlock();
        }
    }

    /** {@code uuid_setvar} of the fence on one leg; false if the leg is gone. */
    @Override
    public boolean setOwner(String legId, long epoch, String replicaId, String bin) throws Unavailable {
        need(true, false);
        lock.lock();
        try {
            Channel c = channels.get(legId);
            if (c == null) {
                return false;
            }
            c.ownerEpoch = epoch;
            c.ownerReplicaId = replicaId;
            c.ownerBin = bin;
            return true;
        } finally {
            lock.unlock();
        }
    }

    /** parity-ms's {@code claim}: raises the media's fence. */
    @Override
    public void mediaClaim(String interactionId, long epoch) throws Unavailable {
        need(false, true);
        lock.lock();
        try {
            mediaEpochs.computeIfPresent(interactionId, (k, v) -> Math.max(v, epoch));
        } finally {
            lock.unlock();
        }
    }

    /** parity-ms {@code update} after a far end moved its media (re-INVITE). */
    @Override
    public void mediaUpdate(String interactionId, long epoch) throws Unavailable {
        need(false, true);
        mediaClaim(interactionId, epoch);
    }

    // ---- Step 4: a leg's media, simulated (plays end only when told: playFinished) ---------------

    private final Map<String, Boolean> playing = new java.util.concurrent.ConcurrentHashMap<>();
    private final Map<String, Boolean> muted = new java.util.concurrent.ConcurrentHashMap<>();
    private final Map<String, String> digitModes = new java.util.concurrent.ConcurrentHashMap<>();

    private final java.util.Set<String> missingPrompts = java.util.concurrent.ConcurrentHashMap.newKeySet();

    /** Test hook (the scripts' {@code prompt_missing}): parity-ms has no such prompt. */
    public void promptMissing(String promptId) {
        missingPrompts.add(promptId);
    }

    private final Map<String, Boolean> looped = new java.util.concurrent.ConcurrentHashMap<>();
    private final Map<String, DtmfSetup> dtmfSetups = new java.util.concurrent.ConcurrentHashMap<>();

    /** Test hook: whether the last play sent to a leg asked for a loop. */
    public boolean lastPlayLooped(String legId) {
        return looped.getOrDefault(legId, false);
    }

    /** Test hook: the DTMF codes and locks last set on a leg (AddLeg's or SetDtmfActions'). */
    public DtmfSetup dtmfSetup(String legId) {
        return dtmfSetups.get(legId);
    }

    private final Map<String, String> playIds = new java.util.concurrent.ConcurrentHashMap<>();
    private volatile boolean loopsNatively;

    /** Test hook: the node loops a looped play itself (a parity-ms with R7), as it then says. */
    public void setLoopsNatively(boolean on) {
        loopsNatively = on;
    }

    /** Test hook: the id TC gave the last play sent to a leg (sent: it may have failed). */
    public String lastPlayId(String legId) {
        return playIds.get(legId);
    }

    @Override
    public boolean play(String legId, String file, boolean loop, long epoch) throws Unavailable {
        return play(legId, file, loop, null, epoch);
    }

    @Override
    public boolean play(String legId, String file, boolean loop, String playId, long epoch) throws Unavailable {
        if (playId != null) {
            playIds.put(legId, playId);
        }
        play(legId, file, epoch);
        looped.put(legId, loop);
        return loop && loopsNatively;
    }

    @Override
    public void play(String legId, String file, long epoch) throws Unavailable {
        need(false, true);
        if (missingPrompts.contains(promptName(file))) {
            throw new Unavailable(Part.PROMPT);
        }
        playing.put(legId, true);
    }

    private static String promptName(String file) {
        return file.substring(file.lastIndexOf('/') + 1).replaceFirst("\\.wav$", "");
    }

    // ---- The play saga (ADR 0004), simulated: a parity-ms with play_async (the default) ----------

    private volatile boolean asyncPlays = true;
    private volatile boolean holdStarts;
    private final java.util.concurrent.atomic.AtomicInteger asyncSends = new java.util.concurrent.atomic.AtomicInteger();
    private final java.util.List<String> stopsById = new java.util.concurrent.CopyOnWriteArrayList<>();
    /** Per leg: the id playing, the id waiting for its prompt ({@link #holdPlayStarts}). */
    private final Map<String, String> currentIds = new java.util.concurrent.ConcurrentHashMap<>();
    private final Map<String, String> pendingIds = new java.util.concurrent.ConcurrentHashMap<>();
    /** Every id the media was given or told to stop (parity-ms's recent ids: a repeat does nothing). */
    private final java.util.Set<String> seenIds = java.util.concurrent.ConcurrentHashMap.newKeySet();

    /** Test hook: plays are asynchronous (true, the default) or synchronous (an older parity-ms). */
    public void setAsyncPlays(boolean on) {
        asyncPlays = on;
    }

    /**
     * Test hook: asynchronous plays wait for their prompt until {@link #promptLoaded} or
     * {@link #promptFailed} (a slow load); otherwise they start (or fail) as they are sent.
     */
    public void holdPlayStarts(boolean on) {
        holdStarts = on;
    }

    /** Test hook: asynchronous play sends so far (retries included). */
    public int playSends() {
        return asyncSends.get();
    }

    /** Test hook: whether the leg plays something now (as far as the media knows). */
    public boolean isPlaying(String legId) {
        return playing.containsKey(legId);
    }

    /** Test hook: the ids {@code stop_play} named, in order. */
    public java.util.List<String> stopsById() {
        return java.util.List.copyOf(stopsById);
    }

    @Override
    public boolean asyncPlays() {
        return asyncPlays;
    }

    @Override
    public boolean loopsNatively() {
        return loopsNatively;
    }

    @Override
    public PlaySent playAsync(String legId, String file, boolean loop, String playId, long epoch) throws Unavailable {
        asyncSends.incrementAndGet();
        playIds.put(legId, playId);
        looped.put(legId, loop);
        need(false, true);
        if (!seenIds.add(playId)) {
            return PlaySent.EXISTING;
        }
        Listener li = listener;
        if (missingPrompts.contains(promptName(file))) {
            if (li != null) {
                li.playFailed(legId, playId, "prompt_not_found");
            }
            return PlaySent.ACCEPTED;
        }
        if (holdStarts) {
            String older = pendingIds.put(legId, playId);
            if (older != null && li != null) {
                li.playFailed(legId, older, "replaced");
            }
            return PlaySent.ACCEPTED;
        }
        start(legId, playId);
        return PlaySent.ACCEPTED;
    }

    private void start(String legId, String playId) {
        playing.put(legId, true);
        currentIds.put(legId, playId);
        Listener li = listener;
        if (li != null) {
            li.playStarted(legId, playId);
        }
    }

    /** Test hook: the prompt of the leg's waiting play loaded: it starts (and reports it). */
    public void promptLoaded(String legId) {
        String id = pendingIds.remove(legId);
        if (id != null) {
            start(legId, id);
        }
    }

    /** Test hook: the prompt of the leg's waiting play failed ({@code reason} as parity-ms says it). */
    public void promptFailed(String legId, String reason) {
        String id = pendingIds.remove(legId);
        Listener li = listener;
        if (id != null && li != null) {
            li.playFailed(legId, id, reason);
        }
    }

    /** Test hook: a play the media has starts now, whatever TC thinks of it (a late start). */
    public void startedLate(String legId, String playId) {
        start(legId, playId);
    }

    @Override
    public void stopPlay(String legId, String playId, long epoch) throws Unavailable {
        need(false, true);
        stopsById.add(playId);
        if (pendingIds.remove(legId, playId)) {
            Listener li = listener;
            if (li != null) {
                li.playFailed(legId, playId, "cancelled");
            }
        } else if (playId.equals(currentIds.get(legId))) {
            playing.remove(legId);
        } else {
            seenIds.add(playId);
        }
    }

    @Override
    public void dtmfActions(String legId, String role, List<DtmfBinding> bindings, int interDigitMs, List<String> lock,
            List<String> unlock, long epoch) throws Unavailable {
        need(false, true);
        dtmfSetups.put(legId, new DtmfSetup(role, List.copyOf(bindings), interDigitMs, List.copyOf(lock), List.copyOf(unlock)));
    }

    // ---- Recording into the null sink (nothing is written; the engine reports its own figures) -----

    private volatile boolean nullSink;
    private volatile AgentRegistrar agentRegistrar;

    /** Test hook: refuse agent legs as the real node does with this registrar setting (TC-056). */
    public void setAgentRegistrar(AgentRegistrar r) {
        agentRegistrar = r;
    }

    /** The null recording sink is configured (the fake's profile, tests). */
    public void setRecordingNullSink(boolean on) {
        nullSink = on;
    }

    @Override
    public boolean recordingNullSink() {
        return nullSink;
    }

    @Override
    public void startRecording(String legId, String recordingId, long epoch) throws Unavailable {
        need(false, true);
        if (!nullSink) {
            throw new Unavailable(Part.RECORDING);
        }
    }

    @Override
    public RecordingResult stopRecording(String legId, long epoch) throws Unavailable {
        need(false, true);
        return null;
    }

    /** Test hook (the scripts' {@code reinvite_hold}): the far end held or resumed. */
    public void remoteHold(String legId, boolean on) {
        Listener li = listener;
        if (li != null) {
            li.remoteHold(legId, on);
        }
    }

    /** Test hook (the scripts' {@code media_inactive} / {@code media_active}). */
    public void mediaActivity(String legId, boolean active) {
        Listener li = listener;
        if (li != null) {
            li.mediaActivity(legId, active);
        }
    }

    /** Test hook (the scripts' {@code dtmf_action_match}): a code matched; an applied mute action changes the mute. */
    public void dtmfActionMatch(String legId, String action, String result) {
        boolean m = muted.getOrDefault(legId, false);
        if ("APPLIED".equals(result)) {
            m = switch (action) {
                case "MUTE" -> true;
                case "UNMUTE" -> false;
                case "MUTE_TOGGLE" -> !m;
                default -> m;
            };
            muted.put(legId, m);
        }
        Listener li = listener;
        if (li != null) {
            li.dtmfAction(legId, action, result, m, "");
        }
    }

    @Override
    public boolean stopPlay(String legId, long epoch) throws Unavailable {
        need(false, true);
        return playing.remove(legId) != null;
    }

    @Override
    public boolean mute(String legId, boolean m, long epoch) throws Unavailable {
        need(false, true);
        Boolean before = muted.put(legId, m);
        return (before != null && before) != m;
    }

    @Override
    public void sendDtmf(String legId, String digits, long epoch) throws Unavailable {
        need(false, true);
    }

    @Override
    public void reportDigits(String legId, String mode, long epoch) throws Unavailable {
        need(false, true);
        digitModes.put(legId, mode);
    }

    /** Test hook: the leg's digit reporting now. */
    public String digitMode(String legId) {
        return digitModes.getOrDefault(legId, "off");
    }

    /**
     * Test hook (and the scripts' {@code play_done}): the leg's play ended; an asynchronous media
     * reports it under the id of the play it last started, as parity-ms does.
     */
    public void playDone(String legId, String reason, long playedMs) {
        playing.remove(legId);
        Listener li = listener;
        if (li == null) {
            return;
        }
        String id = asyncPlays ? currentIds.get(legId) : null;
        if (id != null) {
            li.playFinished(legId, reason, playedMs, id);
        } else {
            li.playFinished(legId, reason, playedMs);
        }
    }

    /** Test hook: a play ended, reported with its id (as parity-ms does from requirement 42's fix round). */
    public void playDone(String legId, String reason, long playedMs, String playId) {
        Listener li = listener;
        if (li != null) {
            li.playFinished(legId, reason, playedMs, playId);
        }
    }

    /** Test hook (the scripts' {@code dtmf}): a digit from the leg, if its reporting allows it. */
    public void remoteDigit(String legId, String digit, String source) {
        String mode = digitMode(legId);
        Listener li = listener;
        if (li != null && !"off".equals(mode)) {
            li.digit(legId, "masked".equals(mode) ? "*" : digit, 100, source);
        }
    }

    @Override
    public int freeLegs() {
        lock.lock();
        try {
            return freeLegs;
        } finally {
            lock.unlock();
        }
    }

    public boolean freeswitchUp() {
        return freeswitchUp;
    }

    public boolean parityMsUp() {
        return parityMsUp;
    }

    // ---- what a rebuild reads ------------------------------------------------------------

    /** Copies of the live channels, in creation order. */
    @Override
    public List<Channel> channels() {
        lock.lock();
        try {
            return channels.values().stream().map(Channel::copy).toList();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public java.util.Optional<Channel> channel(String legId) {
        lock.lock();
        try {
            Channel c = channels.get(legId);
            return c == null ? java.util.Optional.empty() : java.util.Optional.of(c.copy());
        } finally {
            lock.unlock();
        }
    }

    /** The CDR files written for ended channels. */
    @Override
    public List<Journal.LegEnded> cdrs(long sinceMs) {
        lock.lock();
        try {
            return List.copyOf(cdrs);
        } finally {
            lock.unlock();
        }
    }

    /** parity-ms's fence for a session's media, or -1 without media. */
    @Override
    public long mediaEpoch(String interactionId) {
        lock.lock();
        try {
            return mediaEpochs.getOrDefault(interactionId, -1L);
        } finally {
            lock.unlock();
        }
    }

    // ---- the far ends and the node (a script's inject) -----------------------------------

    /** A carrier's INVITE parked by the dialplan. */
    public void park(Channel c) {
        lock.lock();
        try {
            channels.put(c.legId, c);
        } finally {
            lock.unlock();
        }
        Listener l = listener;
        if (l != null) {
            l.parked(c.copy());
        }
    }

    /** The far end of an originated leg rings (180). */
    public void remoteRing(String legId) {
        boolean ok = with(legId, c -> c.state = LegState.RINGING);
        Listener l = listener;
        if (ok && l != null) {
            l.ringing(legId);
        }
    }

    /** The far end answers an originated leg. */
    public void remoteAnswer(String legId) {
        long[] at = {0};
        boolean ok = with(legId, c -> {
            c.state = LegState.ANSWERED;
            c.answeredAtMs = clock.nowMs();
            at[0] = c.answeredAtMs;
        });
        Listener l = listener;
        if (ok && l != null) {
            l.answered(legId, at[0]);
        }
    }

    /** The far end ends a leg: a failure before answer, a hangup after, or a CANCEL. */
    public void remoteEnd(String legId, EndCause cause, int sipStatus) {
        List<Ended> ended;
        lock.lock();
        try {
            ended = endWithHook(legId, cause, sipStatus, EndedBy.REMOTE);
        } finally {
            lock.unlock();
        }
        Listener l = listener;
        if (!ended.isEmpty() && l != null) {
            l.ended(ended);
        }
    }

    /** A far end's re-INVITE that freeswitch answered itself ((c+), TC PLAN R2-1). */
    public void reinvite(String legId, RemoteMediaKind kind) {
        Listener l = listener;
        if (l != null) {
            l.remoteMedia(legId, kind);
        }
    }

    public void setFreeswitchUp(boolean up) {
        freeswitchUp = up;
    }

    public void setParityMsUp(boolean up) {
        parityMsUp = up;
    }

    public void setFreeLegs(int n) {
        lock.lock();
        try {
            freeLegs = n;
        } finally {
            lock.unlock();
        }
    }

    /** freeswitch died: every channel is gone, no CDR written. */
    public void loseFreeswitch() {
        lock.lock();
        try {
            channels.clear();
        } finally {
            lock.unlock();
        }
        Listener l = listener;
        if (l != null) {
            l.lost(Part.FREESWITCH);
        }
    }

    /** parity-ms died: every session's media is gone. */
    public void loseParityMs() {
        lock.lock();
        try {
            mediaEpochs.clear();
        } finally {
            lock.unlock();
        }
        Listener l = listener;
        if (l != null) {
            l.lost(Part.PARITY_MS);
        }
    }

    /** Invariants a script's end checks: no leg originated twice. */
    @Override
    public List<String> invariantViolations() {
        lock.lock();
        try {
            List<String> out = new ArrayList<>();
            originates.forEach((leg, n) -> {
                if (n > 1) {
                    out.add("leg " + leg + " was originated " + n + " times");
                }
            });
            return out;
        } finally {
            lock.unlock();
        }
    }

    // ---- internals ------------------------------------------------------------------------

    private void need(boolean freeswitch, boolean parityMs) throws Unavailable {
        if (freeswitch && !freeswitchUp) {
            throw new Unavailable(Part.FREESWITCH);
        }
        if (parityMs && !parityMsUp) {
            throw new Unavailable(Part.PARITY_MS);
        }
    }

    private boolean with(String legId, java.util.function.Consumer<Channel> f) {
        lock.lock();
        try {
            Channel c = channels.get(legId);
            if (c == null) {
                return false;
            }
            f.accept(c);
            return true;
        } finally {
            lock.unlock();
        }
    }

    /** Ends a leg and, for the customer's, every other leg of its call. Lock held. */
    private List<Ended> endWithHook(String legId, EndCause cause, int sipStatus, EndedBy by) {
        Channel c = channels.get(legId);
        if (c == null) {
            return List.of();
        }
        List<Ended> out = new ArrayList<>();
        out.add(end(legId, cause, sipStatus, by));
        if (c.role == Role.CUSTOMER) {
            for (Channel other : List.copyOf(channels.values())) {
                if (other.interactionId.equals(c.interactionId)) {
                    EndCause oc = other.state == LegState.ANSWERED ? EndCause.NORMAL : EndCause.CANCELED;
                    out.add(end(other.legId, oc, 0, EndedBy.SYSTEM));
                }
            }
        }
        return out;
    }

    /** Ends one channel and writes its CDR. Lock held. */
    private Ended end(String legId, EndCause cause, int sipStatus, EndedBy by) {
        Channel c = channels.remove(legId);
        if (c == null) {
            return null;
        }
        if (c.state == LegState.ANSWERED || !c.inbound) {
            freeLegs++;
        }
        long now = clock.nowMs();
        cdrs.add(new Journal.LegEnded(c.tenantId, c.interactionId, c.legId, c.role, c.endpoint, c.sipCallId, cause,
                sipStatus, by, c.answeredAtMs, now, c.addLegFingerprint));
        if (channels.values().stream().noneMatch(o -> o.interactionId.equals(c.interactionId))) {
            mediaEpochs.remove(c.interactionId);
        }
        return new Ended(legId, cause, sipStatus, by, now);
    }
}
