package parity.telephonycontroller.fake;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import parity.telephonycontroller.model.Kinds.EndCause;
import parity.telephonycontroller.model.Kinds.Endpoint;
import parity.telephonycontroller.model.Kinds.LegState;
import parity.telephonycontroller.model.Kinds.RemoteMediaKind;
import parity.telephonycontroller.model.Kinds.Role;
import parity.telephonycontroller.node.NodePort;
import parity.telephonycontroller.node.SimulatedNode;
import parity.telephonycontroller.owner.OwnerDirectory;
import parity.telephonycontroller.owner.OwnerDirectoryUnavailable;
import parity.telephonycontroller.owner.OwnerRecord;
import parity.telephonycontroller.owner.OwnerWriter;
import parity.telephonycontroller.session.EngineHolder;
import parity.telephonycontroller.session.Requests;
import parity.telephonycontroller.session.TelephonyException;
import parity.telephonycontroller.session.TelephonyNode;
import parity.telephonycontroller.time.ManualClock;
import tools.jackson.databind.JsonNode;

/**
 * What a script's {@code given}, {@code inject} and {@code advance} do (TC ADR 0001 §6), for
 * {@code fake-telephony-controller}'s control endpoint and for the runner that plays the scripts
 * against TC's own server. Never wired outside the {@code fake} profile and tests.
 */
public final class ScriptHooks {

    /** Injects for what later steps build (none left after step 4). */
    private static final Set<String> LATER = Set.of();

    private final EngineHolder holder;
    private final ManualClock clock;
    private final OwnerWriter writer;
    private final OwnerDirectory owners;
    private final Runnable clearJournal;

    public ScriptHooks(EngineHolder holder, ManualClock clock, OwnerWriter writer, OwnerDirectory owners,
            Runnable clearJournal) {
        this.holder = holder;
        this.clock = clock;
        this.writer = writer;
        this.owners = owners;
        this.clearJournal = clearJournal;
    }

    /** A fresh node from a script's {@code given}; returns script epoch → real create revision. */
    public Map<Long, Long> reset(JsonNode given) {
        // The new node keeps the recording sink the fake was configured with (the null one: nothing written).
        boolean nullSink = holder.node() != null && holder.node().recordingNullSink();
        return reset(given, freeLegs -> {
            SimulatedNode n = new SimulatedNode(clock, freeLegs);
            n.setRecordingNullSink(nullSink);
            return n;
        });
    }

    /** As {@link #reset(JsonNode)}, with the node the caller provides (the real-node runner's). */
    public Map<Long, Long> reset(JsonNode given, java.util.function.IntFunction<NodePort> node) {
        clock.reset(Long.parseLong(given.get("start_ms").asString()));
        writer.clear();
        clearJournal.run();
        for (JsonNode r : given.path("replicas")) {
            writer.register(r.get("bin").asString(), r.get("replica_id").asString());
        }
        Map<Long, Long> epochs = new LinkedHashMap<>();
        for (JsonNode o : given.path("owners")) {
            long epoch = Long.parseLong(o.get("owner_epoch").asString());
            epochs.put(epoch, claim(o, epoch));
        }
        int freeLegs = given.has("free_legs") ? given.get("free_legs").asInt() : 1000;
        Set<String> bins = new LinkedHashSet<>();
        given.get("bins").forEach(b -> bins.add(b.asString()));
        TelephonyNode.Config config = new TelephonyNode.Config(given.get("node_id").asString(),
                given.get("version").asString(), bins, given.get("incarnation").asString(), List.of());
        holder.reset(config, node.apply(freeLegs));
        return epochs;
    }

    /** One {@code inject}; for {@code owner_claim}, the record's real create revision (else 0). */
    public long inject(String what, JsonNode a) {
        if (LATER.contains(what)) {
            throw new UnsupportedOperationException("this inject comes with the step that builds it");
        }
        switch (what) {
            case "stream_drop" -> holder.get().dropStream(s(a, "replica_id"));
            case "tc_restart" -> holder.restart(s(a, "incarnation"), Long.parseLong(s(a, "rebuild_ms")));
            case "tc_crash_mid_fence_raise" -> crashMidRaise(a);
            case "owner_claim" -> {
                return claim(a, Long.parseLong(s(a, "owner_epoch")));
            }
            case "owner_delete" -> writer.delete(s(a, "interaction_id"));
            case "etcd_down" -> writer.setAvailable(false);
            case "etcd_up" -> writer.setAvailable(true);
            case "drain" -> holder.get().drain();
            case "mailbox_full" -> holder.get().mailboxCapacity(s(a, "tenant_id"), s(a, "interaction_id"), 0);
            case "mailbox_free" -> holder.get().mailboxCapacity(s(a, "tenant_id"), s(a, "interaction_id"),
                    parity.telephonycontroller.session.TelephonyNode.MAILBOX_CAPACITY);
            default -> farEnd(what, a);
        }
        return 0;
    }

    /** What the far ends and the simulated node do (the real-node runner does these itself). */
    private void farEnd(String what, JsonNode a) {
        SimulatedNode node = (SimulatedNode) holder.node();
        switch (what) {
            case "arrival" -> {
                List<String[]> headers = new ArrayList<>();
                a.path("headers").forEach(h -> headers.add(new String[] {h.get("name").asString(), h.get("value").asString()}));
                node.park(new NodePort.Channel(s(a, "leg_id"), s(a, "tenant_id"), s(a, "interaction_id"),
                        s(a, "bin"), Role.CUSTOMER, Endpoint.SIP, s(a, "sip_call_id"), true, clock.nowMs(),
                        s(a, "called_number"), s(a, "caller_number"), s(a, "trunk_id"), headers, null, LegState.PARKED));
            }
            case "leg_ring" -> node.remoteRing(s(a, "leg_id"));
            case "leg_answer" -> node.remoteAnswer(s(a, "leg_id"));
            case "leg_fail" -> node.remoteEnd(s(a, "leg_id"), cause(a, EndCause.SYSTEM), a.path("sip_status").asInt(0));
            case "leg_hangup" -> node.remoteEnd(s(a, "leg_id"), cause(a, EndCause.NORMAL), a.path("sip_status").asInt(0));
            case "caller_cancel" -> node.remoteEnd(s(a, "leg_id"), EndCause.CANCELED, 487);
            case "reinvite_address" -> node.reinvite(s(a, "leg_id"), RemoteMediaKind.ADDRESS);
            case "reinvite_codec" -> node.reinvite(s(a, "leg_id"), RemoteMediaKind.UNANSWERABLE);
            case "freeswitch_unreachable" -> node.setFreeswitchUp(false);
            case "freeswitch_reachable" -> node.setFreeswitchUp(true);
            case "parity_ms_unreachable" -> node.setParityMsUp(false);
            case "parity_ms_reachable" -> node.setParityMsUp(true);
            case "freeswitch_lost" -> node.loseFreeswitch();
            case "parity_ms_lost" -> node.loseParityMs();
            case "capacity" -> node.setFreeLegs(a.get("free_legs").asInt());
            case "play_done" -> node.playDone(s(a, "leg_id"), s(a, "reason"), a.path("played_ms").asLong(0));
            case "prompt_missing" -> node.promptMissing(s(a, "prompt_id"));
            case "dtmf" -> node.remoteDigit(s(a, "leg_id"), s(a, "digit"), source(a));
            case "reinvite_hold" -> node.remoteHold(s(a, "leg_id"), a.path("on").asBoolean(false));
            case "media_inactive" -> node.mediaActivity(s(a, "leg_id"), false);
            case "media_active" -> node.mediaActivity(s(a, "leg_id"), true);
            case "dtmf_action_match" -> node.dtmfActionMatch(s(a, "leg_id"),
                    s(a, "action").substring("DTMF_ACTION_KIND_".length()),
                    s(a, "result").substring("DTMF_ACTION_RESULT_".length()));
            default -> throw new IllegalArgumentException("unknown inject");
        }
    }

    public void advance(long ms) {
        clock.advance(ms);
    }

    public List<String> invariants() {
        return holder.get().invariantViolations();
    }

    /** TC dies while raising a claim's fence, after {@code after_legs} legs; then restarts. */
    private void crashMidRaise(JsonNode a) {
        JsonNode c = a.get("claim");
        Requests.Owner owner = new Requests.Owner(s(c, "tenant_id"), s(c, "interaction_id"),
                Long.parseLong(s(c, "owner_epoch")), s(c, "replica_id"), 0, "");
        String bin;
        try {
            bin = owners.owner(owner.interactionId()).map(OwnerRecord::bin).orElseThrow();
        } catch (OwnerDirectoryUnavailable e) {
            throw new IllegalStateException("etcd is down", e);
        }
        TelephonyNode engine = holder.get();
        engine.crashAfterLegs(a.get("after_legs").asInt());
        try {
            engine.claim(new Requests.Caller(bin), owner);
        } catch (TelephonyNode.SimulatedCrash expected) {
            // The process is gone mid-raise.
        } catch (TelephonyException e) {
            throw new IllegalStateException("the claim was refused: " + e.code(), e);
        } finally {
            engine.crashAfterLegs(-1);
        }
        holder.restart(s(a, "incarnation"), Long.parseLong(s(a, "rebuild_ms")));
    }

    private long claim(JsonNode o, long epoch) {
        return writer.claim(s(o, "interaction_id"), epoch,
                new OwnerRecord(epoch, s(o, "tenant_id"), s(o, "bin"), s(o, "replica_id"), s(o, "node_id")));
    }

    /** A {@code dtmf} inject's source as parity-ms names it (RFC 4733 unless said). */
    public static String source(JsonNode a) {
        return "DIGIT_SOURCE_INBAND".equals(s(a, "source")) ? "inband" : "rfc4733";
    }

    private static EndCause cause(JsonNode a, EndCause otherwise) {
        JsonNode c = a.get("cause");
        return c == null ? otherwise : EndCause.valueOf(c.asString().substring("END_CAUSE_".length()));
    }

    private static String s(JsonNode n, String name) {
        JsonNode v = n.get(name);
        return v == null || v.isNull() ? null : v.asString();
    }
}
