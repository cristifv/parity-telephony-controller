package parity.telephonycontroller.scripts;

import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import parity.contracts.telephony.v1.AddLegRequest;
import parity.contracts.telephony.v1.AnswerLegRequest;
import parity.contracts.telephony.v1.CodecPolicy;
import parity.contracts.telephony.v1.Endpoint;
import parity.contracts.telephony.v1.LegRole;
import parity.contracts.telephony.v1.LegState;
import parity.contracts.telephony.v1.SessionOwner;
import parity.contracts.telephony.v1.TelephonyServiceGrpc;
import parity.contracts.telephony.v1.WatchRequest;
import parity.contracts.telephony.v1.WatchResponse;
import parity.telephonycontroller.model.UuidV7;
import parity.telephonycontroller.session.TelephonyNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * Plays IE against TC on real freeswitch for the evidence tests (TC ADR 0002 §10): one replica's
 * Watch stream, arrivals from the SIP agent, owner claims in etcd, and AnswerLeg and AddLeg over
 * gRPC, in real time (the scripts' clock is set to now and not advanced, so no deadline fires).
 */
final class LiveCalls implements AutoCloseable {

    static final String TENANT = "acme";
    static final String BIN = "bin-a";
    static final String REPLICA = "ie-bin-a-1";
    private static final JsonMapper JSON = JsonMapper.builder().build();

    /** One inbound call as IE knows it. */
    static final class Inbound {
        final String interactionId = UuidV7.create();
        final String customerLeg = UuidV7.create();
        final String sipCallId = UUID.randomUUID() + "@carrier.test";
        volatile long epoch;
    }

    private final RealNodeTarget target;
    private final List<WatchResponse> events = new CopyOnWriteArrayList<>();
    private final Map<String, Long> seq = new ConcurrentHashMap<>();
    private final AtomicInteger commands = new AtomicInteger();
    private volatile String incarnation;
    private volatile TelephonyServiceGrpc.TelephonyServiceBlockingStub stub;
    private volatile io.grpc.Context.CancellableContext watch;

    LiveCalls(RealNodeTarget target) {
        this.target = target;
    }

    /** The engine's identity for a run: what {@link #begin} gave it, or a new incarnation. */
    static TelephonyNode.Config config(String incarnation) {
        return new TelephonyNode.Config("media-1", "tc-evidence", java.util.Set.of(BIN), incarnation, List.of());
    }

    /** A fresh run: TC reset with its clock at now, and the replica's Watch stream open. */
    void begin() throws Exception {
        ObjectNode given = JSON.createObjectNode();
        given.put("start_ms", Long.toString(System.currentTimeMillis()));
        given.put("incarnation", UuidV7.create());
        given.put("node_id", "media-1");
        given.put("version", "tc-evidence");
        given.putArray("bins").add(BIN);
        given.putArray("replicas").addObject().put("bin", BIN).put("replica_id", REPLICA);
        given.putArray("owners");
        given.put("free_legs", 5000);
        target.reset(given);
        rewatch();
    }

    /** Opens (or reopens, after a TC restart) the replica's Watch stream. */
    void rewatch() throws InterruptedException {
        if (watch != null) {
            watch.cancel(null);
        }
        events.clear();
        seq.clear();
        incarnation = null;
        Metadata m = new Metadata();
        m.put(Metadata.Key.of("x-parity-test-peer", Metadata.ASCII_STRING_MARSHALLER), "parity-interaction-engine." + BIN);
        m.put(Metadata.Key.of("tenant-id", Metadata.ASCII_STRING_MARSHALLER), TENANT);
        stub = TelephonyServiceGrpc.newBlockingStub(target.channel())
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(m));
        TelephonyServiceGrpc.TelephonyServiceStub async = TelephonyServiceGrpc.newStub(target.channel())
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(m));
        io.grpc.Context.CancellableContext ctx = io.grpc.Context.current().withCancellation();
        watch = ctx;
        ctx.run(() -> async.watch(WatchRequest.newBuilder().setReplicaId(REPLICA).setBin(BIN).build(),
                new StreamObserver<>() {
                    @Override
                    public void onNext(WatchResponse r) {
                        if (incarnation == null) {
                            incarnation = r.getIncarnation();
                        }
                        if (r.hasSession() && r.getDeliverySeq() > 0) {
                            seq.merge(r.getSession().getInteractionId(), r.getDeliverySeq(), Math::max);
                        }
                        events.add(r);
                    }

                    @Override
                    public void onError(Throwable t) {
                        // a restart closes it; the test reopens it
                    }

                    @Override
                    public void onCompleted() {
                        // as above
                    }
                }));
        await(() -> incarnation != null, 10_000, "the Watch stream never opened");
    }

    /** A call from the carrier with this SDP: parked, offered to the bin, then claimed by the replica. */
    Inbound arrive(String sdp) throws Exception {
        return arrive(sdp, Map.of());
    }

    /** The same, with extra headers on the carrier's INVITE (names as given, so case variants repeat one). */
    Inbound arrive(String sdp, Map<String, String> extraHeaders) throws Exception {
        Inbound c = new Inbound();
        ObjectNode a = JSON.createObjectNode().put("leg_id", c.customerLeg).put("interaction_id", c.interactionId)
                .put("tenant_id", TENANT).put("bin", BIN).put("called_number", "+447700900100")
                .put("caller_number", "+447700900123").put("sip_call_id", c.sipCallId).put("trunk_id", "carrier-a")
                .put("sdp", sdp);
        if (!extraHeaders.isEmpty()) {
            tools.jackson.databind.node.ArrayNode headers = a.putArray("headers");
            extraHeaders.forEach((k, v) -> headers.addObject().put("name", k).put("value", v));
        }
        target.inject("arrival", a);
        awaitEvent(r -> r.hasArrived() && r.getArrived().getLegId().equals(c.customerLeg), 5_000, "no Arrived");
        c.epoch = target.writer().claimNow(c.interactionId,
                new parity.telephonycontroller.owner.OwnerRecord(0, TENANT, BIN, REPLICA, "media-1"));
        return c;
    }

    void answer(Inbound c) {
        stub.withDeadlineAfter(10, TimeUnit.SECONDS).answerLeg(AnswerLegRequest.newBuilder().setOwner(owner(c))
                .setCommandId(commandId()).setLegId(c.customerLeg).build());
    }

    /** RingLeg: the first verified command, so TC claims the arrival (its safety hangup becomes the backstop). */
    void ring(Inbound c) {
        stub.withDeadlineAfter(10, TimeUnit.SECONDS).ringLeg(parity.contracts.telephony.v1.RingLegRequest.newBuilder()
                .setOwner(owner(c)).setCommandId(commandId()).setLegId(c.customerLeg).build());
    }

    /** AddLeg for an agent; returns the new leg's id (TC dials it now). */
    String addAgent(Inbound c, String contact, Endpoint endpoint) {
        String leg = UuidV7.create();
        stub.withDeadlineAfter(10, TimeUnit.SECONDS).addLeg(AddLegRequest.newBuilder().setOwner(owner(c))
                .setCommandId(commandId()).setLegId(leg).setRole(LegRole.LEG_ROLE_AGENT).setAgentContact(contact)
                .setRingTimeoutS(30).setEndpoint(endpoint).setCodecPolicy(CodecPolicy.CODEC_POLICY_G711_ONLY).build());
        return leg;
    }

    /** AddLeg for an agent with its DTMF action codes (P5); returns the new leg's id. */
    String addAgent(Inbound c, String contact, Endpoint endpoint, parity.contracts.telephony.v1.DtmfActions codes) {
        String leg = UuidV7.create();
        stub.withDeadlineAfter(10, TimeUnit.SECONDS).addLeg(AddLegRequest.newBuilder().setOwner(owner(c))
                .setCommandId(commandId()).setLegId(leg).setRole(LegRole.LEG_ROLE_AGENT).setAgentContact(contact)
                .setRingTimeoutS(30).setEndpoint(endpoint).setCodecPolicy(CodecPolicy.CODEC_POLICY_G711_ONLY)
                .setDtmfActions(codes).build());
        return leg;
    }

    boolean stopPlay(Inbound c, String leg, String playId) {
        return stub.withDeadlineAfter(10, TimeUnit.SECONDS).stopPlay(parity.contracts.telephony.v1.StopPlayRequest.newBuilder()
                .setOwner(owner(c)).setCommandId(commandId()).setLegId(leg).setPlayId(playId).build()).getAlreadyGone();
    }

    /** StartRecording on the null sink: a test key (32 bytes), the null sink's key id. */
    void startRecording(Inbound c, String leg, String recordingId) {
        stub.withDeadlineAfter(10, TimeUnit.SECONDS).startRecording(parity.contracts.telephony.v1.StartRecordingRequest
                .newBuilder().setOwner(owner(c)).setCommandId(commandId()).setLegId(leg).setRecordingId(recordingId)
                .setKey(parity.contracts.telephony.v1.RecordingKey.newBuilder()
                        .setDek(com.google.protobuf.ByteString.copyFrom(new byte[32]))
                        .setKekId(parity.telephonycontroller.session.TelephonyNode.NULL_SINK_KEK))
                .build());
    }

    /** TC's snapshot of a session. */
    parity.contracts.telephony.v1.Session session(Inbound c) {
        return stub.withDeadlineAfter(10, TimeUnit.SECONDS).getSession(parity.contracts.telephony.v1.GetSessionRequest
                .newBuilder().setTenantId(TENANT).setInteractionId(c.interactionId).setReplicaId(REPLICA).build())
                .getSession();
    }

    // ---- Step 4: media commands -------------------------------------------------------------------

    void play(Inbound c, String leg, String playId, String prompt, boolean loop) {
        stub.withDeadlineAfter(10, TimeUnit.SECONDS).play(parity.contracts.telephony.v1.PlayRequest.newBuilder()
                .setOwner(owner(c)).setCommandId(commandId()).setLegId(leg).setPlayId(playId).setPromptId(prompt)
                .setLoop(loop).build());
    }

    void digits(Inbound c, String leg, parity.contracts.telephony.v1.DigitReporting mode, String collection) {
        stub.withDeadlineAfter(10, TimeUnit.SECONDS).setDigitReporting(parity.contracts.telephony.v1.SetDigitReportingRequest
                .newBuilder().setOwner(owner(c)).setCommandId(commandId()).setLegId(leg).setMode(mode)
                .setCollectionId(collection).setMaxDurationS(60).build());
    }

    boolean mute(Inbound c, String leg, boolean muted) {
        return stub.withDeadlineAfter(10, TimeUnit.SECONDS).setMute(parity.contracts.telephony.v1.SetMuteRequest.newBuilder()
                .setOwner(owner(c)).setCommandId(commandId()).setLegId(leg).setMuted(muted).build()).getChanged();
    }

    void dtmfActions(Inbound c, String leg, String sequence, parity.contracts.telephony.v1.DtmfActionKind action) {
        stub.withDeadlineAfter(10, TimeUnit.SECONDS).setDtmfActions(parity.contracts.telephony.v1.SetDtmfActionsRequest
                .newBuilder().setOwner(owner(c)).setCommandId(commandId()).setLegId(leg)
                .setActions(parity.contracts.telephony.v1.DtmfActions.newBuilder().addBindings(
                        parity.contracts.telephony.v1.DtmfActionBinding.newBuilder().setDtmfSequence(sequence)
                                .setAction(action)))
                .build());
    }

    boolean hold(Inbound c, boolean on, String music) {
        return stub.withDeadlineAfter(10, TimeUnit.SECONDS).setHold(parity.contracts.telephony.v1.SetHoldRequest.newBuilder()
                .setOwner(owner(c)).setCommandId(commandId()).setOn(on).setMusicPromptId(music).build()).getChanged();
    }

    /** The last state TC reported for a leg on the stream (UNSPECIFIED if none). */
    LegState state(String legId) {
        LegState s = LegState.LEG_STATE_UNSPECIFIED;
        for (WatchResponse r : events) {
            if (r.hasLeg() && r.getLeg().getLegId().equals(legId)) {
                s = r.getLeg().getState();
            }
        }
        return s;
    }

    void awaitState(String legId, LegState state, long ms) throws InterruptedException {
        await(() -> state(legId) == state, ms, "leg never reached " + state);
    }

    List<WatchResponse> events() {
        return events;
    }

    WatchResponse awaitEvent(Predicate<WatchResponse> p, long ms, String what) throws InterruptedException {
        long until = System.currentTimeMillis() + ms;
        while (System.currentTimeMillis() < until) {
            for (WatchResponse r : events) {
                if (p.test(r)) {
                    return r;
                }
            }
            Thread.sleep(20);
        }
        throw new AssertionError(what);
    }

    private SessionOwner owner(Inbound c) {
        return SessionOwner.newBuilder().setTenantId(TENANT).setInteractionId(c.interactionId).setOwnerEpoch(c.epoch)
                .setReplicaId(REPLICA).setAckedDeliverySeq(seq.getOrDefault(c.interactionId, 0L))
                .setAckedIncarnation(incarnation).build();
    }

    private String commandId() {
        return REPLICA + ":" + UuidV7.create() + "-" + commands.incrementAndGet();
    }

    static void await(BooleanSupplier c, long ms, String what) throws InterruptedException {
        long until = System.currentTimeMillis() + ms;
        while (System.currentTimeMillis() < until) {
            if (c.getAsBoolean()) {
                return;
            }
            Thread.sleep(20);
        }
        if (!c.getAsBoolean()) {
            throw new AssertionError(what);
        }
    }

    static void sleep(Duration d) throws InterruptedException {
        Thread.sleep(d.toMillis());
    }

    @Override
    public void close() {
        if (watch != null) {
            watch.cancel(null);
        }
    }
}
