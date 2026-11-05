package parity.telephonycontroller.api;

import com.google.protobuf.Message;
import com.google.protobuf.Descriptors.FieldDescriptor;
import io.grpc.Context;
import io.grpc.Deadline;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parity.contracts.telephony.v1.AcknowledgeRequest;
import parity.contracts.telephony.v1.AcknowledgeResponse;
import parity.contracts.telephony.v1.AddLegRequest;
import parity.contracts.telephony.v1.AddLegResponse;
import parity.contracts.telephony.v1.AnswerLegRequest;
import parity.contracts.telephony.v1.AnswerLegResponse;
import parity.contracts.telephony.v1.ClaimSessionRequest;
import parity.contracts.telephony.v1.ClaimSessionResponse;
import parity.contracts.telephony.v1.DeclineArrivalRequest;
import parity.contracts.telephony.v1.DeclineArrivalResponse;
import parity.contracts.telephony.v1.DeclineReason;
import parity.contracts.telephony.v1.EndSessionRequest;
import parity.contracts.telephony.v1.EndSessionResponse;
import parity.contracts.telephony.v1.GetSessionRequest;
import parity.contracts.telephony.v1.GetSessionResponse;
import parity.contracts.telephony.v1.RejectLegRequest;
import parity.contracts.telephony.v1.RejectLegResponse;
import parity.contracts.telephony.v1.RemoveLegRequest;
import parity.contracts.telephony.v1.RemoveLegResponse;
import parity.contracts.telephony.v1.RingLegRequest;
import parity.contracts.telephony.v1.RingLegResponse;
import parity.contracts.telephony.v1.SessionOwner;
import parity.contracts.telephony.v1.TelephonyServiceGrpc;
import parity.contracts.telephony.v1.WatchRequest;
import parity.contracts.telephony.v1.WatchResponse;
import parity.telephonycontroller.session.Code;
import parity.telephonycontroller.session.EngineHolder;
import parity.telephonycontroller.session.Requests;
import parity.telephonycontroller.session.TelephonyException;
import parity.telephonycontroller.session.TelephonyNode;
import parity.telephonycontroller.session.Views;

/**
 * {@code parity.telephony.v1.TelephonyService} (TC ADR 0001). Each call: the field rules, then the
 * engine. Step 4 adds the media commands (Play, StopPlay, SetMute, SendDtmf, SetDigitReporting,
 * SetHold; TC ADR 0003), SetDtmfActions, and StartRecording/StopRecording on the null sink (local test
 * profiles only, until R9). The RPCs later steps build (isolation, monitor modes, a redirect) keep the
 * generated default, {@code UNIMPLEMENTED}.
 */
public final class TelephonyGrpcService extends TelephonyServiceGrpc.TelephonyServiceImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(TelephonyGrpcService.class);
    private static final java.util.regex.Pattern NAME = java.util.regex.Pattern.compile("^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$");

    private final EngineHolder holder;
    private final int streamCapacity;

    TelephonyGrpcService(EngineHolder holder, int streamCapacity) {
        this.holder = holder;
        this.streamCapacity = streamCapacity;
    }

    @FunctionalInterface
    private interface Call<T> {
        T run() throws TelephonyException;
    }

    @Override
    public void watch(WatchRequest r, StreamObserver<WatchResponse> out) {
        ServerCallStreamObserver<WatchResponse> observer = (ServerCallStreamObserver<WatchResponse>) out;
        GrpcSink sink = new GrpcSink(observer, streamCapacity);
        TelephonyNode engine = holder.get();
        sink.start(() -> engine.streamClosed(r.getReplicaId(), sink));
        try {
            if (!NAME.matcher(r.getReplicaId()).matches() || !NAME.matcher(r.getBin()).matches()) {
                throw new TelephonyException(Code.INVALID_REQUEST);
            }
            engine.watch(caller(), r.getReplicaId(), r.getBin(), sink);
        } catch (TelephonyException e) {
            sink.close(e.code());
        } catch (RuntimeException e) {
            LOG.atError().addKeyValue("event", "watch.failed").addKeyValue("op", "Watch").setCause(e).log("a Watch failed");
            sink.close(Code.INTERNAL);
        }
    }

    @Override
    public void acknowledge(AcknowledgeRequest r, StreamObserver<AcknowledgeResponse> out) {
        unary(out, () -> {
            if (r.getAcksCount() > 500 || r.getAcksList().stream().anyMatch(a -> !RequestChecks.uuid(a.getInteractionId()))) {
                throw new TelephonyException(Code.INVALID_REQUEST);
            }
            List<Requests.Owner> acks = r.getAcksList().stream().map(ProtoMapping::owner).toList();
            return AcknowledgeResponse.newBuilder().setIgnored(holder.get().acknowledge(caller(), acks)).build();
        });
    }

    @Override
    public void claimSession(ClaimSessionRequest r, StreamObserver<ClaimSessionResponse> out) {
        unary(out, () -> {
            RequestChecks.tenant(tenant(), r.getOwner().getTenantId());
            RequestChecks.command(r.getOwner(), r.hasOwner(), "", false);
            TelephonyNode engine = holder.get();
            Views.Claimed c = engine.claim(caller(), ProtoMapping.owner(r.getOwner()));
            return ClaimSessionResponse.newBuilder().setSession(ProtoMapping.session(c.session()))
                    .setExisting(c.existing()).setIncarnation(engine.incarnation()).build();
        });
    }

    @Override
    public void getSession(GetSessionRequest r, StreamObserver<GetSessionResponse> out) {
        unary(out, () -> {
            RequestChecks.tenant(tenant(), r.getTenantId());
            RequestChecks.interaction(r.getInteractionId());
            TelephonyNode engine = holder.get();
            Views.SessionView v = engine.get(caller(), r.getTenantId(), r.getInteractionId());
            return GetSessionResponse.newBuilder().setSession(ProtoMapping.session(v))
                    .setIncarnation(engine.incarnation()).build();
        });
    }

    @Override
    public void declineArrival(DeclineArrivalRequest r, StreamObserver<DeclineArrivalResponse> out) {
        if (r.getReason() == DeclineReason.DECLINE_REASON_WRONG_BIN) {
            out.onError(Status.UNIMPLEMENTED.withDescription("a redirect comes with its step").asRuntimeException());
            return;
        }
        unary(out, () -> {
            RequestChecks.tenant(tenant(), r.getTenantId());
            RequestChecks.decline(r);
            Views.Declined d = holder.get().decline(caller(), new Requests.Decline(r.getTenantId(),
                    r.getInteractionId(), r.getReplicaId(), r.getLegId(), r.getSipStatus()));
            return DeclineArrivalResponse.newBuilder().setExisting(d.existing()).setRejected(d.rejected()).build();
        });
    }

    @Override
    public void ringLeg(RingLegRequest r, StreamObserver<RingLegResponse> out) {
        unary(out, () -> {
            command(r.getOwner(), r.hasOwner(), r.getCommandId());
            RequestChecks.leg(r.getLegId());
            boolean existing = holder.get().ring(caller(), ProtoMapping.owner(r.getOwner()),
                    key(r.getCommandId(), "RingLeg", r), r.getLegId());
            return RingLegResponse.newBuilder().setExisting(existing).build();
        });
    }

    // ---- Step 4: a leg's media (TC ADR 0003 D6) ------------------------------------------------------

    @Override
    public void play(parity.contracts.telephony.v1.PlayRequest r,
            StreamObserver<parity.contracts.telephony.v1.PlayResponse> out) {
        unary(out, () -> {
            command(r.getOwner(), r.hasOwner(), r.getCommandId());
            RequestChecks.play(r.getLegId(), r.getPlayId(), r.getPromptId());
            TelephonyNode engine = holder.get();
            TelephonyNode.Played played = engine.play(caller(), ProtoMapping.owner(r.getOwner()),
                    key(r.getCommandId(), "Play", r), r.getLegId(), r.getPlayId(), r.getPromptId(), r.getLoop(),
                    r.getResultEvents());
            if (played.started() != null) {
                // An IE without result_events (before contracts 0.17.0): answered once the play started,
                // waiting outside the session's turn until its deadline; past it TC calls the play off.
                engine.awaitStart(played, ProtoMapping.owner(r.getOwner()), r.getLegId(), r.getPlayId(), legacyPlayWaitMs());
            }
            return parity.contracts.telephony.v1.PlayResponse.newBuilder().setExisting(played.existing()).build();
        });
    }

    @Override
    public void stopPlay(parity.contracts.telephony.v1.StopPlayRequest r,
            StreamObserver<parity.contracts.telephony.v1.StopPlayResponse> out) {
        unary(out, () -> {
            command(r.getOwner(), r.hasOwner(), r.getCommandId());
            RequestChecks.leg(r.getLegId());
            RequestChecks.leg(r.getPlayId());
            TelephonyNode.Stopped st = holder.get().stopPlay(caller(), ProtoMapping.owner(r.getOwner()),
                    key(r.getCommandId(), "StopPlay", r), r.getLegId(), r.getPlayId());
            return parity.contracts.telephony.v1.StopPlayResponse.newBuilder().setExisting(st.existing())
                    .setAlreadyGone(st.alreadyGone()).build();
        });
    }

    @Override
    public void setMute(parity.contracts.telephony.v1.SetMuteRequest r,
            StreamObserver<parity.contracts.telephony.v1.SetMuteResponse> out) {
        unary(out, () -> {
            command(r.getOwner(), r.hasOwner(), r.getCommandId());
            RequestChecks.leg(r.getLegId());
            boolean changed = holder.get().setMute(caller(), ProtoMapping.owner(r.getOwner()),
                    key(r.getCommandId(), "SetMute", r), r.getLegId(), r.getMuted());
            return parity.contracts.telephony.v1.SetMuteResponse.newBuilder().setChanged(changed).build();
        });
    }

    @Override
    public void sendDtmf(parity.contracts.telephony.v1.SendDtmfRequest r,
            StreamObserver<parity.contracts.telephony.v1.SendDtmfResponse> out) {
        unary(out, () -> {
            command(r.getOwner(), r.hasOwner(), r.getCommandId());
            RequestChecks.leg(r.getLegId());
            RequestChecks.digits(r.getDigits());
            boolean existing = holder.get().sendDtmf(caller(), ProtoMapping.owner(r.getOwner()),
                    key(r.getCommandId(), "SendDtmf", r), r.getLegId(), r.getDigits());
            return parity.contracts.telephony.v1.SendDtmfResponse.newBuilder().setExisting(existing).build();
        });
    }

    @Override
    public void setDigitReporting(parity.contracts.telephony.v1.SetDigitReportingRequest r,
            StreamObserver<parity.contracts.telephony.v1.SetDigitReportingResponse> out) {
        unary(out, () -> {
            command(r.getOwner(), r.hasOwner(), r.getCommandId());
            String mode = r.getMode().name().substring("DIGIT_REPORTING_".length());
            RequestChecks.digitReporting(r.getLegId(), mode, r.getCollectionId(), r.getMaxDurationS());
            boolean existing = holder.get().setDigitReporting(caller(), ProtoMapping.owner(r.getOwner()),
                    key(r.getCommandId(), "SetDigitReporting", r), r.getLegId(), mode, r.getCollectionId(),
                    r.getMaxDurationS());
            return parity.contracts.telephony.v1.SetDigitReportingResponse.newBuilder().setExisting(existing).build();
        });
    }

    @Override
    public void setDtmfActions(parity.contracts.telephony.v1.SetDtmfActionsRequest r,
            StreamObserver<parity.contracts.telephony.v1.SetDtmfActionsResponse> out) {
        unary(out, () -> {
            command(r.getOwner(), r.hasOwner(), r.getCommandId());
            RequestChecks.dtmfActions(r.getLegId(), r.getActions());
            Requests.Dtmf d = dtmf(r.getActions());
            boolean existing = holder.get().setDtmfActions(caller(), ProtoMapping.owner(r.getOwner()),
                    key(r.getCommandId(), "SetDtmfActions", r), r.getLegId(), d.bindings(), d.locks(), d.interDigitMs());
            return parity.contracts.telephony.v1.SetDtmfActionsResponse.newBuilder().setExisting(existing).build();
        });
    }

    /** The contract's DTMF codes and locks, without their enum prefixes. PCI: the codes are digits, never logged. */
    private static Requests.Dtmf dtmf(parity.contracts.telephony.v1.DtmfActions a) {
        java.util.List<parity.telephonycontroller.node.NodePort.DtmfBinding> bindings = a.getBindingsList().stream()
                .map(b -> new parity.telephonycontroller.node.NodePort.DtmfBinding(b.getDtmfSequence(),
                        b.getAction().name().substring("DTMF_ACTION_KIND_".length())))
                .toList();
        java.util.List<String> locks = a.getLocksList().stream().map(f -> f.name().substring("FACET_".length())).toList();
        return new Requests.Dtmf(bindings, locks, a.getInterDigitTimeoutMs());
    }

    @Override
    public void startRecording(parity.contracts.telephony.v1.StartRecordingRequest r,
            StreamObserver<parity.contracts.telephony.v1.StartRecordingResponse> out) {
        unary(out, () -> {
            command(r.getOwner(), r.hasOwner(), r.getCommandId());
            RequestChecks.startRecording(r);
            // The DEK stops here: the null sink encrypts nothing (it writes nothing), and R9's sink
            // will take it in memory only. Only its wrapped form and the key's id go on.
            boolean existing = holder.get().startRecording(caller(), ProtoMapping.owner(r.getOwner()),
                    key(r.getCommandId(), "StartRecording", r), r.getLegId(), r.getRecordingId(),
                    java.util.Base64.getEncoder().encodeToString(r.getKey().getWrappedDek().toByteArray()),
                    r.getKey().getKekId());
            return parity.contracts.telephony.v1.StartRecordingResponse.newBuilder().setExisting(existing).build();
        });
    }

    @Override
    public void stopRecording(parity.contracts.telephony.v1.StopRecordingRequest r,
            StreamObserver<parity.contracts.telephony.v1.StopRecordingResponse> out) {
        unary(out, () -> {
            command(r.getOwner(), r.hasOwner(), r.getCommandId());
            RequestChecks.leg(r.getRecordingId());
            Views.Gone g = holder.get().stopRecording(caller(), ProtoMapping.owner(r.getOwner()),
                    key(r.getCommandId(), "StopRecording", r), r.getRecordingId());
            return parity.contracts.telephony.v1.StopRecordingResponse.newBuilder().setExisting(g.existing())
                    .setAlreadyGone(g.alreadyGone()).build();
        });
    }

    @Override
    public void setHold(parity.contracts.telephony.v1.SetHoldRequest r,
            StreamObserver<parity.contracts.telephony.v1.SetHoldResponse> out) {
        unary(out, () -> {
            command(r.getOwner(), r.hasOwner(), r.getCommandId());
            RequestChecks.prompt(r.getMusicPromptId());
            boolean changed = holder.get().setHold(caller(), ProtoMapping.owner(r.getOwner()),
                    key(r.getCommandId(), "SetHold", r), r.getOn(), r.getMusicPromptId());
            return parity.contracts.telephony.v1.SetHoldResponse.newBuilder().setChanged(changed).build();
        });
    }

    @Override
    public void answerLeg(AnswerLegRequest r, StreamObserver<AnswerLegResponse> out) {
        unary(out, () -> {
            command(r.getOwner(), r.hasOwner(), r.getCommandId());
            RequestChecks.leg(r.getLegId());
            boolean existing = holder.get().answer(caller(), ProtoMapping.owner(r.getOwner()),
                    key(r.getCommandId(), "AnswerLeg", r), r.getLegId());
            return AnswerLegResponse.newBuilder().setExisting(existing).build();
        });
    }

    @Override
    public void rejectLeg(RejectLegRequest r, StreamObserver<RejectLegResponse> out) {
        unary(out, () -> {
            command(r.getOwner(), r.hasOwner(), r.getCommandId());
            RequestChecks.leg(r.getLegId());
            RequestChecks.rejectStatus(r.getSipStatus());
            boolean existing = holder.get().reject(caller(), ProtoMapping.owner(r.getOwner()),
                    key(r.getCommandId(), "RejectLeg", r), r.getLegId(), r.getSipStatus());
            return RejectLegResponse.newBuilder().setExisting(existing).build();
        });
    }

    @Override
    public void addLeg(AddLegRequest r, StreamObserver<AddLegResponse> out) {
        unary(out, () -> {
            command(r.getOwner(), r.hasOwner(), r.getCommandId());
            RequestChecks.addLeg(r);
            AddLegRequest natural = r.toBuilder().clearOwner().clearCommandId().build();
            Requests.AddLeg a = new Requests.AddLeg(r.getLegId(), ProtoMapping.role(r.getRole()),
                    ProtoMapping.endpoint(r.getEndpoint()), r.getRingTimeoutS(), sha256(deterministic(natural)),
                    r.hasPstn() ? null : r.getAgentContact(), r.hasPstn() ? r.getPstn().getNumber() : null,
                    r.hasPstn() ? r.getPstn().getTrunkId() : null, r.getCallerId(),
                    r.hasDtmfActions() ? dtmf(r.getDtmfActions()) : null);
            Views.LegResult res = holder.get().addLeg(caller(), ProtoMapping.owner(r.getOwner()),
                    key(r.getCommandId(), "AddLeg", r), a);
            AddLegResponse.Builder b = AddLegResponse.newBuilder().setExisting(res.existing())
                    .setState(ProtoMapping.state(res.state()));
            if (res.cause() != null) {
                b.setCause(ProtoMapping.cause(res.cause()));
            }
            return b.build();
        });
    }

    @Override
    public void removeLeg(RemoveLegRequest r, StreamObserver<RemoveLegResponse> out) {
        unary(out, () -> {
            command(r.getOwner(), r.hasOwner(), r.getCommandId());
            RequestChecks.leg(r.getLegId());
            RequestChecks.localCause(r.getCause());
            Views.Gone g = holder.get().removeLeg(caller(), ProtoMapping.owner(r.getOwner()),
                    key(r.getCommandId(), "RemoveLeg", r),
                    new Requests.RemoveLeg(r.getLegId(), ProtoMapping.cause(r.getCause())));
            return RemoveLegResponse.newBuilder().setExisting(g.existing()).setAlreadyGone(g.alreadyGone()).build();
        });
    }

    @Override
    public void endSession(EndSessionRequest r, StreamObserver<EndSessionResponse> out) {
        unary(out, () -> {
            command(r.getOwner(), r.hasOwner(), r.getCommandId());
            RequestChecks.localCause(r.getCause());
            Views.Gone g = holder.get().endSession(caller(), ProtoMapping.owner(r.getOwner()),
                    key(r.getCommandId(), "EndSession", r), ProtoMapping.cause(r.getCause()));
            return EndSessionResponse.newBuilder().setExisting(g.existing()).setAlreadyGone(g.alreadyGone()).build();
        });
    }

    // ---- helpers ---------------------------------------------------------------------------------

    private static void command(SessionOwner owner, boolean hasOwner, String commandId) throws TelephonyException {
        RequestChecks.tenant(tenant(), owner.getTenantId());
        RequestChecks.command(owner, hasOwner, commandId, true);
    }

    private static Requests.Caller caller() {
        Requests.Caller c = CallerInterceptor.CALLER.get();
        return c == null ? new Requests.Caller(null) : c;
    }

    private static String tenant() {
        return CallerInterceptor.TENANT.get();
    }

    /** The de-duplication key: the request without its acks, which a retry may advance; and the attempt. */
    private static Requests.CommandKey key(String commandId, String rpc, Message request) throws TelephonyException {
        Message.Builder b = request.toBuilder();
        FieldDescriptor f = request.getDescriptorForType().findFieldByName("owner");
        if (f != null && request.hasField(f)) {
            SessionOwner o = (SessionOwner) request.getField(f);
            b.setField(f, o.toBuilder().clearAckedDeliverySeq().clearAckedIncarnation().build());
        }
        return new Requests.CommandKey(commandId, rpc, sha256(deterministic(b.build())), attempt());
    }

    /** Commands without a gRPC deadline get this one (TC ADR 0002 §2.5). */
    static final long DEFAULT_DEADLINE_NANOS = TimeUnit.SECONDS.toNanos(10);

    /**
     * This attempt's expiry (TC ADR 0002 §2.5, R1-8): {@code parity-attempt-expires-ms} if IE sent it,
     * and gRPC's relative deadline (10 s without one). Both together estimate the clock offset
     * between IE's host and TC's: the header minus (TC's clock plus the time the deadline has left).
     */
    private static Requests.Attempt attempt() throws TelephonyException {
        long now = System.nanoTime();
        Deadline d = Context.current().getDeadline();
        long remaining = d == null ? DEFAULT_DEADLINE_NANOS : d.timeRemaining(TimeUnit.NANOSECONDS);
        String header = CallerInterceptor.ATTEMPT_EXPIRES.get();
        long expires = 0;
        if (header != null) {
            try {
                expires = Long.parseLong(header.trim());
            } catch (NumberFormatException e) {
                throw new TelephonyException(Code.INVALID_REQUEST);
            }
            if (expires <= 0) {
                throw new TelephonyException(Code.INVALID_REQUEST);
            }
            if (d != null) {
                CLOCK_OFFSET_MS.set(expires - (System.currentTimeMillis() + TimeUnit.NANOSECONDS.toMillis(remaining)));
            }
        }
        return new Requests.Attempt(expires, now + remaining);
    }

    /** The last estimate of IE's clock minus TC's, in ms, give or take the transit (a gauge, alerted above 100). */
    static final java.util.concurrent.atomic.AtomicLong CLOCK_OFFSET_MS = new java.util.concurrent.atomic.AtomicLong();

    /** The gauge's value, for the metrics. */
    public static long clockOffsetMs() {
        return CLOCK_OFFSET_MS.get();
    }

    /** A message's bytes in protobuf's deterministic serialization, so equal requests hash equal. */
    private static byte[] deterministic(Message m) {
        try {
            java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream(m.getSerializedSize());
            com.google.protobuf.CodedOutputStream out = com.google.protobuf.CodedOutputStream.newInstance(bytes);
            out.useDeterministicSerialization();
            m.writeTo(out);
            out.flush();
            return bytes.toByteArray();
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    private static String sha256(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /** What is left of the caller's deadline (the media RPCs' 300 ms when it set none), less a margin for the answer's way back. */
    private static long legacyPlayWaitMs() {
        io.grpc.Deadline d = io.grpc.Context.current().getDeadline();
        long left = d == null ? 300 : d.timeRemaining(java.util.concurrent.TimeUnit.MILLISECONDS);
        return Math.max(1, Math.min(1_000, left - 20));
    }

    private static <T> void unary(StreamObserver<T> out, Call<T> call) {
        T result;
        try {
            result = call.run();
        } catch (TelephonyException e) {
            out.onError(Errors.status(e.code()));
            return;
        } catch (RuntimeException e) {
            LOG.atError().addKeyValue("event", "rpc.failed").setCause(e).log("a telephony call failed");
            out.onError(Errors.status(Code.INTERNAL));
            return;
        }
        out.onNext(result);
        out.onCompleted();
    }
}
