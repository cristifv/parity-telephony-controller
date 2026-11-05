package parity.telephonycontroller.api;

import java.util.Locale;
import parity.contracts.telephony.v1.Arrived;
import parity.contracts.telephony.v1.EndCause;
import parity.contracts.telephony.v1.EndedBy;
import parity.contracts.telephony.v1.Endpoint;
import parity.contracts.telephony.v1.Heartbeat;
import parity.contracts.telephony.v1.LegRole;
import parity.contracts.telephony.v1.LegSnapshot;
import parity.contracts.telephony.v1.LegState;
import parity.contracts.telephony.v1.LegUpdate;
import parity.contracts.telephony.v1.MediaKind;
import parity.contracts.telephony.v1.NodeStatus;
import parity.contracts.telephony.v1.RemoteMediaChanged;
import parity.contracts.telephony.v1.RemoteMediaKind;
import parity.contracts.telephony.v1.Session;
import parity.contracts.telephony.v1.SessionEndReason;
import parity.contracts.telephony.v1.SessionEnded;
import parity.contracts.telephony.v1.SessionOwner;
import parity.contracts.telephony.v1.SessionRef;
import parity.contracts.telephony.v1.SessionState;
import parity.contracts.telephony.v1.SipHeader;
import parity.contracts.telephony.v1.WatchResponse;
import parity.telephonycontroller.model.Kinds;
import parity.telephonycontroller.session.Events;
import parity.telephonycontroller.session.Requests;
import parity.telephonycontroller.session.Views;

/** Between {@code telephony.v1}'s generated types and the controller's own (§4.7: only here). */
final class ProtoMapping {

    private ProtoMapping() {}

    static Requests.Owner owner(SessionOwner o) {
        return new Requests.Owner(o.getTenantId(), o.getInteractionId(), o.getOwnerEpoch(), o.getReplicaId(),
                o.getAckedDeliverySeq(), o.getAckedIncarnation());
    }

    static WatchResponse delivery(Events.Delivery d) {
        WatchResponse.Builder b = WatchResponse.newBuilder().setIncarnation(d.incarnation()).setNodeId(d.nodeId())
                .setDeliverySeq(d.seq()).setAtMs(d.atMs());
        if (d.session() != null) {
            b.setSession(SessionRef.newBuilder().setTenantId(d.session().tenantId())
                    .setInteractionId(d.session().interactionId()).setOwnerEpoch(d.session().ownerEpoch()));
        }
        switch (d.event()) {
            case Events.Arrived a -> b.setArrived(Arrived.newBuilder().setBin(a.bin()).setLegId(a.legId())
                    .setSipCallId(nz(a.sipCallId())).setCalledNumber(nz(a.calledNumber()))
                    .setCallerNumber(nz(a.callerNumber()))
                    .addAllHeaders(a.headers().stream()
                            .map(h -> SipHeader.newBuilder().setName(h.name()).setValue(h.value()).build()).toList())
                    .setAttempt(a.attempt()).setRedirectedFromBin(nz(a.redirectedFromBin()))
                    .setDeadlineMs(a.deadlineMs()).setTrunkId(nz(a.trunkId())));
            case Events.LegUpdate u -> {
                LegUpdate.Builder l = LegUpdate.newBuilder().setLegId(u.legId()).setRole(role(u.role()))
                        .setState(state(u.state())).setSipStatus(u.sipStatus()).setAnsweredAtMs(u.answeredAtMs());
                if (u.cause() != null) {
                    l.setCause(cause(u.cause()));
                }
                if (u.endedBy() != null) {
                    l.setEndedBy(endedBy(u.endedBy()));
                }
                b.setLeg(l);
            }
            case Events.RemoteMediaChanged r -> b.setRemoteMediaChanged(RemoteMediaChanged.newBuilder()
                    .setLegId(r.legId()).setKind(RemoteMediaKind.valueOf("REMOTE_MEDIA_KIND_" + r.kind().name())));
            case Events.SessionEnded e -> b.setSessionEnded(SessionEnded.newBuilder()
                    .setReason(endReason(e.reason())).setSipStatus(e.sipStatus()));
            case Events.NodeStatus n -> b.setNodeStatus(NodeStatus.newBuilder().setSessions(n.sessions())
                    .setFreeLegs(Math.max(0, n.freeLegs())).setDraining(n.draining()).setRebuilding(n.rebuilding())
                    .setVersion(n.version()).addAllCapabilities(n.capabilities()));
            case Events.Heartbeat h -> b.setHeartbeat(Heartbeat.getDefaultInstance());
            case Events.PlayFinished p -> b.setPlayFinished(parity.contracts.telephony.v1.PlayFinished.newBuilder()
                    .setLegId(p.legId()).setPlayId(p.playId())
                    .setReason(parity.contracts.telephony.v1.PlayEndReason.valueOf("PLAY_END_REASON_" + p.reason()))
                    .setPlayedMs((int) Math.min(Integer.MAX_VALUE, p.playedMs())));
            case Events.PlayStarted p -> b.setPlayStarted(parity.contracts.telephony.v1.PlayStarted.newBuilder()
                    .setLegId(p.legId()).setPlayId(p.playId()));
            case Events.PlayFailed p -> b.setPlayFailed(parity.contracts.telephony.v1.PlayFailed.newBuilder()
                    .setLegId(p.legId()).setPlayId(p.playId())
                    .setReason(parity.contracts.telephony.v1.PlayFailReason.valueOf("PLAY_FAIL_REASON_" + p.reason())));
            case Events.RemoteHold r -> b.setRemoteHold(parity.contracts.telephony.v1.RemoteHold.newBuilder()
                    .setLegId(r.legId()).setOn(r.on()));
            case Events.MediaActivity m -> b.setMediaActivity(parity.contracts.telephony.v1.MediaActivity.newBuilder()
                    .setLegId(m.legId()).setActive(m.active()));
            case Events.DtmfAction a -> b.setDtmfAction(parity.contracts.telephony.v1.DtmfAction.newBuilder()
                    .setLegId(a.legId()).setRole(role(a.role()))
                    .setAction(parity.contracts.telephony.v1.DtmfActionKind.valueOf("DTMF_ACTION_KIND_" + a.action()))
                    .setResult(parity.contracts.telephony.v1.DtmfActionResult.valueOf("DTMF_ACTION_RESULT_" + a.result()))
                    .setState(mediaState(a.muted(), a.recording())));
            case Events.RecordingStarted r -> b.setRecordingStarted(parity.contracts.telephony.v1.RecordingStarted.newBuilder()
                    .setRecordingId(r.recordingId()).setLegId(r.legId()));
            case Events.RecordingStopped r -> {
                parity.contracts.telephony.v1.RecordingStopped.Builder rs = parity.contracts.telephony.v1.RecordingStopped
                        .newBuilder().setRecordingId(r.recordingId()).setLegId(r.legId()).setPath(r.path())
                        .setSizeBytes(r.sizeBytes()).setDurationMs(Math.toIntExact(Math.min(Integer.MAX_VALUE, r.durationMs())))
                        .setSha256(r.sha256())
                        .setWrappedDek(com.google.protobuf.ByteString.copyFrom(java.util.Base64.getDecoder().decode(r.wrappedDekBase64())))
                        .setKekId(r.kekId());
                for (parity.telephonycontroller.node.NodePort.RecordingPause p : r.pauses()) {
                    parity.contracts.telephony.v1.RecordingPause.Builder pb = parity.contracts.telephony.v1.RecordingPause
                            .newBuilder().setStartMs(p.startMs()).setEndMs(p.endMs()).setByDtmf(p.byDtmf());
                    if (p.role() != null) {
                        pb.setRole(role(p.role()));
                    }
                    rs.addPauses(pb);
                }
                b.setRecordingStopped(rs);
            }
            case Events.DtmfReceived r -> b.setDtmf(parity.contracts.telephony.v1.DtmfReceived.newBuilder()
                    .setLegId(r.legId()).setDigit(r.digit()).setDurationMs(r.durationMs())
                    .setSource(parity.contracts.telephony.v1.DigitSource.valueOf("DIGIT_SOURCE_" + r.source()))
                    .setCollectionId(r.collectionId()));
        }
        return b.build();
    }

    /** A leg's media state after a DTMF action: its mute; the session's recording if known. */
    private static parity.contracts.telephony.v1.LegMediaState.Builder mediaState(boolean muted, String recording) {
        parity.contracts.telephony.v1.LegMediaState.Builder m = parity.contracts.telephony.v1.LegMediaState.newBuilder()
                .setMuted(muted);
        if (recording != null && recording.matches("ACTIVE|PAUSED|STOPPED")) {
            m.setRecording(parity.contracts.telephony.v1.RecordingState.valueOf("RECORDING_STATE_" + recording));
        }
        return m;
    }

    static Session session(Views.SessionView v) {
        Session.Builder b = Session.newBuilder().setTenantId(v.tenantId()).setInteractionId(v.interactionId())
                .setOwnerEpoch(v.ownerEpoch()).setOwnerReplicaId(nz(v.ownerReplicaId()))
                .setState(SessionState.valueOf("SESSION_STATE_" + v.state().name()))
                .setMedia(MediaKind.valueOf("MEDIA_KIND_" + v.media().name())).setOnHold(v.onHold())
                .setLastDeliverySeq(v.lastDeliverySeq()).setEndedAtMs(v.endedAtMs())
                .setArrivalDeadlineMs(v.arrivalDeadlineMs());
        if (v.endReason() != null) {
            b.setEndReason(endReason(v.endReason()));
        }
        for (Views.LegView l : v.legs()) {
            LegSnapshot.Builder s = LegSnapshot.newBuilder().setLegId(l.legId()).setRole(role(l.role()))
                    .setState(state(l.state())).setSipCallId(nz(l.sipCallId())).setSipStatus(l.sipStatus())
                    .setAnsweredAtMs(l.answeredAtMs()).setEndedAtMs(l.endedAtMs());
            if (l.endpoint() != null) {
                s.setEndpoint(Endpoint.valueOf("ENDPOINT_" + l.endpoint().name()));
            }
            if (l.cause() != null) {
                s.setCause(cause(l.cause()));
            }
            if (l.endedBy() != null) {
                s.setEndedBy(endedBy(l.endedBy()));
            }
            b.addLegs(s);
        }
        return b.build();
    }

    static LegRole role(Kinds.Role r) {
        return LegRole.valueOf("LEG_ROLE_" + r.name());
    }

    static Kinds.Role role(LegRole r) {
        return Kinds.Role.valueOf(r.name().substring("LEG_ROLE_".length()));
    }

    static LegState state(Kinds.LegState s) {
        return LegState.valueOf("LEG_STATE_" + s.name());
    }

    static EndCause cause(Kinds.EndCause c) {
        return EndCause.valueOf("END_CAUSE_" + c.name());
    }

    static Kinds.EndCause cause(EndCause c) {
        return Kinds.EndCause.valueOf(c.name().substring("END_CAUSE_".length()));
    }

    static Kinds.Endpoint endpoint(Endpoint e) {
        return Kinds.Endpoint.valueOf(e.name().substring("ENDPOINT_".length()));
    }

    static EndedBy endedBy(Kinds.EndedBy e) {
        return EndedBy.valueOf("ENDED_BY_" + e.name());
    }

    static SessionEndReason endReason(Kinds.SessionEndReason r) {
        return SessionEndReason.valueOf("SESSION_END_REASON_" + r.name());
    }

    static String lower(Enum<?> e) {
        return e.name().toLowerCase(Locale.ROOT);
    }

    private static String nz(String s) {
        return s == null ? "" : s;
    }
}
