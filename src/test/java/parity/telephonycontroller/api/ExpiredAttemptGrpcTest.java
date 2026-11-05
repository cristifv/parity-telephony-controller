package parity.telephonycontroller.api;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import parity.contracts.telephony.v1.AddLegRequest;
import parity.contracts.telephony.v1.CodecPolicy;
import parity.contracts.telephony.v1.Endpoint;
import parity.contracts.telephony.v1.LegRole;
import parity.contracts.telephony.v1.SessionOwner;
import parity.contracts.telephony.v1.TelephonyServiceGrpc;
import parity.telephonycontroller.journal.InMemoryJournal;
import parity.telephonycontroller.model.Kinds;
import parity.telephonycontroller.node.NodePort;
import parity.telephonycontroller.node.SimulatedNode;
import parity.telephonycontroller.owner.InMemoryOwnerDirectory;
import parity.telephonycontroller.owner.OwnerDirectory;
import parity.telephonycontroller.owner.OwnerDirectoryUnavailable;
import parity.telephonycontroller.owner.OwnerRecord;
import parity.telephonycontroller.session.EngineHolder;
import parity.telephonycontroller.session.Requests;
import parity.telephonycontroller.session.TelephonyNode;
import parity.telephonycontroller.time.ManualClock;

/**
 * TC ADR 0002 §2.5 with real grpc-java: an AddLeg queued behind a slow turn (an adoption whose etcd
 * read takes 600 ms) until its gRPC deadline (200 ms) has passed does nothing when its turn comes:
 * no leg is originated, so "leg absent" stays true.
 */
class ExpiredAttemptGrpcTest {

    private static final String I = "0192f6a1-7c3e-7b21-9a4d-5e6f70819203";
    private static final String C = "0192f6a1-8000-7000-8000-000000000001";
    private static final String A = "0192f6a1-8000-7000-8000-0000000000a1";

    private final ManualClock clock = new ManualClock(1_792_800_000_000L);
    private final SimulatedNode node = new SimulatedNode(clock, 1_000_000);
    private GrpcServer server;
    private ManagedChannel channel;
    private EngineHolder holder;

    /** etcd, made slow on demand: the read blocks until released. */
    static final class SlowOwners implements OwnerDirectory {
        final InMemoryOwnerDirectory inner = new InMemoryOwnerDirectory();
        volatile CountDownLatch gate;
        final CountDownLatch entered = new CountDownLatch(1);

        @Override
        public Optional<OwnerRecord> owner(String interactionId) throws OwnerDirectoryUnavailable {
            CountDownLatch g = gate;
            if (g != null) {
                entered.countDown();
                try {
                    g.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return inner.owner(interactionId);
        }

        @Override
        public boolean replicaRegistered(String bin, String replicaId) throws OwnerDirectoryUnavailable {
            return inner.replicaRegistered(bin, replicaId);
        }
    }

    @AfterEach
    void stop() {
        if (channel != null) {
            channel.shutdownNow();
        }
        if (server != null) {
            server.close();
        }
        if (holder != null) {
            holder.get().close();
        }
    }

    @Test
    void anAddLegWhoseDeadlinePassesInTheQueueOriginatesNothing() throws Exception {
        SlowOwners owners = new SlowOwners();
        owners.inner.register("bin-a", "r1");
        owners.inner.register("bin-a", "r2");
        holder = new EngineHolder(new TelephonyNode.Config("media-1", "test", Set.of("bin-a"), "inc-1", List.of()),
                clock, owners, new InMemoryJournal(), node);
        holder.start();
        server = new GrpcServer(0, holder, true).start();
        channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.port()).usePlaintext().build();
        Requests.Caller caller = new Requests.Caller("bin-a");

        node.park(new NodePort.Channel(C, "acme", I, "bin-a", Kinds.Role.CUSTOMER, Kinds.Endpoint.SIP, "x@carrier", true,
                clock.nowMs(), "+447700900100", "+447700900123", "carrier-a", List.of(), null, Kinds.LegState.PARKED));
        owners.inner.claim(I, 1042, new OwnerRecord(1042, "acme", "bin-a", "r1", "media-1"));
        holder.get().answer(caller, new Requests.Owner("acme", I, 1042, "r1", 0, ""),
                new Requests.CommandKey("answer", "AnswerLeg", "fp"), C);

        // Warm the gRPC path first, so the AddLeg below reaches the engine well within its deadline.
        Metadata warm = new Metadata();
        warm.put(Metadata.Key.of("x-parity-test-peer", Metadata.ASCII_STRING_MARSHALLER), "parity-interaction-engine.bin-a");
        warm.put(Metadata.Key.of("tenant-id", Metadata.ASCII_STRING_MARSHALLER), "acme");
        TelephonyServiceGrpc.newBlockingStub(channel).withInterceptors(MetadataUtils.newAttachHeadersInterceptor(warm))
                .withDeadlineAfter(5, TimeUnit.SECONDS).getSession(parity.contracts.telephony.v1.GetSessionRequest.newBuilder()
                        .setTenantId("acme").setInteractionId(I).setReplicaId("r1").build());

        // r2 adopts: its etcd read hangs, holding the session's turn.
        owners.inner.delete(I);
        owners.inner.claim(I, 1052, new OwnerRecord(1052, "acme", "bin-a", "r2", "media-1"));
        owners.gate = new CountDownLatch(1);
        Thread adoption = Thread.ofVirtual().start(() -> {
            try {
                holder.get().claim(caller, new Requests.Owner("acme", I, 1052, "r2", 0, ""));
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
        assertThat(owners.entered.await(2, TimeUnit.SECONDS)).isTrue();
        long expiredBefore = TelephonyNode.expiredAttempts();

        // r2's AddLeg, 200 ms deadline, queues behind the adoption.
        Metadata md = new Metadata();
        md.put(Metadata.Key.of("x-parity-test-peer", Metadata.ASCII_STRING_MARSHALLER), "parity-interaction-engine.bin-a");
        md.put(Metadata.Key.of("tenant-id", Metadata.ASCII_STRING_MARSHALLER), "acme");
        TelephonyServiceGrpc.TelephonyServiceBlockingStub stub = TelephonyServiceGrpc.newBlockingStub(channel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(md))
                .withDeadlineAfter(200, TimeUnit.MILLISECONDS);
        AddLegRequest req = AddLegRequest.newBuilder()
                .setOwner(SessionOwner.newBuilder().setTenantId("acme").setInteractionId(I).setOwnerEpoch(1052)
                        .setReplicaId("r2"))
                .setCommandId("ie-bin-a-2:0001").setLegId(A).setRole(LegRole.LEG_ROLE_AGENT)
                .setAgentContact("sip:agent-7f3a@acme.agents.example").setRingTimeoutS(30)
                .setEndpoint(Endpoint.ENDPOINT_WEBRTC).setCodecPolicy(CodecPolicy.CODEC_POLICY_G711_ONLY).build();
        Status.Code got;
        try {
            stub.addLeg(req);
            got = Status.Code.OK;
        } catch (StatusRuntimeException e) {
            got = e.getStatus().getCode();
        }
        assertThat(got).isEqualTo(Status.Code.DEADLINE_EXCEEDED);

        Thread.sleep(400);
        owners.gate.countDown();
        adoption.join(2_000);
        long until = System.currentTimeMillis() + 2_000;
        while (TelephonyNode.expiredAttempts() == expiredBefore && System.currentTimeMillis() < until) {
            Thread.sleep(10);
        }
        assertThat(TelephonyNode.expiredAttempts()).as("the queued attempt was refused when its turn came")
                .isGreaterThan(expiredBefore);
        assertThat(node.channel(A)).as("no leg was originated").isEmpty();
        assertThat(node.invariantViolations()).isEmpty();
    }
}
