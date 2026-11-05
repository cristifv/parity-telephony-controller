package parity.telephonycontroller.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import parity.contracts.telephony.v1.GetSessionRequest;
import parity.contracts.telephony.v1.TelephonyServiceGrpc;
import parity.contracts.telephony.v1.WatchRequest;
import parity.contracts.telephony.v1.WatchResponse;
import parity.telephonycontroller.journal.InMemoryJournal;
import parity.telephonycontroller.node.SimulatedNode;
import parity.telephonycontroller.owner.InMemoryOwnerDirectory;
import parity.telephonycontroller.session.EngineHolder;
import parity.telephonycontroller.session.TelephonyNode;
import parity.telephonycontroller.time.ManualClock;

/**
 * The test-only identity header (TC ADR 0001 §6) is honoured only when turned on: with it off, a
 * caller presenting it has no identity, and is refused.
 */
class TestPeerHeaderTest {

    private EngineHolder holder;
    private GrpcServer server;
    private ManagedChannel channel;

    private TelephonyServiceGrpc.TelephonyServiceBlockingStub stub(boolean headerOn) {
        ManualClock clock = new ManualClock(1_792_800_000_000L);
        InMemoryOwnerDirectory owners = new InMemoryOwnerDirectory();
        owners.register("bin-a", "r1");
        holder = new EngineHolder(new TelephonyNode.Config("media-1", "test", Set.of("bin-a"), "inc-1", List.of()),
                clock, owners, new InMemoryJournal(), new SimulatedNode(clock, 10));
        holder.start();
        server = new GrpcServer(0, holder, headerOn).start();
        channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.port()).usePlaintext().build();
        Metadata m = new Metadata();
        m.put(Metadata.Key.of("x-parity-test-peer", Metadata.ASCII_STRING_MARSHALLER), "parity-interaction-engine.bin-a");
        m.put(Metadata.Key.of("tenant-id", Metadata.ASCII_STRING_MARSHALLER), "acme");
        return TelephonyServiceGrpc.newBlockingStub(channel).withInterceptors(MetadataUtils.newAttachHeadersInterceptor(m));
    }

    @AfterEach
    void stop() {
        channel.shutdownNow();
        server.close();
        holder.get().close();
    }

    private static String trailer(StatusRuntimeException e) {
        return e.getTrailers().get(Metadata.Key.of("parity-error", Metadata.ASCII_STRING_MARSHALLER));
    }

    @Test
    void withTheHeaderOffACallPresentingItIsRefused() {
        TelephonyServiceGrpc.TelephonyServiceBlockingStub stub = stub(false);

        StatusRuntimeException watch = catchThrowableOfType(StatusRuntimeException.class, () ->
                stub.watch(WatchRequest.newBuilder().setReplicaId("r1").setBin("bin-a").build()).next());
        StatusRuntimeException get = catchThrowableOfType(StatusRuntimeException.class, () ->
                stub.getSession(GetSessionRequest.newBuilder().setTenantId("acme")
                        .setInteractionId("0192f6a1-7c3e-7b21-9a4d-5e6f70819203").setReplicaId("r1").build()));

        assertThat(watch.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
        assertThat(trailer(watch)).isEqualTo("identity_mismatch");
        assertThat(get.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
        assertThat(trailer(get)).isEqualTo("identity_mismatch");
    }

    @Test
    void withTheHeaderOnTheSameCallIsServed() {
        Iterator<WatchResponse> stream =
                stub(true).watch(WatchRequest.newBuilder().setReplicaId("r1").setBin("bin-a").build());
        assertThat(stream.next().hasNodeStatus()).isTrue();
    }
}
