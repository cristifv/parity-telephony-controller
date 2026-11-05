package parity.telephonycontroller.api;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import parity.contracts.telephony.v1.TelephonyServiceGrpc;
import parity.contracts.telephony.v1.WatchRequest;
import parity.contracts.telephony.v1.WatchResponse;
import parity.telephonycontroller.journal.InMemoryJournal;
import parity.telephonycontroller.model.Kinds.Endpoint;
import parity.telephonycontroller.model.Kinds.LegState;
import parity.telephonycontroller.model.Kinds.Role;
import parity.telephonycontroller.node.SimulatedNode;
import parity.telephonycontroller.owner.InMemoryOwnerDirectory;
import parity.telephonycontroller.session.EngineHolder;
import parity.telephonycontroller.session.TelephonyNode;
import parity.telephonycontroller.time.ManualClock;

/**
 * TC ADR 0001 §4.4, end to end: IE stops reading its Watch stream while the node keeps producing.
 * The stream ends with UNAVAILABLE once its queue is full, the node's buffers stay within budget
 * (the oldest dropped, counted), and IE's reconnect is served.
 */
class SlowReaderTest {

    /** Small, so the stuck stream's backlog (a hundred or more events) always exceeds it. */
    private static final long BUDGET = 16 * 1024;
    private final ManualClock clock = new ManualClock(1_792_800_000_000L);
    private final SimulatedNode node = new SimulatedNode(clock, 1_000_000);
    private EngineHolder holder;
    private GrpcServer server;
    private ManagedChannel channel;

    @AfterEach
    void stop() {
        channel.shutdownNow();
        server.close();
        holder.get().close();
    }

    @Test
    void aReaderThatStopsLosesItsStreamAndMemoryStaysBounded() throws Exception {
        InMemoryOwnerDirectory owners = new InMemoryOwnerDirectory();
        owners.register("bin-a", "r1");
        holder = new EngineHolder(new TelephonyNode.Config("media-1", "test", Set.of("bin-a"), "inc-1", List.of(),
                new TelephonyNode.Limits(256, 600_000, BUDGET)), clock, owners, new InMemoryJournal(), node);
        holder.start();
        server = new GrpcServer(0, holder, true, 200).start();
        channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.port()).usePlaintext().build();
        long droppedBefore = TelephonyNode.droppedEvents();

        AtomicInteger received = new AtomicInteger();
        CompletableFuture<Status> closed = new CompletableFuture<>();
        ClientCall<WatchRequest, WatchResponse> call = watch(received, closed);
        call.request(1);

        for (int i = 0; i < 20_000; i++) {
            String leg = String.format("0192f6a1-8000-7000-8000-%012d", i);
            node.park(new parity.telephonycontroller.node.NodePort.Channel(leg, "acme", String.format("0192f6a1-7c3e-7b21-9a4d-%012d", i), "bin-a",
                    Role.CUSTOMER, Endpoint.SIP, leg, true, clock.nowMs(), "+447700900100", null, "carrier-a",
                    List.of(), null, LegState.PARKED));
        }
        long until = System.currentTimeMillis() + 10_000;
        while (holder.get().hasStream("r1") && System.currentTimeMillis() < until) {
            Thread.sleep(20);
        }
        assertThat(holder.get().hasStream("r1")).as("the stuck stream is ended").isFalse();
        clock.advance(2_000);
        assertThat(holder.get().bufferedBytes()).isLessThanOrEqualTo(BUDGET);
        assertThat(TelephonyNode.droppedEvents() - droppedBefore).isPositive();

        call.request(Integer.MAX_VALUE);
        assertThat(closed.get(10, TimeUnit.SECONDS).getCode()).isEqualTo(Status.Code.UNAVAILABLE);

        // IE reconnects and is served again: node_status first, then (not checked here) its
        // unclaimed arrivals offered anew. DeliveryLimitsTest pins the replay's gap.
        AtomicInteger first = new AtomicInteger();
        ClientCall<WatchRequest, WatchResponse> again = watch(first, new CompletableFuture<>());
        again.request(1);
        long by = System.currentTimeMillis() + 5_000;
        while (first.get() == 0 && System.currentTimeMillis() < by) {
            Thread.sleep(10);
        }
        assertThat(first.get()).as("the reconnect's node_status").isEqualTo(1);
        again.cancel("done", null);
    }

    private ClientCall<WatchRequest, WatchResponse> watch(AtomicInteger received, CompletableFuture<Status> closed) {
        ClientCall<WatchRequest, WatchResponse> call =
                channel.newCall(TelephonyServiceGrpc.getWatchMethod(), CallOptions.DEFAULT);
        Metadata headers = new Metadata();
        headers.put(Metadata.Key.of("x-parity-test-peer", Metadata.ASCII_STRING_MARSHALLER), "parity-interaction-engine.bin-a");
        call.start(new ClientCall.Listener<>() {
            @Override
            public void onMessage(WatchResponse m) {
                received.incrementAndGet();
            }

            @Override
            public void onClose(Status status, Metadata trailers) {
                closed.complete(status);
            }
        }, headers);
        call.sendMessage(WatchRequest.newBuilder().setReplicaId("r1").setBin("bin-a").build());
        call.halfClose();
        return call;
    }
}
