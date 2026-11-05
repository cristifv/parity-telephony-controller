package parity.telephonycontroller.api;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import parity.contracts.telephony.v1.WatchResponse;
import parity.telephonycontroller.session.Events;

/** A Watch stream whose reader stopped: its queue is capped and the stream ends with UNAVAILABLE. */
class GrpcSinkTest {

    /** A client that never reads: gRPC reports the stream not ready. */
    private static final class NotReading extends ServerCallStreamObserver<WatchResponse> {
        final List<WatchResponse> sent = new ArrayList<>();
        final List<Throwable> errors = new ArrayList<>();

        @Override public boolean isCancelled() { return false; }
        @Override public void setOnCancelHandler(Runnable r) { }
        @Override public void setCompression(String c) { }
        @Override public boolean isReady() { return false; }
        @Override public void setOnReadyHandler(Runnable r) { }
        @Override public void disableAutoInboundFlowControl() { }
        @Override public void request(int n) { }
        @Override public void setMessageCompression(boolean b) { }
        @Override public void onNext(WatchResponse v) { sent.add(v); }
        @Override public void onError(Throwable t) { errors.add(t); }
        @Override public void onCompleted() { }
    }

    @Test
    void aFullQueueEndsTheStreamWithUnavailable() {
        NotReading client = new NotReading();
        GrpcSink sink = new GrpcSink(client, 3);
        Events.Delivery hb = new Events.Delivery("inc", "media-1", 0, null, 1, new Events.Heartbeat());

        assertThat(sink.send(hb)).isTrue();
        assertThat(sink.send(hb)).isTrue();
        assertThat(sink.send(hb)).isTrue();
        assertThat(sink.drained()).isFalse();
        assertThat(sink.send(hb)).isFalse();
        assertThat(sink.send(hb)).isFalse();

        assertThat(client.sent).isEmpty();
        assertThat(client.errors).singleElement()
                .satisfies(t -> assertThat(Status.fromThrowable(t).getCode()).isEqualTo(Status.Code.UNAVAILABLE));
    }
}
