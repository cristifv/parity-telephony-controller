package parity.telephonycontroller.api;

import io.grpc.stub.ServerCallStreamObserver;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;
import parity.contracts.telephony.v1.WatchResponse;
import parity.telephonycontroller.session.Code;
import parity.telephonycontroller.session.Events;
import parity.telephonycontroller.session.StreamSink;

/**
 * A Watch stream. {@link #send} never blocks: messages queue here and go out as gRPC's flow
 * control allows ({@code isReady}). The queue is capped: a reader that falls that far behind gets
 * its stream ended with UNAVAILABLE (TC ADR 0001 §4.4), reconnects, and is replayed what it hasn't
 * acknowledged, within the engine's limits.
 */
final class GrpcSink implements StreamSink {

    /** Messages a stream may have waiting before its reader counts as stuck. */
    static final int DEFAULT_CAPACITY = 4096;

    private final ServerCallStreamObserver<WatchResponse> observer;
    private final int capacity;
    private final ConcurrentLinkedQueue<WatchResponse> queue = new ConcurrentLinkedQueue<>();
    private final java.util.concurrent.atomic.AtomicInteger queued = new java.util.concurrent.atomic.AtomicInteger();
    private final ReentrantLock lock = new ReentrantLock();
    private volatile boolean done;

    GrpcSink(ServerCallStreamObserver<WatchResponse> observer, int capacity) {
        this.observer = observer;
        this.capacity = capacity;
    }

    void start(Runnable onCancel) {
        observer.setOnReadyHandler(this::drain);
        observer.setOnCancelHandler(() -> {
            done = true;
            onCancel.run();
        });
    }

    @Override
    public boolean send(Events.Delivery d) {
        if (done) {
            return false;
        }
        if (queued.get() >= capacity) {
            overflow();
            return false;
        }
        queue.add(ProtoMapping.delivery(d));
        queued.incrementAndGet();
        drain();
        return true;
    }

    @Override
    public boolean drained() {
        return !done && queued.get() == 0 && observer.isReady();
    }

    /** The reader fell behind by the whole queue: end its stream, keep nothing. */
    private void overflow() {
        lock.lock();
        try {
            if (done) {
                return;
            }
            done = true;
            queue.clear();
            queued.set(0);
            observer.onError(io.grpc.Status.UNAVAILABLE.withDescription("the stream's reader fell behind")
                    .asRuntimeException());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close(Code code) {
        lock.lock();
        try {
            if (done) {
                return;
            }
            done = true;
            queue.clear();
            queued.set(0);
            observer.onError(Errors.status(code));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void drop() {
        lock.lock();
        try {
            if (done) {
                return;
            }
            done = true;
            queue.clear();
            queued.set(0);
            observer.onError(io.grpc.Status.UNAVAILABLE.asRuntimeException());
        } finally {
            lock.unlock();
        }
    }

    private void drain() {
        while (true) {
            if (!lock.tryLock()) {
                return;
            }
            try {
                WatchResponse m;
                while (!done && observer.isReady() && (m = queue.poll()) != null) {
                    queued.decrementAndGet();
                    observer.onNext(m);
                }
            } finally {
                lock.unlock();
            }
            if (done || queue.isEmpty() || !observer.isReady()) {
                return;
            }
        }
    }
}
