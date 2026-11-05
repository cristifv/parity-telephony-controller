package parity.telephonycontroller.api;

import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import parity.telephonycontroller.session.EngineHolder;

/**
 * The {@code telephony.v1} port (9700). Plain grpc-java, not spring-grpc (TC ADR 0001 §7): handlers
 * run on virtual threads, messages are capped at 1 MB, and HTTP/2 keepalive is on. mTLS comes with
 * step 8; until then the identity comes from the test header where a profile allows it.
 */
public final class GrpcServer implements AutoCloseable {

    private final Server server;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    public GrpcServer(int port, EngineHolder holder, boolean testPeerHeader) {
        this(port, holder, testPeerHeader, GrpcSink.DEFAULT_CAPACITY);
    }

    /** {@code streamCapacity}: messages a Watch stream may have waiting (tests make it small). */
    public GrpcServer(int port, EngineHolder holder, boolean testPeerHeader, int streamCapacity) {
        this.server = NettyServerBuilder.forPort(port)
                .executor(executor)
                .maxInboundMessageSize(1 << 20)
                .keepAliveTime(20, TimeUnit.SECONDS)
                .keepAliveTimeout(5, TimeUnit.SECONDS)
                .permitKeepAliveTime(5, TimeUnit.SECONDS)
                .addService(ServerInterceptors.intercept(new TelephonyGrpcService(holder, streamCapacity),
                        new CallerInterceptor(testPeerHeader)))
                .build();
    }

    public GrpcServer start() {
        try {
            server.start();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return this;
    }

    /** The bound port (useful with port 0). */
    public int port() {
        return server.getPort();
    }

    @Override
    public void close() {
        server.shutdown();
        try {
            if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
                server.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            server.shutdownNow();
        }
        executor.shutdownNow();
    }
}
