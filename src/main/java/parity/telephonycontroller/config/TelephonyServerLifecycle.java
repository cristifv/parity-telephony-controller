package parity.telephonycontroller.config;

import org.springframework.context.SmartLifecycle;
import parity.telephonycontroller.api.GrpcServer;
import parity.telephonycontroller.session.EngineHolder;
import parity.telephonycontroller.session.TelephonyNode;

/**
 * Starts the first run (the rebuild) and then the {@code telephony.v1} port, before the healthy
 * file (whose lifecycle runs last); stops them after the drain.
 */
public final class TelephonyServerLifecycle implements SmartLifecycle {

    private final int port;
    private final EngineHolder holder;
    private final boolean testPeerHeader;
    private volatile GrpcServer server;

    TelephonyServerLifecycle(int port, EngineHolder holder, boolean testPeerHeader) {
        this.port = port;
        this.holder = holder;
        this.testPeerHeader = testPeerHeader;
    }

    @Override
    public void start() {
        holder.start();
        server = new GrpcServer(port, holder, testPeerHeader).start();
    }

    @Override
    public void stop() {
        GrpcServer s = server;
        if (s != null) {
            s.close();
        }
        TelephonyNode t = holder.get();
        if (t != null) {
            t.close();
        }
        server = null;
    }

    @Override
    public boolean isRunning() {
        return server != null;
    }

    @Override
    public int getPhase() {
        return 0;
    }

    /** The bound port. */
    public int port() {
        return server.port();
    }
}
