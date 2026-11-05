package parity.telephonycontroller.readiness;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;
import parity.telephonycontroller.config.ControllerProperties;
import parity.telephonycontroller.lifecycle.NodeLock;

/**
 * The controller's readiness checks. Step 0 has two kinds:
 * <ul>
 *   <li>the node lock is held;</li>
 *   <li>each configured endpoint (freeswitch's ESL, parity-ms's control port) accepts a TCP
 *       connection. Steps 3 and 4 replace these with the ESL event subscription and parity-ms's
 *       {@code hello} (PLAN.md "Health and drain").</li>
 * </ul>
 */
@Component
public class ReadinessChecks {

    private final List<ReadinessCheck> checks = new ArrayList<>();

    public ReadinessChecks(NodeLock lock, ControllerProperties properties,
            parity.telephonycontroller.node.NodePort node) {
        checks.add(new LockCheck(lock));
        if (node instanceof parity.telephonycontroller.node.RealNode real) {
            // TC ADR 0002 §1: healthy only while ESL is connected, subscribed and snapshotted.
            checks.add(new EslCheck(real));
        }
        int timeoutMs = (int) properties.readiness().connectTimeout().toMillis();
        properties.readiness().endpointsOrEmpty().forEach((name, address) -> {
            if (address != null && !address.isBlank()) {
                checks.add(new TcpEndpointCheck(name, address, timeoutMs));
            }
        });
    }

    /** Every failing check's reason, by name; empty when all pass. */
    public Map<String, String> evaluate() {
        Map<String, String> failures = new LinkedHashMap<>();
        for (ReadinessCheck check : checks) {
            String failure = check.failure();
            if (failure != null) {
                failures.put(check.name(), failure);
            }
        }
        return failures;
    }

    public List<String> names() {
        return checks.stream().map(ReadinessCheck::name).toList();
    }

    /** freeswitch's event socket is live. */
    record EslCheck(parity.telephonycontroller.node.RealNode node) implements ReadinessCheck {

        @Override
        public String name() {
            return "freeswitch-esl";
        }

        @Override
        public String failure() {
            return node.live() ? null : "freeswitch's ESL is not live";
        }
    }

    /** The node lock is still held. */
    record LockCheck(NodeLock lock) implements ReadinessCheck {

        @Override
        public String name() {
            return "node-lock";
        }

        @Override
        public String failure() {
            return lock.held() ? null : "the node lock is not held";
        }
    }

    /** A TCP connect within the timeout. */
    record TcpEndpointCheck(String name, String address, int timeoutMs) implements ReadinessCheck {

        @Override
        public String failure() {
            int colon = address.lastIndexOf(':');
            String host = address.substring(0, colon);
            int port = Integer.parseInt(address.substring(colon + 1));
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress(host, port), timeoutMs);
                return null;
            } catch (IOException e) {
                return "no TCP connection to " + address + ": " + e.getClass().getSimpleName();
            }
        }
    }
}
