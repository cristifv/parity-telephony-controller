package parity.telephonycontroller;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ConfigurableApplicationContext;
import parity.telephonycontroller.lifecycle.ExitCodes;
import parity.telephonycontroller.lifecycle.ShutdownSignals;

/**
 * The telephony controller: one per media node, next to freeswitch and parity-ms (PLAN.md).
 * Step 0 is the skeleton: the node lock, readiness and the healthy file, the drain, masked JSON
 * logs and metrics. ESL, parity-ms and gRPC come with steps 3 to 5.
 *
 * <p>Exit codes: 0 drained (also after a signal during the start); 1 a drain that ran out of
 * time, or a failure; 2 a configuration error (an invalid or unknown property, a YAML error, a
 * refused setting); 3 another controller holds the node lock; 128 + n a second signal n.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class ParityTelephonyControllerApplication {

    public static void main(String[] args) {
        // Before the start: a signal during it must not leave a healthy file or the JVM's 143.
        ShutdownSignals.install();
        ConfigurableApplicationContext context;
        try {
            context = SpringApplication.run(ParityTelephonyControllerApplication.class, args);
        } catch (Throwable failure) {
            int code = ExitCodes.forStartupFailure(failure);
            ShutdownSignals.stopped(code);
            System.exit(code);
            return;
        }
        ShutdownSignals.started(context);
    }
}
