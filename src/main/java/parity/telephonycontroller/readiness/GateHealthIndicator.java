package parity.telephonycontroller.readiness;

import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;
import parity.telephonycontroller.lifecycle.LifecycleFiles;

/**
 * The {@code gate} health contributor, part of the readiness group: {@code /readyz} answers 200
 * only while every readiness check passes and no drain has started. Spring Boot's own
 * {@code readinessState} turns ready as soon as the application starts, so it can't say this
 * alone.
 */
@Component("gateHealthIndicator")
public class GateHealthIndicator implements HealthIndicator {

    private final LifecycleFiles lifecycle;

    public GateHealthIndicator(LifecycleFiles lifecycle) {
        this.lifecycle = lifecycle;
    }

    @Override
    public Health health() {
        if (lifecycle.ready()) {
            return Health.up().build();
        }
        return Health.outOfService().withDetails(lifecycle.failures()).build();
    }
}
