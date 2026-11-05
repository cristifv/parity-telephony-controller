package parity.telephonycontroller.lifecycle;

import java.time.Duration;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import parity.telephonycontroller.config.ControllerProperties;

/**
 * Test only ({@code lifecycle.drain-hold}, default 0): holds the drain this long, standing in
 * for the sessions step 0 doesn't have yet, so the tests can prove what a drain that runs out of
 * time does (no drained file, exit 1) and what a second signal does (exit 128 + n). Replaced by
 * the session registry's drain in step 3.
 */
@Component
class DrainHold implements Drainable {

    private final Duration hold;

    DrainHold(ControllerProperties properties, Environment environment) {
        this.hold = properties.lifecycle().drainHoldOrZero();
        if (!hold.isZero() && !environment.matchesProfiles("test")) {
            throw new ConfigurationRefusedException(
                    "parity.telephony-controller.lifecycle.drain-hold is test only: set it only with the 'test' profile");
        }
    }

    @Override
    public boolean drain(Duration timeout) throws InterruptedException {
        if (hold.isZero() || hold.isNegative()) {
            return true;
        }
        Thread.sleep(Math.min(hold.toMillis(), timeout.toMillis()));
        return hold.compareTo(timeout) <= 0;
    }
}
