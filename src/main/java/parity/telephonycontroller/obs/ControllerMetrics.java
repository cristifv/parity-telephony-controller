package parity.telephonycontroller.obs;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.springframework.stereotype.Component;
import parity.telephonycontroller.lifecycle.LifecycleFiles;

/**
 * Step 0's own series (PLAN.md "Observability"; the rest come with steps 3 to 6). Every series
 * carries the common labels {@code service}, {@code zone}, {@code node}, {@code instance},
 * {@code version} (application.yml).
 */
@Component
public class ControllerMetrics implements MeterBinder {

    private final LifecycleFiles lifecycle;

    public ControllerMetrics(LifecycleFiles lifecycle) {
        this.lifecycle = lifecycle;
    }

    @Override
    public void bindTo(MeterRegistry registry) {
        Gauge.builder("telephony_controller_ready", lifecycle, l -> l.ready() ? 1 : 0)
                .description("1 while every readiness check passes and no drain has started")
                .register(registry);
        FunctionCounter.builder("telephony_controller_healthy_file_errors", lifecycle, LifecycleFiles::healthyFileErrors)
                .description("Healthy-file writes that failed (each is tried again with a backoff)")
                .register(registry);
        FunctionCounter.builder("telephony_controller_log_proto_args", ProtoLogGuard.class, c -> ProtoLogGuard.caught())
                .description("Log lines that carried a protobuf message (never log one: ADR 0001 R1-5)")
                .register(registry);
        FunctionCounter.builder("telephony_controller_log_lines_dropped", CountingAsyncAppender.class,
                        c -> CountingAsyncAppender.dropped())
                .description("Log lines dropped because the log queue was full")
                .register(registry);
    }
}
