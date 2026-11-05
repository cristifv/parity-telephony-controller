package parity.telephonycontroller.obs;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.LoggingEvent;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;
import parity.contracts.telephony.v1.Arrived;
import parity.contracts.telephony.v1.DtmfActionBinding;
import parity.contracts.telephony.v1.WatchResponse;

/** R1-5: a protobuf message that reaches a log line anyway shows only masked fields. */
class ProtoLogGuardTest {

    private final LoggerContext context = context();

    private static LoggerContext context() {
        LoggerContext c = new LoggerContext();
        c.setMDCAdapter(new ch.qos.logback.classic.util.LogbackMDCAdapter());
        return c;
    }
    private final JsonLogFormatter formatter = new JsonLogFormatter(JsonLogFormatter.Identity.of(new MockEnvironment()));

    private String format(String message, Object arg, String key, Object value) {
        LoggingEvent e = new LoggingEvent("fqcn", context.getLogger("t"), Level.INFO, message, null, new Object[] {arg});
        e.addKeyValuePair(new org.slf4j.event.KeyValuePair(key, value));
        return formatter.format(e);
    }

    @Test
    void aMessageArgumentAndAKeyValueAreMaskedByFieldName() {
        WatchResponse w = WatchResponse.newBuilder()
                .setArrived(Arrived.newBuilder().setCalledNumber("+447700900100").setCallerNumber("+447700900123"))
                .build();
        DtmfActionBinding b = DtmfActionBinding.newBuilder().setDtmfSequence("*1").build();
        long before = ProtoLogGuard.caught();

        String line = format("offered {}", w, "binding", b);

        assertThat(line).doesNotContain("+447700900100", "+447700900123", "\"*1\"", "*1\"");
        assertThat(line).contains("***100", "***123");
        assertThat(ProtoLogGuard.caught() - before).isEqualTo(2);
    }
}
