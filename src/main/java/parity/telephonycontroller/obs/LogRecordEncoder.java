package parity.telephonycontroller.obs;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.encoder.EncoderBase;
import java.nio.charset.StandardCharsets;
import org.springframework.core.env.Environment;

/**
 * Logback's side of {@link JsonLogFormatter}: log-record.v1 lines, UTF-8. Who is logging comes
 * from the application's settings when Spring Boot set the logging system up (it leaves its
 * {@link Environment} in the logger context), else from the system ({@link
 * JsonLogFormatter.Identity#fromSystem}): plain unit tests, and the moments before the start.
 */
public class LogRecordEncoder extends EncoderBase<ILoggingEvent> {

    private volatile JsonLogFormatter formatter = new JsonLogFormatter(JsonLogFormatter.Identity.fromSystem());

    @Override
    public void start() {
        Object env = getContext() == null ? null : getContext().getObject(Environment.class.getName());
        JsonLogFormatter.Identity id = env instanceof Environment e
                ? JsonLogFormatter.Identity.of(e)
                : JsonLogFormatter.Identity.fromSystem();
        LogProcess.identity(id);
        formatter = new JsonLogFormatter(id);
        super.start();
    }

    @Override
    public byte[] headerBytes() {
        return null;
    }

    @Override
    public byte[] encode(ILoggingEvent event) {
        return formatter.format(event).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] footerBytes() {
        return null;
    }
}
