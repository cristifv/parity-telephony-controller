package parity.telephonycontroller.esl;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * One ESL frame (TC ADR 0002 §1): header lines {@code Name: value}, an empty line, then exactly
 * {@code Content-Length} bytes of body. Headers keep their order; lookups ignore case.
 * {@link #toString()} names the content type and the header names only: values and bodies may
 * carry numbers, SDPs or variables, which are never logged.
 */
public final class EslFrame {

    private final Map<String, String> headers;
    private final Map<String, String> lower;
    private final byte[] body;

    public EslFrame(Map<String, String> headers, byte[] body) {
        this.headers = Collections.unmodifiableMap(new LinkedHashMap<>(headers));
        Map<String, String> l = new LinkedHashMap<>();
        headers.forEach((k, v) -> l.put(k.toLowerCase(Locale.ROOT), v));
        this.lower = l;
        this.body = body == null ? new byte[0] : body.clone();
    }

    /** The headers, in order. */
    public Map<String, String> headers() {
        return headers;
    }

    /** A header's value, ignoring case; null when absent. */
    public String header(String name) {
        return lower.get(name.toLowerCase(Locale.ROOT));
    }

    public String contentType() {
        String t = header("Content-Type");
        return t == null ? "" : t;
    }

    public byte[] body() {
        return body.clone();
    }

    public int bodyLength() {
        return body.length;
    }

    public String bodyText() {
        return new String(body, StandardCharsets.UTF_8);
    }

    /** {@code Reply-Text}'s outcome only: {@code +OK} or {@code -ERR} (the rest may echo arguments). */
    public String replyOutcome() {
        String r = header("Reply-Text");
        if (r == null) {
            return "";
        }
        return r.startsWith("+OK") ? "+OK" : r.startsWith("-ERR") ? "-ERR" : "";
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof EslFrame f && headers.equals(f.headers) && java.util.Arrays.equals(body, f.body);
    }

    @Override
    public int hashCode() {
        return headers.hashCode() * 31 + java.util.Arrays.hashCode(body);
    }

    @Override
    public String toString() {
        return "EslFrame[" + contentType() + ", headers " + headers.keySet() + ", body " + body.length + " bytes]";
    }
}
