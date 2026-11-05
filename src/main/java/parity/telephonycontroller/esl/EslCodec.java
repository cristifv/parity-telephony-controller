package parity.telephonycontroller.esl;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * The ESL wire format (TC ADR 0002 §1), both ways, with no I/O.
 *
 * <p>{@link Decoder} is incremental: bytes arrive in any split and frames come out whole. Its limits
 * bound memory whatever freeswitch (or a fuzzer) sends: a header block of at most
 * {@value #MAX_HEADER_BYTES} bytes and {@value #MAX_HEADERS} headers, a body of at most
 * {@value #MAX_BODY_BYTES} bytes; past them, {@link EslProtocolException}.
 *
 * <p>The encoder refuses what could smuggle a second command into one: CR, LF or NUL in a command
 * line or a header value, a header name outside {@code [A-Za-z0-9_-]}. A multi-line argument (an
 * SDP) travels only as a body with its {@code Content-Length}.
 */
public final class EslCodec {

    public static final int MAX_HEADER_BYTES = 16 * 1024;
    public static final int MAX_HEADERS = 64;
    public static final int MAX_BODY_BYTES = 32 * 1024 * 1024;

    private static final Pattern HEADER_NAME = Pattern.compile("[A-Za-z0-9_-]{1,64}");

    private EslCodec() {}

    /** A value the encoder refuses: the message says which rule, never the value. */
    public static final class Refused extends IllegalArgumentException {
        private static final long serialVersionUID = 1L;

        public Refused(@com.google.errorprone.annotations.CompileTimeConstant String rule) {
            super(rule);
        }
    }

    /** {@code line} then an empty line: {@code api …}, {@code event …}, {@code auth …}. */
    public static byte[] command(String line) {
        return command(line, Map.of(), null);
    }

    /**
     * A command with header lines and, optionally, a body: {@code Content-Length} is added for the
     * body (and {@code content-type: text/plain} if the headers name none).
     */
    public static byte[] command(String line, Map<String, String> headers, byte[] body) {
        checkLine(line);
        StringBuilder b = new StringBuilder(line).append('\n');
        boolean typed = false;
        for (Map.Entry<String, String> h : headers.entrySet()) {
            if (!HEADER_NAME.matcher(h.getKey()).matches()) {
                throw new Refused("a header name outside [A-Za-z0-9_-]");
            }
            if (h.getKey().equalsIgnoreCase("content-length")) {
                throw new Refused("content-length is the encoder's");
            }
            checkLine(h.getValue());
            typed |= h.getKey().equalsIgnoreCase("content-type");
            b.append(h.getKey()).append(": ").append(h.getValue()).append('\n');
        }
        if (body != null) {
            if (!typed) {
                b.append("content-type: text/plain\n");
            }
            b.append("content-length: ").append(body.length).append('\n');
        }
        b.append('\n');
        byte[] head = b.toString().getBytes(StandardCharsets.UTF_8);
        if (body == null) {
            return head;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream(head.length + body.length);
        out.writeBytes(head);
        out.writeBytes(body);
        return out.toByteArray();
    }

    /** No CR, LF or NUL: one line can't become two commands. */
    public static void checkLine(String s) {
        if (s == null || s.isEmpty()) {
            throw new Refused("an empty command line or header value");
        }
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\r' || c == '\n' || c == 0) {
                throw new Refused("CR, LF or NUL in a command line or header value");
            }
        }
    }

    /** A command as freeswitch reads it: its first line, its headers, its body (fake-freeswitch). */
    public static final class Command {
        private final String line;
        private final Map<String, String> headers;
        private final byte[] body;

        public Command(String line, Map<String, String> headers, byte[] body) {
            this.line = line;
            this.headers = java.util.Collections.unmodifiableMap(new LinkedHashMap<>(headers));
            this.body = body.clone();
        }

        public String line() {
            return line;
        }

        public Map<String, String> headers() {
            return headers;
        }

        public byte[] body() {
            return body.clone();
        }

        public String bodyText() {
            return new String(body, StandardCharsets.UTF_8);
        }

        /** A header, ignoring case; null when absent. */
        public String header(String name) {
            for (Map.Entry<String, String> h : headers.entrySet()) {
                if (h.getKey().equalsIgnoreCase(name)) {
                    return h.getValue();
                }
            }
            return null;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Command c && line.equals(c.line) && headers.equals(c.headers)
                    && java.util.Arrays.equals(body, c.body);
        }

        @Override
        public int hashCode() {
            return line.hashCode() * 31 + headers.hashCode();
        }

        @Override
        public String toString() {
            return "Command[" + line.length() + " chars, headers " + headers.keySet() + ", body " + body.length + "]";
        }
    }

    /** Commands from a byte stream (the server's side of {@link #command}): for fake-freeswitch. */
    public static final class CommandDecoder {

        private final Decoder frames = new Decoder(true);

        public List<Command> feed(byte[] data, int off, int len) throws EslProtocolException {
            List<Command> out = new ArrayList<>();
            for (EslFrame f : frames.feed(data, off, len)) {
                Map<String, String> h = new LinkedHashMap<>(f.headers());
                String line = h.remove(Decoder.COMMAND_LINE);
                out.add(new Command(line, h, f.body()));
            }
            return out;
        }
    }

    /** Frames from a byte stream, in order, however the bytes are split. Not thread-safe. */
    public static final class Decoder {

        /** In command mode, the first line of a block is kept under this pseudo-header. */
        static final String COMMAND_LINE = "\u0000command";

        private final boolean commands;
        private byte[] buf = new byte[4096];
        private int start;
        private int end;
        // A header block already parsed, waiting for its body (so a large body isn't rescanned).
        private Map<String, String> pendingHeaders;
        private int pendingBodyLen;

        /** freeswitch's frames. */
        public Decoder() {
            this(false);
        }

        Decoder(boolean commands) {
            this.commands = commands;
        }

        /** Feeds bytes; returns every frame now complete. */
        public List<EslFrame> feed(byte[] data, int off, int len) throws EslProtocolException {
            append(data, off, len);
            List<EslFrame> out = new ArrayList<>();
            while (true) {
                EslFrame f = next();
                if (f == null) {
                    return out;
                }
                out.add(f);
            }
        }

        /** Bytes received and not yet part of a frame (tests). */
        public int buffered() {
            return end - start;
        }

        private void append(byte[] data, int off, int len) throws EslProtocolException {
            if (end + len > buf.length) {
                compact();
                if (end + len > buf.length) {
                    long need = (long) end + len;
                    if (need > (long) MAX_HEADER_BYTES + MAX_BODY_BYTES + 2) {
                        throw new EslProtocolException(EslProtocolException.Reason.BODY_TOO_LONG);
                    }
                    int size = buf.length;
                    while (size < need) {
                        size = (int) Math.min((long) size * 2, (long) MAX_HEADER_BYTES + MAX_BODY_BYTES + 2);
                    }
                    buf = java.util.Arrays.copyOf(buf, size);
                }
            }
            System.arraycopy(data, off, buf, end, len);
            end += len;
        }

        private void compact() {
            if (start > 0) {
                System.arraycopy(buf, start, buf, 0, end - start);
                end -= start;
                start = 0;
            }
        }

        /** One frame, or null if the bytes so far don't hold a whole one. */
        private EslFrame next() throws EslProtocolException {
            if (pendingHeaders != null) {
                if (end - start < pendingBodyLen) {
                    return null;
                }
                return take(pendingHeaders, start, pendingBodyLen);
            }
            // Blank lines between frames are padding.
            while (start < end && buf[start] == '\n') {
                start++;
            }
            int headEnd = -1;
            for (int i = start; i + 1 < end; i++) {
                if (buf[i] == '\n' && buf[i + 1] == '\n') {
                    headEnd = i;
                    break;
                }
                if (i - start >= MAX_HEADER_BYTES) {
                    throw new EslProtocolException(EslProtocolException.Reason.HEADER_BLOCK_TOO_LONG);
                }
            }
            if (headEnd < 0) {
                if (end - start > MAX_HEADER_BYTES) {
                    throw new EslProtocolException(EslProtocolException.Reason.HEADER_BLOCK_TOO_LONG);
                }
                return null;
            }
            Map<String, String> headers = parseHeaders(buf, start, headEnd, commands);
            int bodyLen = 0;
            String cl = null;
            for (Map.Entry<String, String> h : headers.entrySet()) {
                if (h.getKey().equalsIgnoreCase("Content-Length")) {
                    cl = h.getValue();
                }
            }
            if (cl != null) {
                bodyLen = parseLength(cl.trim());
            }
            int bodyStart = headEnd + 2;
            if (end - bodyStart < bodyLen) {
                pendingHeaders = headers;
                pendingBodyLen = bodyLen;
                start = bodyStart;
                return null;
            }
            return take(headers, bodyStart, bodyLen);
        }

        private EslFrame take(Map<String, String> headers, int bodyStart, int bodyLen) {
            byte[] body = java.util.Arrays.copyOfRange(buf, bodyStart, bodyStart + bodyLen);
            start = bodyStart + bodyLen;
            pendingHeaders = null;
            pendingBodyLen = 0;
            if (start == end) {
                start = 0;
                end = 0;
            }
            return new EslFrame(headers, body);
        }

        private static Map<String, String> parseHeaders(byte[] b, int from, int to, boolean commands)
                throws EslProtocolException {
            Map<String, String> headers = new LinkedHashMap<>();
            int lineStart = from;
            boolean first = commands;
            for (int i = from; i <= to; i++) {
                if (i == to || b[i] == '\n') {
                    int lineEnd = i > lineStart && b[i - 1] == '\r' ? i - 1 : i;
                    if (lineEnd > lineStart) {
                        String line = new String(b, lineStart, lineEnd - lineStart, StandardCharsets.UTF_8);
                        if (first) {
                            headers.put(COMMAND_LINE, line);
                            first = false;
                            lineStart = i + 1;
                            continue;
                        }
                        int colon = line.indexOf(": ");
                        if (colon <= 0) {
                            throw new EslProtocolException(EslProtocolException.Reason.BAD_HEADER_LINE);
                        }
                        if (headers.size() >= MAX_HEADERS) {
                            throw new EslProtocolException(EslProtocolException.Reason.TOO_MANY_HEADERS);
                        }
                        headers.put(line.substring(0, colon), line.substring(colon + 2));
                    }
                    lineStart = i + 1;
                }
            }
            return headers;
        }

        private static int parseLength(String s) throws EslProtocolException {
            if (s.isEmpty() || s.length() > 10) {
                throw new EslProtocolException(EslProtocolException.Reason.BAD_CONTENT_LENGTH);
            }
            long v = 0;
            for (int i = 0; i < s.length(); i++) {
                char c = s.charAt(i);
                if (c < '0' || c > '9') {
                    throw new EslProtocolException(EslProtocolException.Reason.BAD_CONTENT_LENGTH);
                }
                v = v * 10 + (c - '0');
            }
            if (v > MAX_BODY_BYTES) {
                throw new EslProtocolException(EslProtocolException.Reason.BODY_TOO_LONG);
            }
            return (int) v;
        }
    }
}
