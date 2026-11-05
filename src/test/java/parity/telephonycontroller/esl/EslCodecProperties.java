package parity.telephonycontroller.esl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.Size;

/**
 * The ESL codec's properties (TC ADR 0002 §1): frames survive any split of the byte stream; encoding
 * then decoding is the identity; arbitrary bytes give frames or {@link EslProtocolException}, never
 * another exception; the encoder refuses CR, LF and NUL wherever a command or header value goes.
 */
class EslCodecProperties {

    @Property(tries = 300)
    void framesSurviveAnySplit(@ForAll("frames") List<EslFrame> frames, @ForAll("cuts") List<Integer> cuts) throws Exception {
        byte[] wire = wire(frames);
        EslCodec.Decoder d = new EslCodec.Decoder();
        List<EslFrame> got = new ArrayList<>();
        int at = 0;
        for (int c : cuts) {
            int n = Math.min(c, wire.length - at);
            got.addAll(d.feed(wire, at, n));
            at += n;
        }
        got.addAll(d.feed(wire, at, wire.length - at));
        assertThat(got).isEqualTo(frames);
        assertThat(d.buffered()).isZero();
    }

    @Property(tries = 300)
    void encodeThenDecodeIsTheIdentity(@ForAll("commandLines") String line, @ForAll("headerMaps") Map<String, String> headers,
            @ForAll("bodies") byte[] body) throws Exception {
        byte[] wire = EslCodec.command(line, headers, body.length == 0 ? null : body);
        List<EslCodec.Command> got = new EslCodec.CommandDecoder().feed(wire, 0, wire.length);
        assertThat(got).hasSize(1);
        EslCodec.Command c = got.get(0);
        assertThat(c.line()).isEqualTo(line);
        headers.forEach((k, v) -> assertThat(c.header(k)).isEqualTo(v));
        assertThat(c.body()).isEqualTo(body);
    }

    @Property(tries = 1000)
    void arbitraryBytesGiveFramesOrAProtocolError(@ForAll @Size(max = 4096) byte[] noise) {
        EslCodec.Decoder d = new EslCodec.Decoder();
        try {
            d.feed(noise, 0, noise.length);
        } catch (EslProtocolException e) {
            assertThat(e.reason()).isNotNull();
        }
        assertThat(d.buffered()).isLessThanOrEqualTo(EslCodec.MAX_HEADER_BYTES + EslCodec.MAX_BODY_BYTES + 2);
    }

    @Property(tries = 300)
    void theEncoderRefusesLineBreaksAndNul(@ForAll("commandLines") String line, @ForAll("breakers") String breaker,
            @ForAll @IntRange(min = 0, max = 3) int where) {
        String bad = line.substring(0, Math.min(where, line.length())) + breaker + line.substring(Math.min(where, line.length()));
        assertThatThrownBy(() -> EslCodec.command(bad)).isInstanceOf(EslCodec.Refused.class);
        assertThatThrownBy(() -> EslCodec.command("api status", Map.of("x-value", bad), null))
                .isInstanceOf(EslCodec.Refused.class);
    }

    @Property(tries = 50)
    void aBodyOverTheLimitIsRefusedBeforeItIsBuffered(@ForAll @IntRange(min = 1, max = 1000) int extra) {
        String head = "Content-Type: api/response\nContent-Length: " + (EslCodec.MAX_BODY_BYTES + extra) + "\n\n";
        byte[] b = head.getBytes(StandardCharsets.UTF_8);
        assertThatThrownBy(() -> new EslCodec.Decoder().feed(b, 0, b.length))
                .isInstanceOf(EslProtocolException.class);
    }

    @Provide
    Arbitrary<List<EslFrame>> frames() {
        Arbitrary<EslFrame> frame = Combinators.combine(headerMaps(), bodies()).as((h, body) -> {
            Map<String, String> headers = new LinkedHashMap<>(h);
            headers.put("Content-Type", "text/event-json");
            if (body.length > 0) {
                headers.put("Content-Length", Integer.toString(body.length));
            }
            return new EslFrame(headers, body);
        });
        return frame.list().ofMaxSize(6);
    }

    @Provide
    Arbitrary<List<Integer>> cuts() {
        return Arbitraries.integers().between(0, 300).list().ofMaxSize(40);
    }

    @Provide
    Arbitrary<String> commandLines() {
        return Arbitraries.strings().withCharRange('a', 'z').withChars(' ', '-', '_', '+', '.')
                .ofMinLength(1).ofMaxLength(40).filter(s -> !s.contains(": ") && !s.isBlank() && s.charAt(0) != ' ')
                .map(s -> "api " + s);
    }

    @Provide
    Arbitrary<Map<String, String>> headerMaps() {
        Arbitrary<String> names = Arbitraries.strings().withCharRange('a', 'z').withChars('-', '_').ofMinLength(1)
                .ofMaxLength(20).filter(n -> !n.equalsIgnoreCase("content-length") && !n.equalsIgnoreCase("content-type"));
        Arbitrary<String> values = Arbitraries.strings().ascii().ofMinLength(1).ofMaxLength(60)
                .filter(v -> v.chars().noneMatch(c -> c == '\r' || c == '\n' || c == 0) && v.charAt(0) != ' '
                        && v.charAt(v.length() - 1) != ' ');
        return Arbitraries.maps(names, values).ofMaxSize(8);
    }

    @Provide
    Arbitrary<byte[]> bodies() {
        return Arbitraries.bytes().array(byte[].class).ofMaxSize(512);
    }

    @Provide
    Arbitrary<String> breakers() {
        return Arbitraries.of("\n", "\r", "\u0000", "\r\n", "\n\n");
    }

    private static byte[] wire(List<EslFrame> frames) {
        StringBuilder sb = new StringBuilder();
        List<byte[]> parts = new ArrayList<>();
        for (EslFrame f : frames) {
            sb.setLength(0);
            f.headers().forEach((k, v) -> sb.append(k).append(": ").append(v).append('\n'));
            sb.append('\n');
            parts.add(sb.toString().getBytes(StandardCharsets.UTF_8));
            parts.add(f.body());
        }
        int n = parts.stream().mapToInt(p -> p.length).sum();
        byte[] out = new byte[n];
        int at = 0;
        for (byte[] p : parts) {
            System.arraycopy(p, 0, out, at, p.length);
            at += p.length;
        }
        return out;
    }
}
