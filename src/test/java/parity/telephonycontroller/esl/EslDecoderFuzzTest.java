package parity.telephonycontroller.esl;

import com.code_intelligence.jazzer.junit.FuzzTest;

/**
 * The ESL decoder and the event parser on arbitrary bytes (TC ADR 0002 §1): frames or
 * {@link EslProtocolException}, nothing else, within the limits. Every run replays the corpus under
 * {@code src/test/resources/parity/telephonycontroller/esl/EslDecoderFuzzTestInputs/}; with
 * {@code JAZZER_FUZZ=1} (scripts/fuzz.sh) Jazzer fuzzes and adds what it finds.
 */
class EslDecoderFuzzTest {

    @FuzzTest(maxDuration = "10m")
    void decodesOrRefuses(byte[] data) {
        EslCodec.Decoder d = new EslCodec.Decoder();
        try {
            for (EslFrame f : d.feed(data, 0, data.length)) {
                if ("text/event-json".equals(f.contentType())) {
                    NodeEvent.parse(f.body());
                }
            }
        } catch (EslProtocolException e) {
            // refused: the connection would close
        }
        try {
            NodeEvent.parse(data);
        } catch (EslProtocolException e) {
            // refused
        }
        if (d.buffered() > EslCodec.MAX_HEADER_BYTES + EslCodec.MAX_BODY_BYTES + 2) {
            throw new AssertionError("the decoder holds more than its limits");
        }
    }
}
