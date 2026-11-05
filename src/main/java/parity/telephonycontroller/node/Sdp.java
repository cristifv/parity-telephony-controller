package parity.telephonycontroller.node;

import java.util.ArrayList;
import java.util.List;

/**
 * What a re-INVITE's handling needs from an SDP (TC ADR 0002 §5): the {@code o=} version, the
 * audio stream's address, port and formats, and its direction. Never logged.
 */
record Sdp(long version, String address, int port, List<String> codecs, String direction) {

    static final Sdp NONE = new Sdp(-1, "", -1, List.of(), "sendrecv");

    /** The {@code o=} line's session version, or -1. */
    static long version(String sdp) {
        return parse(sdp).version();
    }

    static Sdp parse(String sdp) {
        if (sdp == null || sdp.isEmpty()) {
            return NONE;
        }
        long version = -1;
        String sessionAddress = "";
        String mediaAddress = null;
        int port = -1;
        List<String> codecs = new ArrayList<>();
        String direction = "sendrecv";
        boolean inAudio = false;
        for (String raw : sdp.lines().toList()) {
            String line = raw.trim();
            if (line.startsWith("o=")) {
                String[] f = line.substring(2).split("\\s+", -1);
                if (f.length >= 3) {
                    try {
                        version = Long.parseLong(f[2]);
                    } catch (NumberFormatException e) {
                        version = -1;
                    }
                }
            } else if (line.startsWith("m=")) {
                inAudio = line.startsWith("m=audio ");
                if (inAudio) {
                    String[] f = line.substring(2).split("\\s+", -1);
                    try {
                        port = Integer.parseInt(f[1]);
                    } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
                        port = -1;
                    }
                    for (int i = 3; i < f.length; i++) {
                        codecs.add(f[i]);
                    }
                }
            } else if (line.startsWith("c=")) {
                String[] f = line.substring(2).split("\\s+", -1);
                String addr = f.length >= 3 ? f[2] : "";
                if (inAudio) {
                    mediaAddress = addr;
                } else if (port < 0) {
                    sessionAddress = addr;
                }
            } else if (inAudio && (line.equals("a=sendonly") || line.equals("a=recvonly") || line.equals("a=inactive")
                    || line.equals("a=sendrecv"))) {
                direction = line.substring(2);
            }
        }
        return new Sdp(version, mediaAddress != null ? mediaAddress : sessionAddress, port, List.copyOf(codecs), direction);
    }

    @Override
    public String toString() {
        return "Sdp[version " + version + ", " + codecs.size() + " formats]";
    }
}
