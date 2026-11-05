package parity.telephonycontroller.realnode;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

/**
 * A small SIP user agent over UDP on 127.0.0.1, in test scope (TC ADR 0002 R1-11: SIPp's 3PCC
 * socket couldn't drive single steps, see evidence/step3/sipp-3pcc-probe). It plays the carrier
 * (it sends INVITEs to freeswitch) and the agents and PSTN numbers TC originates to (it receives
 * them), one step at a time: ring, answer, fail, cancel, hang up, re-INVITE with another SDP. It
 * answers what freeswitch sends in a dialog (BYE, re-INVITE, UPDATE, OPTIONS) and keeps a log of
 * every message, so tests can assert what freeswitch sent (for example: no re-INVITE to an agent).
 */
public final class TestSipAgent implements AutoCloseable {

    private final DatagramSocket socket;
    private final String host = "127.0.0.1";
    private final Map<String, Call> calls = new ConcurrentHashMap<>();
    private final List<Call> incoming = new CopyOnWriteArrayList<>();
    private final AtomicInteger branches = new AtomicInteger();
    private volatile boolean closed;

    public TestSipAgent(int port) throws SocketException {
        socket = new DatagramSocket(new InetSocketAddress(InetAddress.getLoopbackAddress(), port));
        Thread.ofVirtual().name("test-sip").start(this::read);
    }

    public int port() {
        return socket.getLocalPort();
    }

    /** One message, parsed. */
    public static final class Msg {
        final String first;
        final Map<String, List<String>> headers = new LinkedHashMap<>();
        final String body;
        final InetSocketAddress from;

        Msg(String first, String body, InetSocketAddress from) {
            this.first = first;
            this.body = body;
            this.from = from;
        }

        public boolean isRequest() {
            return !first.startsWith("SIP/2.0");
        }

        public String method() {
            return isRequest() ? first.substring(0, first.indexOf(' ')) : cseq().split(" ")[1];
        }

        public int status() {
            return isRequest() ? 0 : Integer.parseInt(first.split(" ")[1]);
        }

        public String h(String name) {
            List<String> v = headers.get(name.toLowerCase(Locale.ROOT));
            return v == null || v.isEmpty() ? null : v.get(0);
        }

        /** Every header, by lower-case name, values in arrival order. */
        public Map<String, List<String>> headers() {
            return java.util.Collections.unmodifiableMap(headers);
        }

        public List<String> all(String name) {
            List<String> v = headers.get(name.toLowerCase(Locale.ROOT));
            return v == null ? List.of() : v;
        }

        String cseq() {
            return h("cseq");
        }

        String branch() {
            String via = h("via");
            int i = via == null ? -1 : via.indexOf("branch=");
            if (i < 0) {
                return "";
            }
            String b = via.substring(i + 7);
            int end = b.indexOf(';');
            return end < 0 ? b.trim() : b.substring(0, end).trim();
        }
    }

    /** A dialog, either way. */
    public final class Call {
        final String callId;
        final boolean outbound;
        public final List<String> log = new CopyOnWriteArrayList<>();
        public volatile Msg invite;
        volatile InetSocketAddress peer;
        volatile String localTag = "t" + UUID.randomUUID().toString().substring(0, 8);
        volatile String remoteTag;
        volatile String remoteTarget;
        volatile String localSdp;
        public volatile String remoteSdp;
        public volatile int finalStatus;
        volatile boolean ended;
        final AtomicInteger cseq = new AtomicInteger(1);
        volatile String inviteBranch;
        volatile String fromHeader;
        volatile String toHeader;
        volatile String ruri;
        volatile Msg lastResponse;
        final Map<String, String> sentResponses = new ConcurrentHashMap<>();

        Call(String callId, boolean outbound) {
            this.callId = callId;
            this.outbound = outbound;
        }

        /** UAS: 180 Ringing. */
        public void ring() throws IOException {
            respond(invite, 180, "Ringing", null, null);
        }

        /** UAS: 200 OK with an SDP; extra headers (Session-Expires) as given. */
        public void answer(String sdp, Map<String, String> extra) throws IOException {
            localSdp = sdp;
            respond(invite, 200, "OK", sdp, extra);
        }

        public void answer(String sdp) throws IOException {
            answer(sdp, Map.of());
        }

        /** UAS: a final failure. */
        public void fail(int status) throws IOException {
            respond(invite, status, "Failed", null, null);
            finalStatus = status;
            ended = true;
        }

        /** UAC: CANCEL before a final response. */
        public void cancel() throws IOException {
            String m = "CANCEL " + ruri + " SIP/2.0\r\n"
                    + "Via: SIP/2.0/UDP " + host + ":" + port() + ";rport;branch=" + inviteBranch + "\r\n"
                    + "From: " + fromHeader + "\r\nTo: " + toHeader + "\r\nCall-ID: " + callId + "\r\n"
                    + "CSeq: " + 1 + " CANCEL\r\nMax-Forwards: 70\r\nContent-Length: 0\r\n\r\n";
            send(m, peer, this);
        }

        /** Either side: BYE. */
        public void bye() throws IOException {
            sendInDialog("BYE", null, Map.of());
            ended = true;
        }

        /** Either side: a re-INVITE with this SDP; ACKs the 200. */
        public void reinvite(String sdp) throws IOException {
            localSdp = sdp;
            sendInDialog("INVITE", sdp, Map.of());
        }

        void sendInDialog(String method, String sdp, Map<String, String> extra) throws IOException {
            int n = cseq.incrementAndGet();
            String branch = branch();
            String from = outbound ? fromHeader : toWithTag();
            String to = outbound ? toHeader + (remoteTag != null && !toHeader.contains("tag=") ? ";tag=" + remoteTag : "")
                    : fromHeader;
            StringBuilder m = new StringBuilder(method + " " + remoteTarget + " SIP/2.0\r\n")
                    .append("Via: SIP/2.0/UDP ").append(host).append(':').append(port()).append(";rport;branch=").append(branch)
                    .append("\r\nFrom: ").append(from).append("\r\nTo: ").append(to).append("\r\nCall-ID: ").append(callId)
                    .append("\r\nCSeq: ").append(n).append(' ').append(method)
                    .append("\r\nContact: <sip:ua@").append(host).append(':').append(port()).append(">\r\nMax-Forwards: 70\r\n");
            extra.forEach((k, v) -> m.append(k).append(": ").append(v).append("\r\n"));
            body(m, sdp);
            send(m.toString(), peer, this);
        }

        String toWithTag() {
            return invite.h("to") + (invite.h("to").contains("tag=") ? "" : ";tag=" + localTag);
        }

        /** The SDP this side last sent. */
        public String localSdp() {
            return localSdp;
        }

        public boolean received(String what) {
            return log.stream().anyMatch(l -> l.startsWith("<- " + what));
        }

        public long count(String what) {
            return log.stream().filter(l -> l.startsWith("<- " + what)).count();
        }

        public boolean awaitLog(Predicate<List<String>> p, Duration timeout) throws InterruptedException {
            long until = System.nanoTime() + timeout.toNanos();
            while (System.nanoTime() < until) {
                if (p.test(log)) {
                    return true;
                }
                Thread.sleep(10);
            }
            return p.test(log);
        }
    }

    /** UAC: an INVITE to freeswitch. */
    public Call invite(String target, String toUser, String fromUser, String callId, Map<String, String> headers, String sdp)
            throws IOException {
        Call c = new Call(callId, true);
        String[] hp = target.split(":");
        c.peer = new InetSocketAddress(hp[0], Integer.parseInt(hp[1]));
        c.ruri = "sip:" + toUser + "@" + target;
        c.remoteTarget = c.ruri;
        c.fromHeader = "<sip:" + fromUser + "@" + host + ":" + port() + ">;tag=" + c.localTag;
        c.toHeader = "<sip:" + toUser + "@" + target + ">";
        c.inviteBranch = branch();
        c.localSdp = sdp;
        calls.put(callId, c);
        StringBuilder m = new StringBuilder("INVITE " + c.ruri + " SIP/2.0\r\n")
                .append("Via: SIP/2.0/UDP ").append(host).append(':').append(port()).append(";rport;branch=").append(c.inviteBranch)
                .append("\r\nFrom: ").append(c.fromHeader).append("\r\nTo: ").append(c.toHeader)
                .append("\r\nCall-ID: ").append(callId).append("\r\nCSeq: 1 INVITE\r\nContact: <sip:").append(fromUser)
                .append('@').append(host).append(':').append(port()).append(">\r\nMax-Forwards: 70\r\n")
                .append("Allow: INVITE, ACK, BYE, CANCEL, OPTIONS, UPDATE\r\nSupported: timer\r\n");
        headers.forEach((k, v) -> m.append(k).append(": ").append(v).append("\r\n"));
        body(m, sdp);
        send(m.toString(), c.peer, c);
        return c;
    }

    /** UAS: the next INVITE from freeswitch matching {@code p} (by its headers). */
    public Call awaitIncoming(Predicate<Msg> p, Duration timeout) throws InterruptedException {
        long until = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < until) {
            for (Call c : incoming) {
                if (p.test(c.invite)) {
                    return c;
                }
            }
            Thread.sleep(10);
        }
        return null;
    }

    public Call call(String callId) {
        return calls.get(callId);
    }

    public List<Call> incoming() {
        return List.copyOf(incoming);
    }

    /** Hangs up every live dialog (a test's cleanup). */
    public void hangUpAll() {
        for (Call c : calls.values()) {
            try {
                if (!c.ended && c.finalStatus == 200) {
                    c.bye();
                } else if (!c.ended && c.outbound) {
                    c.cancel();
                } else if (!c.ended && !c.outbound && c.finalStatus == 0) {
                    c.fail(480);
                }
            } catch (IOException e) {
                // the test is over
            }
        }
    }

    public void forget() {
        calls.clear();
        incoming.clear();
    }

    @Override
    public void close() {
        closed = true;
        socket.close();
    }

    // ---- the wire --------------------------------------------------------------------------------

    private String branch() {
        return "z9hG4bK-test-" + branches.incrementAndGet() + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    private static void body(StringBuilder m, String sdp) {
        if (sdp == null) {
            m.append("Content-Length: 0\r\n\r\n");
        } else {
            byte[] b = sdp.getBytes(StandardCharsets.UTF_8);
            m.append("Content-Type: application/sdp\r\nContent-Length: ").append(b.length).append("\r\n\r\n").append(sdp);
        }
    }

    private void send(String text, InetSocketAddress to, Call c) throws IOException {
        byte[] b = text.getBytes(StandardCharsets.UTF_8);
        socket.send(new DatagramPacket(b, b.length, to));
        if (c != null) {
            c.log.add("-> " + text.substring(0, text.indexOf('\r')));
        }
    }

    private void respond(Msg req, int status, String reason, String sdp, Map<String, String> extra) throws IOException {
        Call c = calls.get(req.h("call-id"));
        StringBuilder m = new StringBuilder("SIP/2.0 " + status + " " + reason + "\r\n");
        for (String via : req.all("via")) {
            m.append("Via: ").append(via).append("\r\n");
        }
        String to = req.h("to");
        if (status > 100 && !to.contains("tag=")) {
            to = to + ";tag=" + (c == null ? "x" : c.localTag);
        }
        m.append("From: ").append(req.h("from")).append("\r\nTo: ").append(to).append("\r\nCall-ID: ")
                .append(req.h("call-id")).append("\r\nCSeq: ").append(req.cseq()).append("\r\n");
        if (status >= 180 && status < 300) {
            m.append("Contact: <sip:ua@").append(host).append(':').append(port()).append(">\r\n");
        }
        if (extra != null) {
            extra.forEach((k, v) -> m.append(k).append(": ").append(v).append("\r\n"));
        }
        body(m, sdp);
        String text = m.toString();
        if (c != null) {
            c.sentResponses.put(req.branch() + "/" + req.method(), text);
        }
        send(text, req.from, c);
    }

    private void read() {
        byte[] buf = new byte[65_535];
        while (!closed) {
            DatagramPacket p = new DatagramPacket(buf, buf.length);
            try {
                socket.receive(p);
            } catch (IOException e) {
                return;
            }
            String text = new String(p.getData(), 0, p.getLength(), StandardCharsets.UTF_8);
            try {
                handle(parse(text, (InetSocketAddress) p.getSocketAddress()));
            } catch (IOException | RuntimeException e) {
                // a message we don't understand: ignored
            }
        }
    }

    static Msg parse(String text, InetSocketAddress from) {
        int split = text.indexOf("\r\n\r\n");
        String head = split < 0 ? text : text.substring(0, split);
        String body = split < 0 ? "" : text.substring(split + 4);
        String[] lines = head.split("\r\n");
        Msg m = new Msg(lines[0], body, from);
        for (int i = 1; i < lines.length; i++) {
            int colon = lines[i].indexOf(':');
            if (colon <= 0) {
                continue;
            }
            String name = compact(lines[i].substring(0, colon).trim().toLowerCase(Locale.ROOT));
            m.headers.computeIfAbsent(name, k -> new ArrayList<>()).add(lines[i].substring(colon + 1).trim());
        }
        return m;
    }

    private static String compact(String n) {
        return switch (n) {
            case "i" -> "call-id";
            case "f" -> "from";
            case "t" -> "to";
            case "v" -> "via";
            case "m" -> "contact";
            case "l" -> "content-length";
            case "c" -> "content-type";
            default -> n;
        };
    }

    private void handle(Msg m) throws IOException {
        String callId = m.h("call-id");
        if (callId == null) {
            return;
        }
        Call c = calls.get(callId);
        if (!m.isRequest()) {
            if (c == null) {
                return;
            }
            c.log.add("<- " + m.status() + " " + m.method());
            onResponse(c, m);
            return;
        }
        String method = m.method();
        if (c != null) {
            c.log.add("<- " + method);
            String again = c.sentResponses.get(m.branch() + "/" + method);
            if (again != null && !method.equals("ACK")) {
                send(again, m.from, c);
                return;
            }
        }
        switch (method) {
            case "INVITE" -> {
                if (c == null) {
                    c = new Call(callId, false);
                    c.invite = m;
                    c.peer = m.from;
                    c.fromHeader = m.h("from");
                    c.remoteTarget = target(m.h("contact"), m.from);
                    c.remoteSdp = m.body;
                    calls.put(callId, c);
                    c.log.add("<- INVITE");
                    respond(m, 100, "Trying", null, null);
                    incoming.add(c);
                } else {
                    // A re-INVITE: answer with our SDP (or the last one).
                    c.remoteSdp = m.body.isEmpty() ? c.remoteSdp : m.body;
                    respond(m, 200, "OK", c.localSdp, Map.of());
                }
            }
            case "ACK" -> {
                // nothing to send
            }
            case "BYE" -> {
                respond(m, 200, "OK", null, null);
                if (c != null) {
                    c.ended = true;
                }
            }
            case "CANCEL" -> {
                respond(m, 200, "OK", null, null);
                if (c != null && c.invite != null && c.finalStatus == 0) {
                    respond(c.invite, 487, "Request Terminated", null, null);
                    c.finalStatus = 487;
                    c.ended = true;
                }
            }
            case "UPDATE" -> {
                Map<String, String> extra = new LinkedHashMap<>();
                if (m.h("session-expires") != null) {
                    extra.put("Session-Expires", m.h("session-expires"));
                    extra.put("Require", "timer");
                }
                respond(m, 200, "OK", m.body.isEmpty() ? null : c == null ? null : c.localSdp, extra);
            }
            case "OPTIONS", "INFO" -> respond(m, 200, "OK", null, null);
            default -> respond(m, 405, "Method Not Allowed", null, null);
        }
    }

    private void onResponse(Call c, Msg m) throws IOException {
        int status = m.status();
        String method = m.method();
        if (!method.equals("INVITE")) {
            return;
        }
        String to = m.h("to");
        int t = to == null ? -1 : to.indexOf("tag=");
        if (t >= 0) {
            c.remoteTag = to.substring(t + 4).split(";")[0];
        }
        if (status < 200) {
            return;
        }
        c.lastResponse = m;
        boolean initial = m.cseq().startsWith("1 ");
        if (status >= 300) {
            if (initial) {
                c.finalStatus = status;
                c.ended = true;
            }
            // ACK for a failure: the INVITE's branch, the To with its tag.
            String ack = "ACK " + c.ruri + " SIP/2.0\r\nVia: SIP/2.0/UDP " + host + ":" + port() + ";rport;branch="
                    + (initial ? c.inviteBranch : branch()) + "\r\nFrom: " + c.fromHeader + "\r\nTo: " + to
                    + "\r\nCall-ID: " + c.callId + "\r\nCSeq: " + m.cseq().split(" ")[0] + " ACK\r\nMax-Forwards: 70\r\n"
                    + "Content-Length: 0\r\n\r\n";
            send(ack, c.peer, c);
            return;
        }
        if (initial) {
            c.finalStatus = 200;
            c.remoteSdp = m.body;
            c.remoteTarget = target(m.h("contact"), c.peer);
        }
        String ack = "ACK " + c.remoteTarget + " SIP/2.0\r\nVia: SIP/2.0/UDP " + host + ":" + port() + ";rport;branch="
                + branch() + "\r\nFrom: " + (c.outbound ? c.fromHeader : c.toWithTag()) + "\r\nTo: "
                + (c.outbound ? to : c.fromHeader) + "\r\nCall-ID: " + c.callId + "\r\nCSeq: " + m.cseq().split(" ")[0]
                + " ACK\r\nMax-Forwards: 70\r\nContent-Length: 0\r\n\r\n";
        send(ack, c.peer, c);
    }

    private static String target(String contact, InetSocketAddress from) {
        if (contact == null) {
            return "sip:" + from.getHostString() + ":" + from.getPort();
        }
        int lt = contact.indexOf('<');
        int gt = contact.indexOf('>');
        String uri = lt >= 0 && gt > lt ? contact.substring(lt + 1, gt) : contact.split(";")[0];
        return uri;
    }

    /** A G.711 SDP from this agent's side (no one sends media in the scripts). */
    public static String sdp(int port, int version, String codecs) {
        return "v=0\r\no=test 1000 " + version + " IN IP4 127.0.0.1\r\ns=-\r\nc=IN IP4 127.0.0.1\r\nt=0 0\r\n"
                + "m=audio " + port + " RTP/AVP " + codecs + " 101\r\n"
                + (codecs.startsWith("0") ? "a=rtpmap:0 PCMU/8000\r\n" : "a=rtpmap:8 PCMA/8000\r\n")
                + "a=rtpmap:101 telephone-event/8000\r\na=fmtp:101 0-16\r\na=sendrecv\r\n";
    }

    public static void sleep(Duration d) {
        try {
            TimeUnit.MILLISECONDS.sleep(d.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
