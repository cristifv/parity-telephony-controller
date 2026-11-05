package parity.telephonycontroller.esl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Function;
import tools.jackson.databind.json.JsonMapper;

/**
 * fake-freeswitch (TC ADR 0002 §10): an ESL server on 127.0.0.1 that speaks the protocol (the
 * greeting, {@code auth}, {@code event json}, {@code filter}, {@code api}, {@code bgapi},
 * {@code sendmsg}, {@code sendevent}) and lets a test answer commands and fire events. Fault knobs:
 * a stall (as {@code SIGSTOP}: nothing read, nothing answered, nothing sent until resumed), dropped
 * connections, a restart (a new {@code Core-UUID}), dropped, duplicated or reordered events,
 * delayed replies.
 */
final class FakeFreeswitch implements AutoCloseable {

    private static final JsonMapper JSON = JsonMapper.builder().build();

    private final String password;
    private final ServerSocket server;
    private final List<Conn> conns = new CopyOnWriteArrayList<>();
    private final List<String> seen = new CopyOnWriteArrayList<>();
    private final AtomicInteger accepted = new AtomicInteger();
    private final ReentrantLock gate = new ReentrantLock();
    private final Condition resumed = gate.newCondition();
    private volatile boolean stalled;
    private volatile boolean closed;
    private volatile String coreUuid = UUID.randomUUID().toString();
    private volatile Function<String, String> api = cmd -> "-ERR command not found";
    private volatile BiFunction<String, EslCodec.Command, String> sendmsg = (uuid, c) -> "+OK";
    private volatile long replyDelayMs;
    private volatile String delayOnlyFor;
    private volatile boolean dropEvents;
    private volatile boolean duplicateEvents;
    private volatile boolean jobEventBeforeReply;
    private final AtomicInteger sequence = new AtomicInteger();

    FakeFreeswitch(String password) throws IOException {
        this.password = password;
        this.server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        Thread.ofVirtual().name("fake-fs-accept").start(this::accept);
    }

    int port() {
        return server.getLocalPort();
    }

    String coreUuid() {
        return coreUuid;
    }

    /** Every command line received, in order (auth lines masked). */
    List<String> commandsSeen() {
        return List.copyOf(seen);
    }

    int connectionsAccepted() {
        return accepted.get();
    }

    int openConnections() {
        return (int) conns.stream().filter(c -> !c.socket.isClosed()).count();
    }

    void onApi(Function<String, String> f) {
        api = f;
    }

    void onSendmsg(BiFunction<String, EslCodec.Command, String> f) {
        sendmsg = f;
    }

    /** Replies to commands whose line starts with {@code prefix} wait {@code ms} (null: every command). */
    void delayReplies(String prefix, long ms) {
        delayOnlyFor = prefix;
        replyDelayMs = ms;
    }

    void dropEvents(boolean on) {
        dropEvents = on;
    }

    void duplicateEvents(boolean on) {
        duplicateEvents = on;
    }

    /** A bgapi's BACKGROUND_JOB goes out before its {@code +OK} reply (it may overtake it). */
    void jobEventBeforeReply(boolean on) {
        jobEventBeforeReply = on;
    }

    /** As SIGSTOP: nothing is read, answered or sent until {@link #resume}. */
    void stall() {
        stalled = true;
    }

    void resume() {
        gate.lock();
        try {
            stalled = false;
            resumed.signalAll();
        } finally {
            gate.unlock();
        }
    }

    /** Every connection closes (freeswitch's listener killed, or the TCP connection lost). */
    void dropConnections() {
        for (Conn c : conns) {
            c.close();
        }
    }

    /** A restart: every connection closes and the next run has a new {@code Core-UUID}. */
    void restart() {
        coreUuid = UUID.randomUUID().toString();
        dropConnections();
    }

    /** Fires an event to every subscribed connection (with Event-Name, Core-UUID and a sequence). */
    void fire(Map<String, String> headers) {
        if (dropEvents) {
            return;
        }
        Map<String, String> h = new LinkedHashMap<>(headers);
        h.putIfAbsent("Core-UUID", coreUuid);
        h.put("Event-Sequence", Integer.toString(sequence.incrementAndGet()));
        for (Conn c : conns) {
            if (c.subscribed) {
                c.event(h);
                if (duplicateEvents) {
                    c.event(h);
                }
            }
        }
    }

    /** Several events to every subscriber in the order given (a test reorders them itself). */
    void fireAll(List<Map<String, String>> events) {
        events.forEach(this::fire);
    }

    @Override
    public void close() {
        closed = true;
        try {
            server.close();
        } catch (IOException ignored) {
            // closing
        }
        dropConnections();
    }

    private void accept() {
        while (!closed) {
            try {
                Socket s = server.accept();
                accepted.incrementAndGet();
                Conn c = new Conn(s);
                conns.add(c);
                Thread.ofVirtual().name("fake-fs-conn").start(c::run);
            } catch (IOException e) {
                return;
            }
        }
    }

    private void waitIfStalled() {
        gate.lock();
        try {
            while (stalled && !closed) {
                resumed.awaitUninterruptibly();
            }
        } finally {
            gate.unlock();
        }
    }

    private final class Conn {
        final Socket socket;
        final OutputStream out;
        // One writer per connection, so replies and events leave in the order they were made.
        final java.util.concurrent.LinkedBlockingQueue<byte[]> outbox = new java.util.concurrent.LinkedBlockingQueue<>();
        volatile boolean subscribed;
        volatile boolean authed;

        Conn(Socket socket) throws IOException {
            this.socket = socket;
            this.out = socket.getOutputStream();
            Thread.ofVirtual().name("fake-fs-writer").start(this::writer);
        }

        void writer() {
            try {
                while (!socket.isClosed()) {
                    byte[] b = outbox.poll(100, java.util.concurrent.TimeUnit.MILLISECONDS);
                    if (b == null) {
                        continue;
                    }
                    waitIfStalled();
                    out.write(b);
                    out.flush();
                }
            } catch (IOException | InterruptedException e) {
                close();
            }
        }

        void run() {
            try (InputStream in = socket.getInputStream()) {
                write(frame(Map.of("Content-Type", "auth/request"), null));
                EslCodec.CommandDecoder d = new EslCodec.CommandDecoder();
                byte[] chunk = new byte[8192];
                while (!closed) {
                    waitIfStalled();
                    int n = in.read(chunk);
                    if (n < 0) {
                        break;
                    }
                    for (EslCodec.Command c : d.feed(chunk, 0, n)) {
                        waitIfStalled();
                        handle(c);
                    }
                }
            } catch (IOException | EslProtocolException e) {
                // the connection ends
            } finally {
                close();
            }
        }

        void handle(EslCodec.Command c) throws IOException {
            String line = c.line();
            seen.add(line.startsWith("auth ") ? "auth ***" : line);
            if (!authed) {
                if (line.equals("auth " + password)) {
                    authed = true;
                    reply("+OK accepted");
                } else {
                    reply("-ERR invalid");
                    close();
                }
                return;
            }
            delay(line);
            if (line.startsWith("event json")) {
                subscribed = true;
                reply("+OK event listener enabled json");
            } else if (line.startsWith("filter ")) {
                reply("+OK filter added");
            } else if (line.startsWith("api ")) {
                String cmd = line.substring(4);
                String body = cmd.equals("global_getvar core_uuid") ? coreUuid : api.apply(cmd);
                write(frame(Map.of("Content-Type", "api/response"), body.getBytes(StandardCharsets.UTF_8)));
            } else if (line.startsWith("bgapi ")) {
                String job = c.header("Job-UUID");
                String result = api.apply(line.substring(6));
                Map<String, String> ev = new LinkedHashMap<>();
                ev.put("Event-Name", "BACKGROUND_JOB");
                ev.put("Job-UUID", job);
                ev.put("_body", result);
                if (jobEventBeforeReply) {
                    fire(ev);
                    reply("+OK Job-UUID: " + job);
                } else {
                    reply("+OK Job-UUID: " + job);
                    fire(ev);
                }
            } else if (line.startsWith("sendmsg ")) {
                reply(sendmsg.apply(line.substring(8).trim(), c));
            } else if (line.startsWith("sendevent ")) {
                Map<String, String> ev = new LinkedHashMap<>(c.headers());
                ev.put("Event-Name", line.substring(10).trim());
                reply("+OK " + UUID.randomUUID());
                fire(ev);
            } else {
                reply("-ERR command not found");
            }
        }

        void delay(String line) {
            long ms = replyDelayMs;
            String only = delayOnlyFor;
            if (ms > 0 && (only == null || line.startsWith(only))) {
                try {
                    Thread.sleep(ms);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        void reply(String text) throws IOException {
            write(frame(Map.of("Content-Type", "command/reply", "Reply-Text", text), null));
        }

        void event(Map<String, String> headers) {
            outbox.add(frame(Map.of("Content-Type", "text/event-json"), JSON.writeValueAsBytes(headers)));
        }

        void write(byte[] b) {
            outbox.add(b);
        }

        void close() {
            try {
                socket.close();
            } catch (IOException ignored) {
                // closing
            }
            conns.remove(this);
        }
    }

    static byte[] frame(Map<String, String> headers, byte[] body) {
        StringBuilder sb = new StringBuilder();
        Map<String, String> h = new LinkedHashMap<>(headers);
        if (body != null) {
            h.put("Content-Length", Integer.toString(body.length));
        }
        h.forEach((k, v) -> sb.append(k).append(": ").append(v).append('\n'));
        sb.append('\n');
        byte[] head = sb.toString().getBytes(StandardCharsets.UTF_8);
        if (body == null) {
            return head;
        }
        List<byte[]> parts = new ArrayList<>(List.of(head, body));
        byte[] out = new byte[head.length + body.length];
        int at = 0;
        for (byte[] p : parts) {
            System.arraycopy(p, 0, out, at, p.length);
            at += p.length;
        }
        return out;
    }
}
