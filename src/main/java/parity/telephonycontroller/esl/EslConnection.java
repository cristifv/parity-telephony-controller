package parity.telephonycontroller.esl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One inbound ESL connection (TC ADR 0002 §1): a socket, a reader on a virtual thread, and a FIFO
 * of commands awaiting their reply. freeswitch answers a connection's commands in order, so the
 * FIFO's head owns the next {@code command/reply} or {@code api/response}.
 *
 * <p>R1-3: a command that times out keeps its slot, so its late reply is consumed there (and
 * counted) instead of answering the next command; the connection stays open. At most
 * {@code pipelineLimit} commands are outstanding, timed-out ones included; past that,
 * {@link Busy}. Events go to the {@link Listener} on the reader's thread, which must not block.
 * No {@code synchronized} anywhere (R1-2): the writer holds a {@link ReentrantLock}; the reader
 * takes no lock to pop the FIFO.
 */
public final class EslConnection implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(EslConnection.class);

    /** What the connection reports. Called on its reader thread: never block. */
    public interface Listener {
        /** An event frame ({@code text/event-json}). */
        void event(EslFrame frame);

        /** The connection is gone (closed by either side, or a protocol error). */
        void closed(EslConnection connection);
    }

    /** Every slot is taken: the caller tries another connection or fails fast. */
    public static final class Busy extends Exception {
        private static final long serialVersionUID = 1L;

        Busy() {
            super("every pipeline slot is taken", null, false, false);
        }
    }

    /** No reply within the timeout: the outcome is unknown. */
    public static final class Timeout extends Exception {
        private static final long serialVersionUID = 1L;

        Timeout() {
            super("no reply within the timeout", null, false, false);
        }
    }

    /** The connection closed before the reply came. */
    public static final class Closed extends Exception {
        private static final long serialVersionUID = 1L;

        Closed() {
            super("the ESL connection closed", null, false, false);
        }
    }

    /** freeswitch refused the password, or answered the greeting with something else. */
    public static final class AuthFailed extends Exception {
        private static final long serialVersionUID = 1L;

        AuthFailed() {
            super("ESL authentication failed", null, false, false);
        }
    }

    private static final class Pending {
        final CompletableFuture<EslFrame> reply = new CompletableFuture<>();
        volatile boolean timedOut;
    }

    private final Socket socket;
    private final OutputStream out;
    private final Listener listener;
    private final int pipelineLimit;
    private final ReentrantLock writeLock = new ReentrantLock();
    private final ConcurrentLinkedQueue<Pending> fifo = new ConcurrentLinkedQueue<>();
    private final AtomicInteger outstanding = new AtomicInteger();
    private final AtomicInteger overdue = new AtomicInteger();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final CompletableFuture<EslFrame> greeting = new CompletableFuture<>();
    private final AtomicLong lateReplies;
    private volatile long lastReadNanos = System.nanoTime();

    private EslConnection(Socket socket, Listener listener, int pipelineLimit, AtomicLong lateReplies)
            throws IOException {
        this.socket = socket;
        this.out = socket.getOutputStream();
        this.listener = listener;
        this.pipelineLimit = pipelineLimit;
        this.lateReplies = lateReplies;
    }

    /**
     * Connects, authenticates and starts the reader. {@code lateReplies} counts replies that came
     * after their command timed out (a metric shared by the node's connections).
     */
    public static EslConnection open(InetSocketAddress address, String password, Duration timeout, int pipelineLimit,
            Listener listener, AtomicLong lateReplies) throws IOException, AuthFailed, Timeout, Closed {
        Socket s = new Socket();
        try {
            s.setTcpNoDelay(true);
            s.connect(address, (int) timeout.toMillis());
            EslConnection c = new EslConnection(s, listener, pipelineLimit, lateReplies);
            Thread.ofVirtual().name("esl-reader").start(c::read);
            EslFrame hello = c.await(c.greeting, timeout);
            if (!"auth/request".equals(hello.contentType())) {
                c.close();
                throw new AuthFailed();
            }
            EslFrame reply;
            try {
                reply = c.call(EslCodec.command("auth " + password), timeout);
            } catch (Busy e) {
                c.close();
                throw new AuthFailed();
            }
            if (!"+OK".equals(reply.replyOutcome())) {
                c.close();
                throw new AuthFailed();
            }
            return c;
        } catch (IOException | AuthFailed | Timeout | Closed | RuntimeException e) {
            try {
                s.close();
            } catch (IOException ignored) {
                // closing a failed socket
            }
            throw e;
        }
    }

    /** Sends a command and waits for its reply; the slot stays if the wait times out. */
    public EslFrame call(byte[] block, Duration timeout) throws Busy, Timeout, Closed, IOException {
        Pending p = enqueue(block);
        try {
            return p.reply.get(timeout.toNanos(), TimeUnit.NANOSECONDS);
        } catch (TimeoutException e) {
            markOverdue(p);
            throw new Timeout();
        } catch (ExecutionException e) {
            throw new Closed();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            markOverdue(p);
            throw new Timeout();
        }
    }

    private void markOverdue(Pending p) {
        if (!p.reply.isDone()) {
            p.timedOut = true;
            overdue.incrementAndGet();
        }
    }

    /** Sends a command; the future completes with its reply (or exceptionally when closed). */
    public CompletableFuture<EslFrame> send(byte[] block) throws Busy, IOException {
        return enqueue(block).reply;
    }

    private Pending enqueue(byte[] block) throws Busy, IOException {
        writeLock.lock();
        try {
            if (closed.get()) {
                throw new IOException("closed");
            }
            if (outstanding.get() >= pipelineLimit) {
                throw new Busy();
            }
            Pending p = new Pending();
            fifo.add(p);
            outstanding.incrementAndGet();
            try {
                out.write(block);
                out.flush();
            } catch (IOException e) {
                close();
                throw e;
            }
            return p;
        } finally {
            writeLock.unlock();
        }
    }

    /** Commands that timed out and are still unanswered: a slow connection, avoided by routing. */
    public int overdue() {
        return overdue.get();
    }

    /** Commands sent and not yet answered (timed-out ones included). */
    public int outstanding() {
        return outstanding.get();
    }

    public int pipelineLimit() {
        return pipelineLimit;
    }

    public boolean isOpen() {
        return !closed.get();
    }

    /** When the last byte arrived (System.nanoTime). */
    public long lastReadNanos() {
        return lastReadNanos;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        try {
            socket.close();
        } catch (IOException ignored) {
            // already closing
        }
        Pending p;
        while ((p = fifo.poll()) != null) {
            outstanding.decrementAndGet();
            p.reply.completeExceptionally(new Closed());
        }
        greeting.completeExceptionally(new Closed());
        listener.closed(this);
    }

    private EslFrame await(CompletableFuture<EslFrame> f, Duration timeout) throws Timeout, Closed {
        try {
            return f.get(timeout.toNanos(), TimeUnit.NANOSECONDS);
        } catch (TimeoutException e) {
            throw new Timeout();
        } catch (ExecutionException e) {
            throw new Closed();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new Timeout();
        }
    }

    private void read() {
        EslCodec.Decoder decoder = new EslCodec.Decoder();
        byte[] chunk = new byte[64 * 1024];
        try (InputStream in = socket.getInputStream()) {
            while (!closed.get()) {
                int n = in.read(chunk);
                if (n < 0) {
                    break;
                }
                lastReadNanos = System.nanoTime();
                List<EslFrame> frames = decoder.feed(chunk, 0, n);
                for (EslFrame f : frames) {
                    dispatch(f);
                }
            }
        } catch (EslProtocolException e) {
            LOG.atWarn().addKeyValue("event", "esl.protocol_violated").addKeyValue("reason", e.reason().name())
                    .log("ESL protocol error; the connection is closed");
        } catch (IOException e) {
            if (!closed.get()) {
                LOG.atInfo().addKeyValue("event", "esl.connection_lost").log("ESL connection lost");
            }
        } finally {
            close();
        }
    }

    private void dispatch(EslFrame f) {
        switch (f.contentType()) {
            case "auth/request" -> greeting.complete(f);
            case "command/reply", "api/response" -> reply(f);
            case "text/event-json" -> listener.event(f);
            case "text/disconnect-notice", "text/rude-rejection" -> close();
            default -> LOG.atDebug().addKeyValue("event", "esl_frame.ignored").addKeyValue("content_type", f.contentType())
                    .log("ESL frame ignored");
        }
    }

    private void reply(EslFrame f) {
        Pending p = fifo.poll();
        if (p == null) {
            LOG.atWarn().addKeyValue("event", "esl_reply.unmatched").log("an ESL reply with no command waiting");
            return;
        }
        outstanding.decrementAndGet();
        if (p.timedOut) {
            overdue.decrementAndGet();
            lateReplies.incrementAndGet();
        }
        p.reply.complete(f);
    }
}
