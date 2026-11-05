package parity.telephonycontroller.realnode;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * One side's media in the real-media tests (TC ADR 0002 §10): G.711 µ-law frames every 20 ms whose
 * payload is one byte repeated (this side's marker), and a count of what arrives, by marker and by
 * source. parity-ms relays PCMU unchanged in passthrough, so a marker seen at the other end shows the
 * audio went through, each way.
 */
public final class RtpPeer implements AutoCloseable {

    private final DatagramSocket socket;
    private final byte marker;
    private final ConcurrentHashMap<Byte, AtomicInteger> received = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicInteger> sources = new ConcurrentHashMap<>();
    private volatile InetSocketAddress target;
    private volatile boolean closed;
    /** A tone instead of the marker (Hz; 0: the marker), for endpoints that decode (the browser). */
    private final int toneHz;
    /** The last second of what arrived, decoded (µ-law), for {@link #toneRatio}. */
    private final short[] recent = new short[8000];
    private final java.util.concurrent.atomic.AtomicLong total = new java.util.concurrent.atomic.AtomicLong();
    /** Audio packets received whose loudest sample is below 500 (silence), since {@link #resetQuiet}. */
    private final AtomicInteger quiet = new AtomicInteger();
    private final java.util.concurrent.locks.ReentrantLock recentLock = new java.util.concurrent.locks.ReentrantLock();
    private int recentAt;
    /** An RFC 4733 key to send next (payload type 101), or -1. */
    private volatile int dtmf = -1;

    public RtpPeer(int port, byte marker) throws IOException {
        this(port, marker, 0);
    }

    public RtpPeer(int port, byte marker, int toneHz) throws IOException {
        this.socket = new DatagramSocket(new InetSocketAddress(InetAddress.getLoopbackAddress(), port));
        this.marker = marker;
        this.toneHz = toneHz;
        Thread.ofVirtual().name("rtp-recv").start(this::receive);
        Thread.ofVirtual().name("rtp-send").start(this::send);
    }

    public int port() {
        return socket.getLocalPort();
    }

    /** Where to send from now on (null: stop sending). */
    public void sendTo(InetSocketAddress t) {
        target = t;
    }

    /** Frames received carrying {@code m}. */
    public int receivedWith(byte m) {
        AtomicInteger n = received.get(m);
        return n == null ? 0 : n.get();
    }

    public java.util.Map<String, AtomicInteger> sources() {
        return sources;
    }

    private void send() {
        byte[] pkt = new byte[172];
        pkt[0] = (byte) 0x80;
        pkt[1] = 0;
        int ssrc = (int) (System.nanoTime() & 0x7fffffff) ^ marker;
        java.util.Arrays.fill(pkt, 12, 172, marker);
        int seq = 0;
        long ts = 0;
        while (!closed) {
            InetSocketAddress t = target;
            int key = dtmf;
            if (t != null && key >= 0) {
                // RFC 4733: one event, 8 packets of 20 ms, the last three with the end bit; one timestamp.
                dtmf = -1;
                byte[] ev = new byte[16];
                for (int i = 0; i < 8; i++) {
                    ev[0] = (byte) 0x80;
                    ev[1] = (byte) (i == 0 ? 0xe5 : 0x65);
                    ev[2] = (byte) (seq >> 8);
                    ev[3] = (byte) seq;
                    for (int b = 0; b < 4; b++) {
                        ev[4 + b] = (byte) (ts >> (24 - 8 * b));
                        ev[8 + b] = (byte) (ssrc >> (24 - 8 * b));
                    }
                    int duration = 160 * (i + 1);
                    ev[12] = (byte) key;
                    ev[13] = (byte) ((i >= 5 ? 0x80 : 0) | 10);
                    ev[14] = (byte) (duration >> 8);
                    ev[15] = (byte) duration;
                    try {
                        socket.send(new DatagramPacket(ev, ev.length, t));
                    } catch (IOException e) {
                        return;
                    }
                    seq = (seq + 1) & 0xffff;
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
                ts += 160 * 8;
                continue;
            }
            if (t != null) {
                if (toneHz > 0) {
                    for (int i = 0; i < 160; i++) {
                        pkt[12 + i] = ulaw((short) (8000 * Math.sin(2 * Math.PI * toneHz * (ts + i) / 8000.0)));
                    }
                }
                pkt[2] = (byte) (seq >> 8);
                pkt[3] = (byte) seq;
                pkt[4] = (byte) (ts >> 24);
                pkt[5] = (byte) (ts >> 16);
                pkt[6] = (byte) (ts >> 8);
                pkt[7] = (byte) ts;
                pkt[8] = (byte) (ssrc >> 24);
                pkt[9] = (byte) (ssrc >> 16);
                pkt[10] = (byte) (ssrc >> 8);
                pkt[11] = (byte) ssrc;
                try {
                    socket.send(new DatagramPacket(pkt, pkt.length, t));
                } catch (IOException e) {
                    return;
                }
                seq = (seq + 1) & 0xffff;
                ts += 160;
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    private void receive() {
        byte[] buf = new byte[2048];
        while (!closed) {
            DatagramPacket p = new DatagramPacket(buf, buf.length);
            try {
                socket.receive(p);
            } catch (IOException e) {
                return;
            }
            if (p.getLength() > 12 && (buf[0] & 0xc0) == 0x80) {
                total.incrementAndGet();
                int peak = 0;
                recentLock.lock();
                try {
                    for (int i = 12; i < p.getLength(); i++) {
                        recent[recentAt] = linear(buf[i]);
                        peak = Math.max(peak, Math.abs(recent[recentAt]));
                        recentAt = (recentAt + 1) % recent.length;
                    }
                } finally {
                    recentLock.unlock();
                }
                if ((buf[1] & 0x7f) == 0 && peak < 500) {
                    quiet.incrementAndGet();
                }
                received.computeIfAbsent(buf[12], k -> new AtomicInteger()).incrementAndGet();
                sources.computeIfAbsent(p.getSocketAddress().toString(), k -> new AtomicInteger()).incrementAndGet();
            }
        }
    }

    /** Sends one RFC 4733 key (0-9; 10 is *, 11 is #) in the stream, instead of 160 ms of audio. */
    public void sendDigit(int key) {
        dtmf = key;
    }

    /** Silent PCMU packets received since {@link #resetQuiet} (a gap in a loop shows here). */
    public int quietPackets() {
        return quiet.get();
    }

    public void resetQuiet() {
        quiet.set(0);
    }

    /** Every RTP packet received. */
    public long receivedTotal() {
        return total.get();
    }

    /**
     * The power at {@code hz} in the last second received, over the mean power at the other
     * frequencies probed (Goertzel): well above 1 means the tone is there.
     */
    public double toneRatio(int hz, int... others) {
        short[] copy;
        recentLock.lock();
        try {
            copy = recent.clone();
        } finally {
            recentLock.unlock();
        }
        double want = goertzel(copy, hz);
        double rest = 0;
        for (int o : others) {
            rest += goertzel(copy, o);
        }
        return want / Math.max(1e-9, rest / Math.max(1, others.length));
    }

    static double goertzel(short[] x, int hz) {
        double w = 2 * Math.PI * hz / 8000.0;
        double c = 2 * Math.cos(w);
        double s1 = 0;
        double s2 = 0;
        for (short v : x) {
            double s0 = v + c * s1 - s2;
            s2 = s1;
            s1 = s0;
        }
        return s1 * s1 + s2 * s2 - c * s1 * s2;
    }

    /** G.711 µ-law encoding of one sample. */
    static byte ulaw(short sample) {
        int s = sample;
        int sign = (s >> 8) & 0x80;
        if (sign != 0) {
            s = -s;
        }
        s = Math.min(s, 32635) + 0x84;
        int exponent = 7;
        for (int mask = 0x4000; (s & mask) == 0 && exponent > 0; mask >>= 1) {
            exponent--;
        }
        int mantissa = (s >> (exponent + 3)) & 0x0f;
        return (byte) ~(sign | (exponent << 4) | mantissa);
    }

    /** G.711 µ-law decoding of one byte. */
    static short linear(byte u) {
        int v = ~u & 0xff;
        int t = ((v & 0x0f) << 3) + 0x84;
        t <<= (v & 0x70) >> 4;
        return (short) ((v & 0x80) != 0 ? 0x84 - t : t - 0x84);
    }

    /** A test SDP for this peer: PCMU and telephone-event on its port. */
    public String sdp(int version) {
        return TestSipAgent.sdp(port(), version, "0");
    }

    @Override
    public void close() {
        closed = true;
        socket.close();
    }
}
