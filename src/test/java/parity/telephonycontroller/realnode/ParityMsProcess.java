package parity.telephonycontroller.realnode;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * parity-ms for the real-media tests (TC ADR 0002 §10): its release binary run on the host, control
 * on loopback, media on 127.0.0.1 in ports 20000–20999, no real-time scheduling (no
 * {@code CAP_SYS_NICE} here). The binary comes from {@code -Dtc.parityMs} (a copy, so a rebuild
 * of parity-ms can't swap it mid-run). Stopped with {@code SIGKILL}: a {@code SIGTERM} drains for up
 * to an hour.
 */
public final class ParityMsProcess implements AutoCloseable {

    public static final int CONTROL_PORT = 22_230;

    private final Process process;
    private final Path dir;

    private ParityMsProcess(Process process, Path dir) {
        this.process = process;
        this.dir = dir;
    }

    public static ParityMsProcess start() throws IOException, InterruptedException {
        String binary = System.getProperty("tc.parityMs", "../parity-ms/target/release/media-server");
        Path dir = Files.createTempDirectory("tc-parity-ms");
        for (String d : List.of("rec", "prompts", "health")) {
            Files.createDirectories(dir.resolve(d));
        }
        Process p = new ProcessBuilder(Path.of(binary).toAbsolutePath().toString(), "--node-id", "media-1",
                "--control-listen", "127.0.0.1:" + CONTROL_PORT, "--media-ip", "127.0.0.1", "--advertise-ip", "127.0.0.1",
                "--port-min", "20000", "--port-max", "20999", "--sched-rtp", "other:0", "--sched-watchdog", "other:0",
                "--rtp-threads", "2", "--idle-timeout-secs", "600", "--recording-dir", dir.resolve("rec").toString(),
                "--prompt-dir", dir.resolve("prompts").toString(), "--health-dir", dir.resolve("health").toString(),
                // MS-012: the tests never write recording audio; R9 brings the encrypted sink.
                "--recording-sink", "null", "--log-json")
                .redirectErrorStream(true).redirectOutput(dir.resolve("parity-ms.log").toFile()).start();
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < until) {
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress(java.net.InetAddress.getLoopbackAddress(), CONTROL_PORT), 200);
                return new ParityMsProcess(p, dir);
            } catch (IOException e) {
                Thread.sleep(100);
            }
        }
        p.destroyForcibly();
        throw new IOException("parity-ms never listened");
    }

    /** A tenant prompt: {@code <prompt-dir>/<tenant>/<id>.wav}, a tone, 8 kHz mono 16-bit PCM. */
    public void prompt(String tenant, String id, int hz, int ms) throws IOException {
        int n = 8 * ms;
        java.nio.ByteBuffer b = java.nio.ByteBuffer.allocate(44 + 2 * n).order(java.nio.ByteOrder.LITTLE_ENDIAN);
        b.put("RIFF".getBytes(java.nio.charset.StandardCharsets.US_ASCII)).putInt(36 + 2 * n)
                .put("WAVEfmt ".getBytes(java.nio.charset.StandardCharsets.US_ASCII)).putInt(16).putShort((short) 1)
                .putShort((short) 1).putInt(8000).putInt(16000).putShort((short) 2).putShort((short) 16)
                .put("data".getBytes(java.nio.charset.StandardCharsets.US_ASCII)).putInt(2 * n);
        for (int i = 0; i < n; i++) {
            b.putShort((short) (8000 * Math.sin(2 * Math.PI * hz * i / 8000.0)));
        }
        Path d = dir.resolve("prompts").resolve(tenant);
        Files.createDirectories(d);
        Files.write(d.resolve(id + ".wav"), b.array());
    }

    /** A file that isn't audio, as a tenant prompt: {@code play} must refuse it and leave the call alone (R7). */
    public void corruptPrompt(String tenant, String id) throws IOException {
        Path d = dir.resolve("prompts").resolve(tenant);
        Files.createDirectories(d);
        Files.write(d.resolve(id + ".wav"), "RIFF\u0010\u0000\u0000\u0000WAVEnot audio at all".getBytes(java.nio.charset.StandardCharsets.US_ASCII));
    }

    /** Files under the recording directory (the null sink writes none). */
    public long recordingFiles() throws IOException {
        try (java.util.stream.Stream<Path> s = Files.walk(dir.resolve("rec"))) {
            return s.filter(Files::isRegularFile).count();
        }
    }

    /** How many calls parity-ms holds now ({@code list}). */
    public int calls() {
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(java.net.InetAddress.getLoopbackAddress(), CONTROL_PORT), 500);
            s.setSoTimeout(2_000);
            s.getOutputStream().write("{\"cmd\":\"list\",\"id\":\"t\"}\n".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            String line = new java.io.BufferedReader(new java.io.InputStreamReader(s.getInputStream(),
                    java.nio.charset.StandardCharsets.UTF_8)).readLine();
            return tools.jackson.databind.json.JsonMapper.builder().build().readTree(line).path("result").path("calls").size();
        } catch (IOException | RuntimeException e) {
            return -1;
        }
    }

    /** Opens a call behind TC's back (an orphan for the sweep to find). */
    public void openOrphan(String callId, String sdp) throws IOException {
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(java.net.InetAddress.getLoopbackAddress(), CONTROL_PORT), 500);
            s.setSoTimeout(2_000);
            tools.jackson.databind.node.ObjectNode o = tools.jackson.databind.json.JsonMapper.builder().build()
                    .createObjectNode().put("cmd", "open").put("id", "o").put("call_id", callId).put("sdp", sdp);
            s.getOutputStream().write((o.toString() + "\n").getBytes(java.nio.charset.StandardCharsets.UTF_8));
            new java.io.BufferedReader(new java.io.InputStreamReader(s.getInputStream(),
                    java.nio.charset.StandardCharsets.UTF_8)).readLine();
        }
    }

    /** Whether parity-ms still holds this call ({@code query}). */
    public boolean has(String callId) {
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(java.net.InetAddress.getLoopbackAddress(), CONTROL_PORT), 500);
            s.setSoTimeout(2_000);
            s.getOutputStream().write(("{\"cmd\":\"query\",\"id\":\"t\",\"call_id\":\"" + callId + "\"}\n")
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8));
            String line = new java.io.BufferedReader(new java.io.InputStreamReader(s.getInputStream(),
                    java.nio.charset.StandardCharsets.UTF_8)).readLine();
            return tools.jackson.databind.json.JsonMapper.builder().build().readTree(line).path("ok").asBoolean(false);
        } catch (IOException | RuntimeException e) {
            return true;
        }
    }

    public Path log() {
        return dir.resolve("parity-ms.log");
    }

    @Override
    public void close() {
        process.destroyForcibly();
        try {
            process.waitFor(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        TempDirs.delete(dir);
    }
}
