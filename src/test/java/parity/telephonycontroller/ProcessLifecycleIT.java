package parity.telephonycontroller;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;

/**
 * TC-S0-AC1 and AC4 on the packaged jar, as real processes and real signals (ARCH §7.3): the
 * healthy file only after the readiness checks; {@code /readyz} and {@code /metrics}; the drain
 * on {@code SIGTERM} (exit 0, drained file); a drain past its timeout (exit 1, no drained
 * file); a second signal (exit 128 + n, no drained file); a configuration error (exit 2); a
 * second controller on the same lock (exit 3, the first one's files untouched). Each process's
 * output is kept in {@code target/it-logs/}.
 */
class ProcessLifecycleIT {

    private static final Path JAR = Path.of(System.getProperty("tc.jar", "target/parity-telephony-controller-0.1.0.jar"));
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    @TempDir
    Path dir;

    private final List<Process> processes = new ArrayList<>();
    private final List<ServerSocket> servers = new ArrayList<>();
    private String testName;

    @BeforeEach
    void name(TestInfo info) {
        testName = info.getTestMethod().map(m -> m.getName()).orElse("test");
    }

    @AfterEach
    void cleanUp() throws IOException {
        for (Process p : processes) {
            p.destroyForcibly();
        }
        for (ServerSocket s : servers) {
            s.close();
        }
    }

    private Path healthy() {
        return dir.resolve("parity-telephony-controller.healthy");
    }

    private Path drained() {
        return dir.resolve("parity-telephony-controller.drained");
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private ServerSocket listen(int port) throws IOException {
        ServerSocket s = new ServerSocket(port);
        servers.add(s);
        return s;
    }

    /** Starts the jar with this test's files; {@code extra} are more {@code --key=value} arguments. */
    private Process start(String label, int httpPort, String esl, String ms, String... extra) throws IOException {
        // Spring Boot joins a repeated command-line property with commas: an extra argument
        // replaces the default instead of repeating it.
        boolean ownHealthy = List.of(extra).stream().anyMatch(a -> a.startsWith("--parity.telephony-controller.lifecycle.healthy-file="));
        List<String> cmd = new ArrayList<>(List.of("java", "-Djdk.tracePinnedThreads=short", "-jar", JAR.toString(),
                "--server.port=" + httpPort,
                "--parity.telephony-controller.lock-dir=" + dir.resolve("lock"),
                ownHealthy ? "--parity.telephony-controller.instance=it" : "--parity.telephony-controller.lifecycle.healthy-file=" + healthy(),
                "--parity.telephony-controller.lifecycle.drained-file=" + drained(),
                "--parity.telephony-controller.lifecycle.deregistration-delay=200ms",
                "--parity.telephony-controller.grpc.port=0",
                "--parity.telephony-controller.journal.dir=" + dir.resolve("journal-" + label),
                "--parity.telephony-controller.readiness.endpoints.freeswitch-esl=" + esl,
                "--parity.telephony-controller.readiness.endpoints.parity-ms=" + ms,
                // The process lifecycle on the simulated node; freeswitch's own tests are RealNodeIT's.
                "--parity.telephony-controller.node.mode=simulated",
                "--parity.telephony-controller.node.media=simulated"));
        cmd.addAll(List.of(extra));
        Path logs = Path.of("target", "it-logs");
        Files.createDirectories(logs);
        Process p = new ProcessBuilder(cmd)
                .redirectErrorStream(true)
                .redirectOutput(logs.resolve(testName + "-" + label + ".log").toFile())
                .start();
        processes.add(p);
        return p;
    }

    private static void signal(Process p, String sig) throws IOException, InterruptedException {
        Process kill = new ProcessBuilder("kill", "-" + sig, Long.toString(p.pid())).inheritIO().start();
        assertThat(kill.waitFor()).isZero();
    }

    private static void await(String what, Duration timeout, BooleanSupplier condition) throws InterruptedException {
        Instant until = Instant.now().plus(timeout);
        while (!condition.getAsBoolean()) {
            if (Instant.now().isAfter(until)) {
                throw new AssertionError("timed out waiting for " + what);
            }
            Thread.sleep(50);
        }
    }

    private static int exitCode(Process p, Duration timeout) throws InterruptedException {
        assertThat(p.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS)).as("the process ended").isTrue();
        return p.exitValue();
    }

    private static HttpResponse<String> get(int port, String path) throws IOException, InterruptedException {
        return HTTP.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                .timeout(Duration.ofSeconds(2)).build(), HttpResponse.BodyHandlers.ofString());
    }

    private static int status(int port, String path) {
        try {
            return get(port, path).statusCode();
        } catch (IOException | InterruptedException e) {
            return -1;
        }
    }

    @Test
    void healthyOnlyAfterTheChecksThenDrainsOnSigtermAndExitsZero() throws Exception {
        int http = freePort();
        int esl = freePort();
        int ms = freePort();
        listen(esl);
        Process p = start("a", http, "127.0.0.1:" + esl, "127.0.0.1:" + ms);

        // parity-ms's port answers nothing yet: started, but not healthy and not ready.
        await("the management port", Duration.ofSeconds(30), () -> status(http, "/livez") == 200);
        Thread.sleep(1500);
        assertThat(healthy()).as("no healthy file while a check fails").doesNotExist();
        assertThat(status(http, "/readyz")).isEqualTo(503);
        String metrics = get(http, "/metrics").body();
        assertThat(metrics).contains("telephony_controller_ready{")
                .containsPattern("telephony_controller_ready\\{[^}]*\\} 0\\.0");
        for (String label : List.of("service=\"parity-telephony-controller\"", "zone=", "node=", "instance=", "version=\"0.1.0\"")) {
            assertThat(metrics).as(label).contains(label);
        }

        // parity-ms answers: healthy, ready.
        listen(ms);
        await("the healthy file", Duration.ofSeconds(5), () -> Files.exists(healthy()));
        await("/readyz 200", Duration.ofSeconds(5), () -> status(http, "/readyz") == 200);
        assertThat(get(http, "/metrics").body()).containsPattern("telephony_controller_ready\\{[^}]*\\} 1\\.0");

        signal(p, "TERM");
        assertThat(exitCode(p, Duration.ofSeconds(20))).isZero();
        assertThat(healthy()).doesNotExist();
        assertThat(drained()).exists();

        // logback-spring.xml's log-record.v1 lines (scripts/ci.sh checks them with contract-check):
        // the process's moments, in order, one incarnation, no seq twice.
        List<tools.jackson.databind.JsonNode> lines = new java.util.ArrayList<>();
        tools.jackson.databind.json.JsonMapper json = tools.jackson.databind.json.JsonMapper.builder().build();
        for (String line : Files.readAllLines(Path.of("target", "it-logs", testName + "-a.log"))) {
            if (line.startsWith("{")) {
                lines.add(json.readTree(line));
            }
        }
        List<String> events = lines.stream().map(l -> l.path("event").asString(""))
                .filter(e -> e.startsWith("service.") || e.startsWith("drain.")).toList();
        assertThat(events).containsSubsequence("service.starting", "service.not_ready", "service.ready",
                "service.draining", "drain.completed", "service.stopped");
        assertThat(lines.stream().map(l -> l.path("incarnation").asString("")).distinct()).hasSize(1);
        assertThat(lines).allSatisfy(l -> assertThat(l.path("seq").asLong()).isPositive());
        assertThat(lines.stream().map(l -> l.path("seq").asLong()).distinct().count()).as("no seq twice").isEqualTo(lines.size());
    }

    @Test
    void staleFilesAreRemovedAtStart() throws Exception {
        Files.createFile(healthy());
        Files.createFile(drained());
        int http = freePort();
        Process p = start("a", http, "127.0.0.1:" + freePort(), "");
        await("the stale files removed", Duration.ofSeconds(30), () -> !Files.exists(healthy()) && !Files.exists(drained()));
        Thread.sleep(1000);
        assertThat(healthy()).as("a failing check: still no healthy file").doesNotExist();
        signal(p, "TERM");
        assertThat(exitCode(p, Duration.ofSeconds(20))).isZero();
    }

    @Test
    void aDrainThatRunsOutOfTimeLeavesNoDrainedFileAndExitsOne() throws Exception {
        int http = freePort();
        Process p = start("a", http, "", "",
                "--spring.profiles.active=test", "--parity.telephony-controller.lifecycle.drain-hold=5s",
                "--parity.telephony-controller.lifecycle.drain-timeout=1s");
        await("the healthy file", Duration.ofSeconds(30), () -> Files.exists(healthy()));
        signal(p, "TERM");
        assertThat(exitCode(p, Duration.ofSeconds(20))).isEqualTo(1);
        assertThat(drained()).doesNotExist();
        assertThat(healthy()).doesNotExist();
    }

    @Test
    void aSecondTermEndsAtOnceWith143AndNoDrainedFile() throws Exception {
        secondSignal("TERM", 143);
    }

    @Test
    void aSecondIntEndsAtOnceWith130AndNoDrainedFile() throws Exception {
        secondSignal("INT", 130);
    }

    private void secondSignal(String sig, int expected) throws Exception {
        int http = freePort();
        Process p = start("a", http, "", "",
                "--spring.profiles.active=test", "--parity.telephony-controller.lifecycle.drain-hold=20s",
                "--parity.telephony-controller.lifecycle.drain-timeout=30s");
        await("the healthy file", Duration.ofSeconds(30), () -> Files.exists(healthy()));
        signal(p, "TERM");
        await("the drain to start", Duration.ofSeconds(5), () -> !Files.exists(healthy()));
        Thread.sleep(500);
        signal(p, sig);
        assertThat(exitCode(p, Duration.ofSeconds(5))).isEqualTo(expected);
        assertThat(drained()).doesNotExist();
    }

    @Test
    void aMisspelledKeyExitsTwo() throws Exception {
        Process p = start("a", freePort(), "", "", "--parity.telephony-controller.lifecycle.drain-timout=1s");
        assertThat(exitCode(p, Duration.ofSeconds(30))).isEqualTo(2);
        assertThat(healthy()).doesNotExist();
    }

    @Test
    void aYamlSyntaxErrorExitsTwo() throws Exception {
        Path bad = dir.resolve("bad.yml");
        Files.writeString(bad, "parity:\n  telephony-controller:\n    zone: [unclosed\n");
        Process p = start("a", freePort(), "", "", "--spring.config.additional-location=file:" + bad);
        assertThat(exitCode(p, Duration.ofSeconds(30))).isEqualTo(2);
        assertThat(healthy()).doesNotExist();
    }

    @Test
    void drainHoldOutsideTheTestProfileIsRefusedWithTwo() throws Exception {
        Process p = start("a", freePort(), "", "", "--parity.telephony-controller.lifecycle.drain-hold=5s");
        assertThat(exitCode(p, Duration.ofSeconds(30))).isEqualTo(2);
        assertThat(Files.readString(Path.of("target", "it-logs", testName + "-a.log"))).contains("drain-hold is test only");
    }

    @Test
    void theTestPeerHeaderOutsideTheTestProfilesIsRefusedWithTwo() throws Exception {
        // Fix round 1 of step 5: the test-only identity header must never reach production.
        Process p = start("a", freePort(), "", "", "--parity.telephony-controller.grpc.test-peer-header=true");
        assertThat(exitCode(p, Duration.ofSeconds(30))).isEqualTo(2);
        assertThat(Files.readString(Path.of("target", "it-logs", testName + "-a.log")))
                .contains("test-peer-header is test only");
    }

    @Test
    void aSignalDuringTheStartLeavesNoHealthyFileDrainsAndExitsZero() throws Exception {
        Path log = Path.of("target", "it-logs", testName + "-a.log");
        Process p = start("a", freePort(), "", "");
        await("the start's first line", Duration.ofSeconds(30),
                () -> Files.exists(log) && readQuietly(log).contains("Starting ParityTelephonyControllerApplication"));
        signal(p, "TERM");
        boolean sawHealthy = false;
        Instant until = Instant.now().plus(Duration.ofSeconds(30));
        while (p.isAlive() && Instant.now().isBefore(until)) {
            sawHealthy |= Files.exists(healthy());
            Thread.sleep(10);
        }
        assertThat(exitCode(p, Duration.ofSeconds(5))).isZero();
        assertThat(sawHealthy).as("a healthy file at any moment").isFalse();
        assertThat(drained()).exists();
        assertThat(readQuietly(log)).as("the signal came during the start").contains("a signal during the start");
    }

    @Test
    void aHealthyFileThatCantBeWrittenIsCountedAndRetried() throws Exception {
        Path readOnly = Files.createDirectories(dir.resolve("ro"));
        Path file = readOnly.resolve("parity-telephony-controller.healthy");
        Files.setPosixFilePermissions(readOnly, java.nio.file.attribute.PosixFilePermissions.fromString("r-xr-xr-x"));
        int http = freePort();
        Process p = start("a", http, "", "", "--parity.telephony-controller.lifecycle.healthy-file=" + file);
        await("a counted failure", Duration.ofSeconds(30),
                () -> metric(http, "telephony_controller_healthy_file_errors_total") >= 2);
        assertThat(p.isAlive()).isTrue();
        assertThat(file).doesNotExist();
        Files.setPosixFilePermissions(readOnly, java.nio.file.attribute.PosixFilePermissions.fromString("rwxr-xr-x"));
        await("the healthy file, once writable", Duration.ofSeconds(10), () -> Files.exists(file));
        signal(p, "TERM");
        assertThat(exitCode(p, Duration.ofSeconds(20))).isZero();
    }

    private static String readQuietly(Path file) {
        try {
            return Files.readString(file);
        } catch (IOException e) {
            return "";
        }
    }

    /** A counter's value from /metrics, or -1. */
    private static double metric(int port, String name) {
        try {
            for (String line : get(port, "/metrics").body().lines().toList()) {
                if (line.startsWith(name + "{")) {
                    return Double.parseDouble(line.substring(line.lastIndexOf(' ') + 1));
                }
            }
        } catch (IOException | InterruptedException | NumberFormatException e) {
            return -1;
        }
        return -1;
    }

    @Test
    void aConfigurationErrorExitsTwo() throws Exception {
        Process p = start("a", freePort(), "", "", "--parity.telephony-controller.node-id=not a node id!");
        assertThat(exitCode(p, Duration.ofSeconds(30))).isEqualTo(2);
        assertThat(healthy()).doesNotExist();
    }

    @Test
    void aSecondControllerOnTheSameLockExitsThreeAndLeavesTheFirstAlone() throws Exception {
        int http = freePort();
        Process first = start("first", http, "", "");
        await("the first's healthy file", Duration.ofSeconds(30), () -> Files.exists(healthy()));
        Instant healthySince = Files.getLastModifiedTime(healthy()).toInstant();

        Process second = start("second", freePort(), "", "");
        assertThat(exitCode(second, Duration.ofSeconds(30))).isEqualTo(3);
        assertThat(Files.readString(Path.of("target", "it-logs", testName + "-second.log")))
                .contains("another telephony controller holds the node lock");
        assertThat(healthy()).as("the second never touched the first's healthy file").exists();
        assertThat(Files.getLastModifiedTime(healthy()).toInstant()).isEqualTo(healthySince);
        assertThat(status(http, "/readyz")).isEqualTo(200);

        signal(first, "TERM");
        assertThat(exitCode(first, Duration.ofSeconds(20))).isZero();
        assertThat(drained()).exists();
    }
}
