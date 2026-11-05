package parity.telephonycontroller.realnode;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * freeswitch 1.10.12 for the real-node tests (TC ADR 0002 §7, §10): the spike's image, pinned by
 * digest; TC's {@code freeswitch/conf} with {@code compose/freeswitch-test}'s overlay; host network,
 * non-root, every capability dropped. Run with the docker CLI and labelled, so {@code scripts/ci.sh}
 * removes a leftover. Its logs and CDR files go to a temporary directory the test can read.
 */
public final class Freeswitch implements AutoCloseable {

    public static final String IMAGE =
            "docker.io/safarov/freeswitch:1.10.12@sha256:b31c743f4c911a19687c61e3214968f2a24f93f9d3d667cc26284192e158ffc6";
    public static final String ESL_PASSWORD = "tc-test-esl";
    public static final String NAME = "tc-test-freeswitch";

    private final Path out;

    private Freeswitch(Path out) {
        this.out = out;
    }

    /** A fresh freeswitch (any earlier one of this name removed). */
    public static Freeswitch start() throws IOException, InterruptedException {
        Path base = Path.of(System.getProperty("tc.basedir", ".")).toAbsolutePath();
        Path out = Files.createTempDirectory("tc-fs-out");
        Files.createDirectories(out.resolve("json_cdr"));
        Files.createDirectories(out.resolve("log"));
        for (Path p : List.of(out, out.resolve("json_cdr"), out.resolve("log"))) {
            Files.setPosixFilePermissions(p, PosixFilePermissions.fromString("rwxrwxrwx"));
        }
        docker(List.of("rm", "-f", NAME), true);
        List<String> cmd = new ArrayList<>(List.of("run", "-d", "--name", NAME, "--label", "parity.test=parity-telephony-controller",
                "--network", "host", "--security-opt", "label=disable", "--cap-drop", "ALL",
                "--user", uid() + ":" + gid(), "--tmpfs", "/tmp:mode=1777",
                "-v", base.resolve("freeswitch/conf") + ":/etc/freeswitch:ro",
                "-v", base.resolve("compose/freeswitch-test/vars.xml") + ":/etc/freeswitch/vars.xml:ro",
                "-v", base.resolve("compose/freeswitch-test/directory") + ":/etc/freeswitch/directory:ro",
                "-v", out + ":/tc-out", "--entrypoint", "/usr/bin/freeswitch", IMAGE,
                "-nonat", "-nf", "-nc", "-np", "-conf", "/etc/freeswitch", "-log", "/tc-out/log", "-db", "/tmp/db",
                "-run", "/tmp/run", "-temp", "/tmp", "-cache", "/tmp/cache", "-storage", "/tmp/storage",
                "-recordings", "/tmp/rec", "-sounds", "/tmp/sounds"));
        docker(cmd, false);
        return new Freeswitch(out);
    }

    public Path cdrDir() {
        return out.resolve("json_cdr");
    }

    public Path log() {
        return out.resolve("log/freeswitch.log");
    }

    /** freeswitch dies ({@code SIGKILL}: no BYE, no event) and starts again: a new Core-UUID. */
    public void killAndRestart() throws IOException, InterruptedException {
        docker(List.of("kill", "-s", "KILL", NAME), false);
        docker(List.of("start", NAME), false);
    }

    /** {@code SIGSTOP} for a while, then {@code SIGCONT} (the 3 s stall test, R1-3). */
    public void stall(long ms) throws IOException, InterruptedException {
        docker(List.of("kill", "-s", "STOP", NAME), false);
        Thread.sleep(ms);
        docker(List.of("kill", "-s", "CONT", NAME), false);
    }

    /** freeswitch's process id on the host (its /proc for the G-3 measurement). */
    public static String pid() throws IOException, InterruptedException {
        return docker(List.of("inspect", "-f", "{{.State.Pid}}", NAME), false).trim();
    }

    /** The output of a command inside the container (fs_cli, ss). */
    public String exec(String... args) throws IOException, InterruptedException {
        List<String> cmd = new ArrayList<>(List.of("exec", NAME));
        cmd.addAll(List.of(args));
        return docker(cmd, true);
    }

    @Override
    public void close() {
        try {
            docker(List.of("rm", "-f", NAME), true);
        } catch (IOException e) {
            // the next start removes it
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        TempDirs.delete(out);
    }

    static String docker(List<String> args, boolean tolerant) throws IOException, InterruptedException {
        List<String> cmd = new ArrayList<>(List.of("docker"));
        cmd.addAll(args);
        Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        String output = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        if (!p.waitFor(120, TimeUnit.SECONDS)) {
            p.destroyForcibly();
            throw new IOException("docker timed out");
        }
        if (p.exitValue() != 0 && !tolerant) {
            throw new IOException("docker failed");
        }
        return output;
    }

    private static String uid() throws IOException, InterruptedException {
        return run("id", "-u");
    }

    private static String gid() throws IOException, InterruptedException {
        return run("id", "-g");
    }

    private static String run(String... cmd) throws IOException, InterruptedException {
        Process p = new ProcessBuilder(cmd).start();
        String s = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
        p.waitFor();
        return s;
    }
}
