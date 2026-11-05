package parity.telephonycontroller.realnode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;

/**
 * The spike's browser agent for the real-node tests (TC ADR 0002 §10): SIP.js in headless Chromium
 * (Playwright's image, pinned by digest; sandbox on, as {@code pwuser}, Playwright's seccomp
 * profile), registering over WS on 5066 as {@code agent1}, answering one call and writing what it
 * heard ({@code browser-recv.wav}, {@code browser-result.json}, {@code browser.jsonl}) when the call
 * ends. Run with the docker CLI and labelled, so {@code scripts/ci.sh} removes a leftover.
 */
public final class BrowserAgent implements AutoCloseable {

    public static final String IMAGE =
            "mcr.microsoft.com/playwright:v1.64.0-noble@sha256:06a9939e57531807f8d5fd76ce44b53165ffb7d7501d87ab10e285c20b1e971f";
    public static final String NAME = "tc-test-browser";

    private final Path out;

    private BrowserAgent(Path out) {
        this.out = out;
    }

    /** Starts it; it registers with {@code password} and waits up to {@code maxWaitMs} for its call to end. */
    public static BrowserAgent start(String password, long maxWaitMs) throws IOException, InterruptedException {
        Path base = Path.of(System.getProperty("tc.basedir", ".")).toAbsolutePath();
        Path out = Files.createTempDirectory("tc-browser-out");
        Files.setPosixFilePermissions(out, PosixFilePermissions.fromString("rwxrwxrwx"));
        Freeswitch.docker(List.of("rm", "-f", NAME), true);
        Freeswitch.docker(List.of("run", "-d", "--name", NAME, "--label", "parity.test=parity-telephony-controller",
                "--network", "host", "--user", "pwuser", "--cap-drop", "ALL", "--cap-add", "SYS_CHROOT",
                "--security-opt", "label=disable",
                "--security-opt", "seccomp=" + base.resolve("experiments/compose/chromium-seccomp.json"),
                "--shm-size", "512m", "-w", "/page", "-e", "PAGE_QUERY=?password=" + password,
                "-v", base.resolve("experiments/agent-page") + ":/page:ro",
                "-v", base.resolve("experiments/prompts") + ":/prompts:ro",
                "-v", out + ":/out", "--entrypoint", "node", IMAGE, "/page/run.mjs", "/out", Long.toString(maxWaitMs)), false);
        return new BrowserAgent(out);
    }

    public Path out() {
        return out;
    }

    public boolean registered() {
        return Files.exists(out.resolve("registered"));
    }

    /** Whether it exited (its call ended and its files are written). */
    public boolean exited() throws IOException, InterruptedException {
        return Freeswitch.docker(List.of("inspect", "-f", "{{.State.Running}}", NAME), true).trim().equals("false");
    }

    public String logs() throws IOException, InterruptedException {
        return Freeswitch.docker(List.of("logs", "--tail", "50", NAME), true);
    }

    @Override
    public void close() {
        try {
            Freeswitch.docker(List.of("rm", "-f", NAME), true);
        } catch (IOException e) {
            // ci.sh removes it
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        TempDirs.delete(out);
    }
}
