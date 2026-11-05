package parity.telephonycontroller;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.async.ResultCallback;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;

/**
 * TC-S0-AC2 with real containers: two controllers from the real image, each with the same
 * host directory bind-mounted as {@code /run/parity-telephony-controller} (PLAN.md R1-5). The
 * first takes the node lock and becomes healthy; the second refuses to start (exit 3), and the
 * first keeps running, healthy. Then the first drains on {@code docker stop} and exits 0.
 *
 * <p>SELinux is enforcing on this host, so the containers run with {@code label=disable} (as
 * the spike and parity-contracts' tools do); they run as the host user, who owns the mounted
 * directory. Ryuk is off (failsafe's environment): the test removes its containers itself.
 */
class NodeLockContainerIT {

    private static final Path JAR = Path.of(System.getProperty("tc.jar", "target/parity-telephony-controller-0.1.0.jar"));
    private static final Path BASEDIR = Path.of(System.getProperty("tc.basedir", "."));
    /** Readiness without freeswitch and parity-ms: their TCP checks off. */
    private static final String CONFIG = "{\"parity\":{\"telephony-controller\":{\"readiness\":{\"endpoints\":"
            + "{\"freeswitch-esl\":\"\",\"parity-ms\":\"\"}},\"lifecycle\":{\"deregistration-delay\":\"0s\"},"
            + "\"grpc\":{\"port\":0},\"journal\":{\"dir\":\"/tmp/journal\"},"
            + "\"node\":{\"mode\":\"simulated\",\"media\":\"simulated\"}}}}";

    /** Every container this test makes carries it; scripts/ci.sh removes leftovers by it. */
    static final String LABEL = "parity.test";
    static final String LABEL_VALUE = "parity-telephony-controller";

    @TempDir
    Path hostLockDir;

    private static String hostUser() throws IOException {
        Path home = Path.of(System.getProperty("user.home"));
        Object uid = Files.getAttribute(home, "unix:uid");
        Object gid = Files.getAttribute(home, "unix:gid");
        return uid + ":" + gid;
    }

    @Test
    void aSecondContainerWithTheSameLockMountRefusesToStart() throws Exception {
        String user = hostUser();
        ImageFromDockerfile image = new ImageFromDockerfile("parity-telephony-controller-it", false)
                .withFileFromPath("Dockerfile", BASEDIR.resolve("Dockerfile"))
                .withFileFromPath("app.jar", JAR);
        String imageName = image.get();
        Bind lockMount = Bind.parse(hostLockDir.toAbsolutePath() + ":/run/parity-telephony-controller");

        try (GenericContainer<?> first = new GenericContainer<>(imageName)
                .withLabel(LABEL, LABEL_VALUE)
                .withEnv("SPRING_APPLICATION_JSON", CONFIG)
                .withEnv("MEDIA_NODE_ID", "media-it")
                .withCreateContainerCmdModifier(cmd -> cmd.withUser(user).getHostConfig()
                        .withBinds(lockMount).withSecurityOpts(List.of("label=disable")))
                .waitingFor(Wait.forLogMessage(".*\"event\":\"service.ready\".*", 1)
                        .withStartupTimeout(Duration.ofSeconds(60)))) {
            first.start();
            assertThat(first.execInContainer("test", "-f", "/tmp/parity-telephony-controller.healthy").getExitCode())
                    .as("the first is healthy").isZero();
            assertThat(hostLockDir.resolve("lock")).as("the lock file on the host").exists();

            DockerClient docker = DockerClientFactory.instance().client();
            CreateContainerResponse second = docker.createContainerCmd(imageName)
                    .withLabels(Map.of(LABEL, LABEL_VALUE))
                    .withEnv("SPRING_APPLICATION_JSON=" + CONFIG, "MEDIA_NODE_ID=media-it")
                    .withUser(user)
                    .withHostConfig(HostConfig.newHostConfig().withBinds(lockMount)
                            .withSecurityOpts(List.of("label=disable")))
                    .exec();
            try {
                docker.startContainerCmd(second.getId()).exec();
                int code = docker.waitContainerCmd(second.getId()).start()
                        .awaitStatusCode(60, TimeUnit.SECONDS);
                String logs = logs(docker, second.getId());
                Files.createDirectories(Path.of("target", "it-logs"));
                Files.writeString(Path.of("target", "it-logs", "container-second.log"), logs);
                Files.writeString(Path.of("target", "it-logs", "container-first.log"), first.getLogs());
                assertThat(code).as("the second container's exit code").isEqualTo(3);
                assertThat(logs).contains("another telephony controller holds the node lock");
            } finally {
                docker.removeContainerCmd(second.getId()).withForce(true).exec();
            }

            assertThat(first.isRunning()).as("the first still runs").isTrue();
            assertThat(first.execInContainer("test", "-f", "/tmp/parity-telephony-controller.healthy").getExitCode())
                    .as("the first is still healthy").isZero();

            // docker stop: SIGTERM, then the drain; the first exits 0 with its drained file.
            DockerClient client = DockerClientFactory.instance().client();
            client.stopContainerCmd(first.getContainerId()).withTimeout(20).exec();
            Long exit = client.inspectContainerCmd(first.getContainerId()).exec().getState().getExitCodeLong();
            Files.writeString(Path.of("target", "it-logs", "container-first.log"), first.getLogs());
            assertThat(exit).as("the first's exit code after docker stop").isZero();
            assertThat(first.getLogs()).contains("\"event\":\"drain.completed\"");
        }
    }

    private static String logs(DockerClient docker, String id) throws InterruptedException {
        StringBuilder out = new StringBuilder();
        docker.logContainerCmd(id).withStdOut(true).withStdErr(true).withFollowStream(false)
                .exec(new ResultCallback.Adapter<Frame>() {
                    @Override
                    public void onNext(Frame frame) {
                        out.append(new String(frame.getPayload(), java.nio.charset.StandardCharsets.UTF_8));
                    }
                }).awaitCompletion(30, TimeUnit.SECONDS);
        return out.toString();
    }
}
