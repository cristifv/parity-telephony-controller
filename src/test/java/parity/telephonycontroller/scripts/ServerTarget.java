package parity.telephonycontroller.scripts;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import parity.telephonycontroller.api.GrpcServer;
import parity.telephonycontroller.fake.ScriptHooks;
import parity.telephonycontroller.journal.FileJournal;
import parity.telephonycontroller.node.SimulatedNode;
import parity.telephonycontroller.owner.EtcdOwnerDirectory;
import parity.telephonycontroller.session.EngineHolder;
import parity.telephonycontroller.session.TelephonyNode;
import parity.telephonycontroller.time.ManualClock;
import tools.jackson.databind.JsonNode;

/**
 * TC's own server: the production engine, gRPC server and file journal, with etcd v3.7.2 in a
 * container (read through the JSON gateway, as in production) and the simulated node until steps 3
 * and 4. Only the clock is the scripts' (a manual one).
 */
final class ServerTarget implements ScriptTarget {

    static final String ETCD_IMAGE =
            "gcr.io/etcd-development/etcd@sha256:7c6c239825d00e3f6328a69caafd54be92063acf0c2ce78b8394699f52b75dc3";

    private final GenericContainer<?> etcd;
    private final Path journalDir;
    private final ScriptHooks hooks;
    private final EngineHolder holder;
    private final GrpcServer server;
    private final ManagedChannel channel;

    @SuppressWarnings("resource")
    ServerTarget() throws IOException {
        etcd = new GenericContainer<>(DockerImageName.parse(ETCD_IMAGE).asCompatibleSubstituteFor("etcd"))
                .withLabel("parity.test", "parity-telephony-controller")
                .withCommand("etcd", "--listen-client-urls=http://0.0.0.0:2379",
                        "--advertise-client-urls=http://127.0.0.1:2379", "--data-dir=/tmp/etcd")
                .withExposedPorts(2379)
                .waitingFor(Wait.forHttp("/health").forPort(2379).withStartupTimeout(Duration.ofSeconds(60)));
        etcd.start();
        URI base = URI.create("http://" + etcd.getHost() + ":" + etcd.getMappedPort(2379));
        EtcdTestWriter writer = new EtcdTestWriter(base, etcd.getContainerId());
        EtcdOwnerDirectory owners = new EtcdOwnerDirectory(base, Duration.ofMillis(500));
        journalDir = Files.createTempDirectory("tc-journal");
        FileJournal journal = new FileJournal(journalDir);
        ManualClock clock = new ManualClock(0);
        TelephonyNode.Config config = new TelephonyNode.Config("media-1", "test", Set.of("bin-a"),
                "00000000-0000-7000-8000-000000000000", List.of());
        SimulatedNode node = new SimulatedNode(clock, 1000);
        // As the fake profile: recordings into the null sink (nothing written), for the recording script.
        node.setRecordingNullSink(true);
        holder = new EngineHolder(config, clock, owners, journal, node);
        holder.start();
        hooks = new ScriptHooks(holder, clock, writer, owners, journal::clear);
        server = new GrpcServer(0, holder, true).start();
        channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.port()).usePlaintext().build();
    }

    @Override
    public Map<Long, Long> reset(JsonNode given) {
        return hooks.reset(given);
    }

    @Override
    public long inject(String what, JsonNode args) {
        return hooks.inject(what, args);
    }

    @Override
    public void advance(long ms) {
        hooks.advance(ms);
    }

    @Override
    public List<String> invariants() {
        return hooks.invariants();
    }

    @Override
    public ManagedChannel channel() {
        return channel;
    }

    @Override
    public String name() {
        return "server";
    }

    @Override
    public void close() {
        channel.shutdownNow();
        server.close();
        TelephonyNode t = holder.get();
        if (t != null) {
            t.close();
        }
        etcd.stop();
        parity.telephonycontroller.realnode.TempDirs.delete(journalDir);
    }
}
