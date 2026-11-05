package parity.telephonycontroller.node;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * The parity-ms client against a stand-in parity-ms (newline JSON over loopback TCP): the prompt wait
 * it reports must be shorter than TC's timeout or plays are refused (MS-042); plays carry TC's play
 * id; a request never takes longer than the timeout in all; an older parity-ms without DTMF locks
 * gets the agent's other codes instead of a failed leg (TC-048).
 */
class ParityMsClientTest {

    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final Duration TIMEOUT = Duration.ofMillis(250);

    /** A stand-in parity-ms: answers every request (or none, when silent) and keeps what it got. */
    private static final class Stand implements AutoCloseable {
        final ServerSocket server;
        final List<JsonNode> got = new CopyOnWriteArrayList<>();
        volatile ObjectNode hello = JSON.createObjectNode().put("incarnation", "inc-1");
        volatile boolean silent;

        Stand() throws IOException {
            server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
            Thread.ofVirtual().start(() -> {
                while (!server.isClosed()) {
                    try {
                        Socket s = server.accept();
                        Thread.ofVirtual().start(() -> serve(s));
                    } catch (IOException e) {
                        return;
                    }
                }
            });
        }

        private void serve(Socket s) {
            try (s) {
                BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
                OutputStream out = s.getOutputStream();
                String line;
                while ((line = in.readLine()) != null) {
                    JsonNode req = JSON.readTree(line);
                    String cmd = req.path("cmd").asString("");
                    if (!cmd.equals("hello") && !cmd.equals("list") && !cmd.equals("subscribe") && !cmd.equals("ping")) {
                        got.add(req);
                    }
                    if (silent && !cmd.equals("hello")) {
                        continue;
                    }
                    ObjectNode result = switch (cmd) {
                        case "hello" -> hello;
                        case "list" -> {
                            ObjectNode r = JSON.createObjectNode();
                            r.putArray("calls");
                            yield r;
                        }
                        case "play" -> JSON.createObjectNode().put("loop", req.path("loop").asBoolean(false));
                        default -> JSON.createObjectNode();
                    };
                    ObjectNode answer = JSON.createObjectNode().put("id", req.path("id").asString("")).put("ok", true);
                    answer.set("result", result);
                    out.write((JSON.writeValueAsString(answer) + "\n").getBytes(StandardCharsets.UTF_8));
                    out.flush();
                }
            } catch (IOException | RuntimeException e) {
                // the client went
            }
        }

        List<JsonNode> sent(String cmd) {
            return got.stream().filter(r -> r.path("cmd").asString("").equals(cmd)).toList();
        }

        @Override
        public void close() throws IOException {
            server.close();
        }
    }

    private Stand stand;
    private ParityMsClient client;

    private ParityMsClient start(ObjectNode hello) throws IOException {
        stand = new Stand();
        stand.hello = hello.put("incarnation", "inc-1");
        client = new ParityMsClient(new ParityMsClient.Config("127.0.0.1", stand.server.getLocalPort(), TIMEOUT, 2));
        client.start();
        return client;
    }

    private static ObjectNode hello(long promptWaitMs, String... features) {
        ObjectNode h = JSON.createObjectNode();
        if (promptWaitMs >= 0) {
            h.put("prompt_wait_ms", promptWaitMs);
        }
        var f = h.putArray("features");
        for (String x : features) {
            f.add(x);
        }
        return h;
    }

    @AfterEach
    void stop() throws IOException {
        if (client != null) {
            client.close();
        }
        if (stand != null) {
            stand.close();
        }
    }

    @Test
    void aPromptWaitNotShorterThanTheTimeoutRefusesPlaysWithoutSendingThem() throws Exception {
        ParityMsClient c = start(hello(250, "play_loop", "prompt_cache", "play_id"));
        assertThat(c.promptWaitMs()).isEqualTo(250);
        assertThat(c.promptWaitTooLong()).isTrue();
        assertThatThrownBy(() -> c.play("acme", "i-1", MediaPort.Side.A, "acme/music.wav", true, "tc-1", 1))
                .isInstanceOf(NodePort.Unavailable.class);
        assertThat(stand.sent("play")).as("never sent: it could start after TC gave up").isEmpty();
    }

    @Test
    void aPlayCarriesTcsPlayIdAndReadsTheLoopEcho() throws Exception {
        ParityMsClient c = start(hello(200, "play_loop", "prompt_cache", "play_id"));
        assertThat(c.promptWaitTooLong()).isFalse();
        assertThat(c.play("acme", "i-1", MediaPort.Side.A, "acme/music.wav", true, "tc-7", 1)).isTrue();
        assertThat(stand.sent("play")).singleElement().satisfies(r -> {
            assertThat(r.path("play_id").asString("")).isEqualTo("tc-7");
            assertThat(r.path("loop").asBoolean(false)).isTrue();
        });
    }

    @Test
    void aRequestNeverTakesLongerThanTheTimeoutInAll() throws Exception {
        ParityMsClient c = start(hello(200));
        stand.silent = true;
        long t0 = System.nanoTime();
        assertThatThrownBy(() -> c.play("acme", "i-1", MediaPort.Side.A, "acme/welcome.wav", false, "tc-8", 1))
                .isInstanceOf(NodePort.Unavailable.class);
        long ms = (System.nanoTime() - t0) / 1_000_000;
        assertThat(ms).as("TC gave up at its timeout (and closed the connection)").isBetween(200L, 1_000L);
    }

    @Test
    void anOlderParityMsGetsTheAgentsOtherCodesInsteadOfAFailedLeg() throws Exception {
        ParityMsClient c = start(hello(-1, "play_loop"));
        // A UUIDv7, as every interaction id is: its log record is call-scoped (the xsid).
        String call = "0192f6a1-7c3e-7b21-9a4d-5e6f70819401";
        List<NodePort.DtmfBinding> codes = List.of(new NodePort.DtmfBinding("*1", "RECORDING_PAUSE"),
                new NodePort.DtmfBinding("*2", "RECORDING_RESUME"), new NodePort.DtmfBinding("*3", "MUTE_TOGGLE"));
        c.dtmfActions("acme", call, MediaPort.Side.B, "agent", codes, 2_000, List.of("RECORDING"), List.of(), 1);
        assertThat(stand.sent("set_dtmf_actions")).singleElement().satisfies(r -> {
            assertThat(r.path("actions")).hasSize(1);
            assertThat(r.path("actions").get(0).path("action").asString("")).isEqualTo("mute_toggle");
            assertThat(r.has("lock")).as("an older parity-ms would ignore it").isFalse();
        });
        assertThat(c.dtmfCodesDropped.get()).isEqualTo(2);
        // A supervisor's mode codes, with MODE locked.
        List<NodePort.DtmfBinding> sup = List.of(new NodePort.DtmfBinding("*4", "LISTEN"),
                new NodePort.DtmfBinding("*5", "MUTE_TOGGLE"));
        c.dtmfActions("acme", call, MediaPort.Side.B, "supervisor", sup, 2_000, List.of("MODE"), List.of(), 1);
        assertThat(stand.sent("set_dtmf_actions")).last().satisfies(r -> assertThat(r.path("actions")).hasSize(1));
        // With lock support everything goes, with the locks.
        c.close();
        stand.close();
        ParityMsClient n = start(hello(200, "dtmf_action_locks"));
        n.dtmfActions("acme", call, MediaPort.Side.B, "agent", codes, 2_000, List.of("RECORDING"), List.of(), 1);
        assertThat(stand.sent("set_dtmf_actions")).singleElement().satisfies(r -> {
            assertThat(r.path("actions")).hasSize(3);
            assertThat(r.path("lock").get(0).asString("")).isEqualTo("recording");
        });
    }
}
