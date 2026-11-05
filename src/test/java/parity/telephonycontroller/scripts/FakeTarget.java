package parity.telephonycontroller.scripts;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** fake-telephony-controller, driven as interaction-engine's tests drive it: its control endpoint over HTTP. */
final class FakeTarget implements ScriptTarget {

    private static final JsonMapper JSON = JsonMapper.builder().build();

    private final URI control;
    private final ManagedChannel channel;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();

    FakeTarget(int grpcPort, int controlPort) {
        this.control = URI.create("http://127.0.0.1:" + controlPort + "/_fake/v1/");
        this.channel = ManagedChannelBuilder.forAddress("127.0.0.1", grpcPort).usePlaintext().build();
    }

    @Override
    public Map<Long, Long> reset(JsonNode given) throws Exception {
        JsonNode r = call("reset", given);
        Map<Long, Long> out = new LinkedHashMap<>();
        r.path("epochs").properties().forEach(e -> out.put(Long.parseLong(e.getKey()), Long.parseLong(e.getValue().asString())));
        return out;
    }

    @Override
    public long inject(String what, JsonNode args) throws Exception {
        JsonNode body = JSON.createObjectNode().put("what", what).set("args", args);
        JsonNode revision = call("inject", body).get("revision");
        return revision == null ? 0 : Long.parseLong(revision.asString());
    }

    @Override
    public void advance(long ms) throws Exception {
        call("advance", JSON.createObjectNode().put("ms", Long.toString(ms)));
    }

    @Override
    public List<String> invariants() throws Exception {
        HttpResponse<String> r = http.send(HttpRequest.newBuilder(control.resolve("invariants")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        List<String> out = new ArrayList<>();
        JSON.readTree(r.body()).path("violations").forEach(v -> out.add(v.asString()));
        return out;
    }

    private JsonNode call(String path, JsonNode body) throws Exception {
        HttpResponse<String> r = http.send(HttpRequest.newBuilder(control.resolve(path))
                .POST(HttpRequest.BodyPublishers.ofString(body.toString())).build(), HttpResponse.BodyHandlers.ofString());
        if (r.statusCode() != 200) {
            throw new IllegalStateException("control " + path + ": " + r.statusCode() + " " + r.body());
        }
        return JSON.readTree(r.body());
    }

    @Override
    public ManagedChannel channel() {
        return channel;
    }

    @Override
    public String name() {
        return "fake";
    }

    @Override
    public void close() {
        channel.shutdownNow();
    }
}
