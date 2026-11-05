package parity.telephonycontroller.owner;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Optional;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * etcd through its {@code /v3} JSON gateway (TC ADR 0001 R1-14): {@code POST /v3/kv/range}, which
 * is linearizable unless asked otherwise, over the JDK's HttpClient. No jetcd, so no Vert.x and no
 * unshaded Netty. Timeouts and transport failures are {@link OwnerDirectoryUnavailable}.
 */
public final class EtcdOwnerDirectory implements OwnerDirectory {

    private static final JsonMapper JSON = JsonMapper.builder().build();

    private final URI range;
    private final Duration timeout;
    private final HttpClient http;

    public EtcdOwnerDirectory(URI endpoint, Duration timeout) {
        this.range = endpoint.resolve("/v3/kv/range");
        this.timeout = timeout;
        this.http = HttpClient.newBuilder().connectTimeout(timeout).build();
    }

    @Override
    public Optional<OwnerRecord> owner(String interactionId) throws OwnerDirectoryUnavailable {
        JsonNode kv = first("ie/owner/" + interactionId);
        if (kv == null) {
            return Optional.empty();
        }
        long revision = number(kv.get("create_revision"));
        JsonNode raw = kv.get("value");
        String text = new String(Base64.getDecoder().decode(raw == null ? "" : raw.asString()), StandardCharsets.UTF_8);
        try {
            JsonNode v = JSON.readTree(text);
            return Optional.of(new OwnerRecord(revision, str(v, "tenant_id"), str(v, "bin"),
                    str(v, "replica_id"), str(v, "node_id")));
        } catch (RuntimeException e) {
            return Optional.of(new OwnerRecord(revision, null, null, null, null));
        }
    }

    @Override
    public boolean replicaRegistered(String bin, String replicaId) throws OwnerDirectoryUnavailable {
        return first("ie/replicas/" + bin + "/" + replicaId) != null;
    }

    private JsonNode first(String key) throws OwnerDirectoryUnavailable {
        String body = "{\"key\":\"" + Base64.getEncoder().encodeToString(key.getBytes(StandardCharsets.UTF_8)) + "\"}";
        HttpRequest request = HttpRequest.newBuilder(range)
                .timeout(timeout)
                .header("content-type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        try {
            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new OwnerDirectoryUnavailable(new IOException("etcd answered " + response.statusCode()));
            }
            JsonNode kvs = JSON.readTree(response.body()).path("kvs");
            return kvs.isArray() && !kvs.isEmpty() ? kvs.get(0) : null;
        } catch (IOException | RuntimeException e) {
            throw new OwnerDirectoryUnavailable(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new OwnerDirectoryUnavailable(e);
        }
    }

    /** The gateway writes 64-bit integers as JSON strings. */
    private static long number(JsonNode n) {
        if (n == null) {
            return 0;
        }
        return n.isNumber() ? n.longValue() : Long.parseLong(n.asString());
    }

    private static String str(JsonNode v, String name) {
        JsonNode n = v.get(name);
        return n == null || !n.isString() ? null : n.asString();
    }
}
