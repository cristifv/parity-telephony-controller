package parity.telephonycontroller.scripts;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import org.testcontainers.DockerClientFactory;
import parity.telephonycontroller.owner.OwnerRecord;
import parity.telephonycontroller.owner.OwnerWriter;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * IE's side of etcd, for the scripts against TC's server: owner keys and replica registrations
 * through the same JSON gateway. etcd assigns revisions, so a script's epochs are mapped: within a
 * script, real create revision = script epoch + a constant, which this writer keeps by padding the
 * revision with throwaway puts before each claim. {@code etcd_down} pauses the container.
 */
final class EtcdTestWriter implements OwnerWriter {

    private static final JsonMapper JSON = JsonMapper.builder().build();

    private final URI base;
    private final String containerId;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    private Long delta;
    private boolean paused;

    EtcdTestWriter(URI base, String containerId) {
        this.base = base;
        this.containerId = containerId;
    }

    @Override
    public long claim(String interactionId, long epoch, OwnerRecord v) {
        long current = revision();
        if (delta == null) {
            delta = Math.max(current + 1 - epoch, 0);
        }
        long target = epoch + delta;
        if (target <= current) {
            throw new IllegalStateException("script epoch " + epoch + " is too close to the previous one");
        }
        while (revision() < target - 1) {
            put("pad/x", "{}");
        }
        String value = JSON.createObjectNode().put("tenant_id", v.tenantId()).put("bin", v.bin())
                .put("replica_id", v.replicaId()).put("node_id", v.nodeId()).toString();
        long got = put("ie/owner/" + interactionId, value);
        if (got != target) {
            throw new IllegalStateException("etcd gave revision " + got + ", wanted " + target);
        }
        return got;
    }

    /** The live tests' claim: the owner record written as is; its create revision is the epoch. */
    long claimNow(String interactionId, OwnerRecord v) {
        String value = JSON.createObjectNode().put("tenant_id", v.tenantId()).put("bin", v.bin())
                .put("replica_id", v.replicaId()).put("node_id", v.nodeId()).toString();
        return put("ie/owner/" + interactionId, value);
    }

    @Override
    public void delete(String interactionId) {
        post("/v3/kv/deleterange", "{\"key\":\"" + b64("ie/owner/" + interactionId) + "\"}");
    }

    @Override
    public void register(String bin, String replicaId) {
        put("ie/replicas/" + bin + "/" + replicaId, "{}");
    }

    @Override
    public void setAvailable(boolean up) {
        var docker = DockerClientFactory.instance().client();
        if (!up && !paused) {
            docker.pauseContainerCmd(containerId).exec();
            paused = true;
        } else if (up && paused) {
            docker.unpauseContainerCmd(containerId).exec();
            paused = false;
        }
    }

    @Override
    public void clear() {
        setAvailable(true);
        post("/v3/kv/deleterange", "{\"key\":\"" + b64("ie/") + "\",\"range_end\":\"" + b64("ie0") + "\"}");
        delta = null;
    }

    private long revision() {
        JsonNode r = post("/v3/kv/range", "{\"key\":\"" + b64("pad/x") + "\"}");
        return Long.parseLong(r.path("header").path("revision").asString());
    }

    private long put(String key, String value) {
        JsonNode r = post("/v3/kv/put", "{\"key\":\"" + b64(key) + "\",\"value\":\"" + b64(value) + "\"}");
        return Long.parseLong(r.path("header").path("revision").asString());
    }

    private JsonNode post(String path, String body) {
        try {
            HttpResponse<String> r = http.send(HttpRequest.newBuilder(base.resolve(path))
                    .timeout(Duration.ofSeconds(5)).POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                    HttpResponse.BodyHandlers.ofString());
            if (r.statusCode() != 200) {
                throw new IllegalStateException("etcd " + path + " answered " + r.statusCode());
            }
            return JSON.readTree(r.body());
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static String b64(String s) {
        return Base64.getEncoder().encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }
}
