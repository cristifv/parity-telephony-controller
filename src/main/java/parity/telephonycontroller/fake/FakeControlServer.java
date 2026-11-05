package parity.telephonycontroller.fake;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * {@code fake-telephony-controller}'s control endpoint (TC ADR 0001 §6), test-only and outside
 * {@code telephony.v1}: HTTP on its own port (9702), JSON bodies.
 * <ul>
 *   <li>{@code POST /_fake/v1/reset}: a script's {@code given}; answers {@code {"epochs": {...}}}
 *   <li>{@code POST /_fake/v1/inject}: {@code {"what", "args"}}; answers {@code {"revision": n}}
 *   <li>{@code POST /_fake/v1/advance}: {@code {"ms"}}
 *   <li>{@code GET /_fake/v1/invariants}: {@code {"violations": [...]}}
 * </ul>
 * One request at a time, like a script's steps.
 */
public final class FakeControlServer implements AutoCloseable {

    private static final JsonMapper JSON = JsonMapper.builder().build();

    private final HttpServer http;
    private final ScriptHooks hooks;
    private final ReentrantLock serial = new ReentrantLock();

    public FakeControlServer(String host, int port, ScriptHooks hooks) {
        this.hooks = hooks;
        try {
            this.http = HttpServer.create(new InetSocketAddress(host, port), 16);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        http.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        http.createContext("/_fake/v1/", this::handle);
    }

    public FakeControlServer start() {
        http.start();
        return this;
    }

    public int port() {
        return http.getAddress().getPort();
    }

    @Override
    public void close() {
        http.stop(0);
    }

    private void handle(HttpExchange ex) throws IOException {
        serial.lock();
        try {
            String path = ex.getRequestURI().getPath();
            ObjectNode out = JSON.createObjectNode();
            switch (path) {
                case "/_fake/v1/reset" -> {
                    ObjectNode epochs = out.putObject("epochs");
                    for (Map.Entry<Long, Long> e : hooks.reset(body(ex)).entrySet()) {
                        epochs.put(Long.toString(e.getKey()), Long.toString(e.getValue()));
                    }
                }
                case "/_fake/v1/inject" -> {
                    JsonNode b = body(ex);
                    out.put("revision", Long.toString(hooks.inject(b.get("what").asString(), b.get("args"))));
                }
                case "/_fake/v1/advance" -> hooks.advance(Long.parseLong(body(ex).get("ms").asString()));
                case "/_fake/v1/invariants" -> hooks.invariants().forEach(out.putArray("violations")::add);
                default -> {
                    reply(ex, 404, JSON.createObjectNode().put("error", "no such endpoint"));
                    return;
                }
            }
            reply(ex, 200, out);
        } catch (RuntimeException e) {
            reply(ex, 400, JSON.createObjectNode().put("error", e.getClass().getSimpleName() + ": " + e.getMessage()));
        } finally {
            serial.unlock();
        }
    }

    private static JsonNode body(HttpExchange ex) throws IOException {
        try (InputStream in = ex.getRequestBody()) {
            return JSON.readTree(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    private static void reply(HttpExchange ex, int status, JsonNode body) throws IOException {
        byte[] bytes = JSON.writeValueAsString(body).getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("content-type", "application/json");
        ex.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(bytes);
        }
    }
}
