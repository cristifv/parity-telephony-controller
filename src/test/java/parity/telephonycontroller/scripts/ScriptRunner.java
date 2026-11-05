package parity.telephonycontroller.scripts;

import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptors;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import parity.contracts.telephony.v1.TelephonyServiceGrpc;
import parity.contracts.telephony.v1.WatchRequest;
import parity.contracts.telephony.v1.WatchResponse;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * Plays one {@code telephony.v1} script (TC ADR 0001 §6) against a target and checks its outcome:
 * every call's answer, every delivered message, nothing left unmatched on any stream, and the node's
 * invariants (no leg originated twice; one fence on every live leg and the media).
 *
 * <p>Matching: bodies are parsed into protobuf messages (strict JSON, unknown fields refused) and
 * compared as messages. Session messages are matched in order per (stream, session); node-level ones
 * ({@code node_status}, {@code heartbeat}) only where a step expects one. A second Watch for a replica
 * must end the older stream with ABORTED / {@code stream_superseded}.
 */
final class ScriptRunner {

    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final Metadata.Key<String> PARITY_ERROR = Metadata.Key.of("parity-error", Metadata.ASCII_STRING_MARSHALLER);
    private static final long WAIT_MS = 3_000;

    private final ScriptTarget target;
    private final Map<String, StreamState> streams = new HashMap<>();
    private final Map<String, String[]> declared = new HashMap<>();
    private final Map<String, String> peers = new HashMap<>();
    private Long delta;

    ScriptRunner(ScriptTarget target) {
        this.target = target;
    }

    /** One Watch stream as the runner sees it. */
    private static final class StreamState {
        final String replicaId;
        final CompletableFuture<WatchResponse> first = new CompletableFuture<>();
        final CompletableFuture<Status> closed = new CompletableFuture<>();
        final LinkedBlockingDeque<WatchResponse> node = new LinkedBlockingDeque<>();
        final ConcurrentHashMap<String, LinkedBlockingDeque<WatchResponse>> sessions = new ConcurrentHashMap<>();
        ClientCall<WatchRequest, WatchResponse> call;

        StreamState(String replicaId) {
            this.replicaId = replicaId;
        }

        LinkedBlockingDeque<WatchResponse> session(String key) {
            return sessions.computeIfAbsent(key, k -> new LinkedBlockingDeque<>());
        }
    }

    void run(Path file) throws Exception {
        JsonNode script = JSON.readTree(Files.readString(file));
        try {
            script.path("streams").properties().forEach(e -> declared.put(e.getKey(),
                    new String[] {e.getValue().get("replica_id").asString(), e.getValue().get("bin").asString()}));
            script.path("peers").properties().forEach(e -> peers.put(e.getKey(), e.getValue().asString()));
            Map<Long, Long> epochs = target.reset(script.get("given"));
            epochs.forEach((s, r) -> delta = r - s);
            int i = 0;
            for (JsonNode step : script.get("steps")) {
                try {
                    step(step);
                } catch (AssertionError | RuntimeException e) {
                    String what = step.has("method") ? step.get("method").asString()
                            : step.has("what") ? step.get("what").asString() : "";
                    throw new AssertionError("step " + i + " (" + step.get("op").asString() + " " + what + "): "
                            + e.getMessage(), e);
                }
                i++;
            }
            Thread.sleep(200);
            for (Map.Entry<String, StreamState> e : streams.entrySet()) {
                e.getValue().sessions.forEach((key, q) -> {
                    if (!q.isEmpty()) {
                        throw new AssertionError("stream " + e.getKey() + " has an unmatched message for " + key + ": "
                                + print(q.peek()));
                    }
                });
            }
            List<String> violations = target.invariants();
            if (!violations.isEmpty()) {
                throw new AssertionError("invariants: " + violations);
            }
        } finally {
            streams.values().forEach(s -> {
                if (s.call != null) {
                    s.call.cancel("script over", null);
                }
            });
        }
    }

    private void step(JsonNode step) throws Exception {
        switch (step.get("op").asString()) {
            case "advance" -> target.advance(Long.parseLong(step.get("ms").asString()));
            case "inject" -> inject(step.get("what").asString(), mapEpochs(step.get("args")));
            case "call" -> call(step);
            case "deliver" -> deliver(step);
            default -> throw new IllegalArgumentException("unknown op");
        }
    }

    private void inject(String what, JsonNode args) throws Exception {
        ObjectNode a = (ObjectNode) args;
        switch (what) {
            case "stream_drop" -> {
                StreamState s = streams.get(a.get("stream").asString());
                a.put("replica_id", s.replicaId);
                target.inject(what, a);
                expectClosed(s, Status.Code.UNAVAILABLE, null);
            }
            case "owner_claim" -> {
                long scriptEpoch = Long.parseLong(args.get("owner_epoch").asString()) - (delta == null ? 0 : delta);
                long real = target.inject(what, unmapped(a, scriptEpoch));
                delta = real - scriptEpoch;
            }
            case "tc_restart", "tc_crash_mid_fence_raise" -> {
                List<StreamState> open = streams.values().stream().filter(s -> !s.closed.isDone()).toList();
                target.inject(what, a);
                for (StreamState s : open) {
                    expectClosed(s, Status.Code.UNAVAILABLE, null);
                }
            }
            default -> target.inject(what, a);
        }
    }

    /** owner_claim's args carry the script's epoch: the writer decides the real one. */
    private static JsonNode unmapped(ObjectNode a, long scriptEpoch) {
        ObjectNode copy = a.deepCopy();
        copy.put("owner_epoch", Long.toString(scriptEpoch));
        return copy;
    }

    private void call(JsonNode step) throws Exception {
        String method = step.get("method").asString();
        String label = step.get("stream").asString();
        MethodDescriptor<?, ?> md = TelephonyServiceGrpc.getServiceDescriptor().getMethods().stream()
                .filter(m -> m.getFullMethodName().equals(method)).findFirst()
                .orElseThrow(() -> new AssertionError("no method " + method));
        Message request = parse(prototype(md).newBuilderForType(), mapEpochs(step.get("body")));
        Metadata metadata = new Metadata();
        step.path("metadata").properties().forEach(e ->
                metadata.put(Metadata.Key.of(e.getKey(), Metadata.ASCII_STRING_MARSHALLER), e.getValue().asString()));
        String peer = step.has("peer") ? peers.get(step.get("peer").asString())
                : "parity-interaction-engine." + declared.get(label)[1];
        metadata.put(Metadata.Key.of("x-parity-test-peer", Metadata.ASCII_STRING_MARSHALLER), peer);
        JsonNode expect = step.get("expect");
        if (md.getType() == MethodDescriptor.MethodType.SERVER_STREAMING) {
            watch(label, (WatchRequest) request, metadata, expect);
            return;
        }
        Message answer;
        try {
            answer = unary(md, request, metadata);
        } catch (StatusRuntimeException e) {
            if (!expect.has("status")) {
                throw new AssertionError("expected " + expect.get("message").asString() + ", got " + e.getStatus().getCode()
                        + " / " + trailer(e));
            }
            checkStatus(expect, e.getStatus(), e.getTrailers());
            return;
        }
        if (expect.has("status")) {
            throw new AssertionError("expected " + expect.get("status").asString() + " / " + expect.get("parity_error").asString()
                    + ", got " + print(answer));
        }
        Message wanted = parse(answer.newBuilderForType(), mapEpochs(expect.get("body")));
        if (!wanted.equals(answer)) {
            throw new AssertionError("answer differs:\n  want " + print(wanted) + "\n  got  " + print(answer));
        }
    }

    @SuppressWarnings("unchecked")
    private Message unary(MethodDescriptor<?, ?> md, Message request, Metadata metadata) {
        MethodDescriptor<Message, Message> m = (MethodDescriptor<Message, Message>) md;
        Channel ch = ClientInterceptors.intercept(target.channel(), MetadataUtils.newAttachHeadersInterceptor(metadata));
        return ClientCalls.blockingUnaryCall(ch, m, CallOptions.DEFAULT.withDeadlineAfter(5, TimeUnit.SECONDS), request);
    }

    private void watch(String label, WatchRequest request, Metadata metadata, JsonNode expect) throws Exception {
        StreamState older = streams.values().stream()
                .filter(s -> s.replicaId.equals(request.getReplicaId()) && !s.closed.isDone()).findFirst().orElse(null);
        StreamState s = new StreamState(request.getReplicaId());
        ClientCall<WatchRequest, WatchResponse> call = ClientInterceptors
                .intercept(target.channel(), MetadataUtils.newAttachHeadersInterceptor(metadata))
                .newCall(TelephonyServiceGrpc.getWatchMethod(), CallOptions.DEFAULT);
        s.call = call;
        ClientCalls.asyncServerStreamingCall(call, request, new StreamObserver<>() {
            @Override
            public void onNext(WatchResponse m) {
                if (!s.first.isDone()) {
                    s.first.complete(m);
                } else if (m.hasSession()) {
                    s.session(m.getSession().getTenantId() + "/" + m.getSession().getInteractionId()).add(m);
                } else {
                    s.node.add(m);
                }
            }

            @Override
            public void onError(Throwable t) {
                Status st = Status.fromThrowable(t);
                Metadata trailers = Status.trailersFromThrowable(t);
                s.closed.complete(st.withDescription(trailers == null ? null : trailers.get(PARITY_ERROR)));
                s.first.completeExceptionally(t);
            }

            @Override
            public void onCompleted() {
                s.closed.complete(Status.OK);
            }
        });
        streams.put(label, s);
        if (expect.has("status")) {
            Status closed;
            try {
                closed = s.closed.get(WAIT_MS, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                throw new AssertionError("Watch: expected it to end " + expect.get("status").asString() + " / "
                        + expect.get("parity_error").asString() + ", but the stream stayed open", e);
            }
            if (!closed.getCode().name().equals(expect.get("status").asString())
                    || !expect.get("parity_error").asString().equals(closed.getDescription())) {
                throw new AssertionError("Watch: expected " + expect + ", got " + closed.getCode() + " / " + closed.getDescription());
            }
            return;
        }
        WatchResponse first;
        try {
            first = s.first.get(WAIT_MS, TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.ExecutionException e) {
            throw new AssertionError("Watch failed: " + Status.fromThrowable(e.getCause()), e);
        } catch (TimeoutException e) {
            throw new AssertionError("Watch: no first message within " + WAIT_MS + " ms", e);
        }
        WatchResponse wanted = (WatchResponse) parse(WatchResponse.newBuilder(), mapEpochs(expect.get("body")));
        if (!wanted.equals(first)) {
            throw new AssertionError("Watch's first message differs:\n  want " + print(wanted) + "\n  got  " + print(first));
        }
        if (older != null) {
            expectClosed(older, Status.Code.ABORTED, "stream_superseded");
        }
    }

    private void deliver(JsonNode step) throws Exception {
        StreamState s = streams.get(step.get("stream").asString());
        if (s == null) {
            throw new AssertionError("no stream " + step.get("stream").asString());
        }
        WatchResponse wanted = (WatchResponse) parse(WatchResponse.newBuilder(), mapEpochs(step.get("body")));
        if (!wanted.hasSession()) {
            long until = System.currentTimeMillis() + WAIT_MS;
            while (System.currentTimeMillis() < until) {
                WatchResponse m = s.node.poll(until - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
                if (m != null && m.equals(wanted)) {
                    return;
                }
            }
            throw new AssertionError("no node-level message " + print(wanted));
        }
        String key = wanted.getSession().getTenantId() + "/" + wanted.getSession().getInteractionId();
        WatchResponse got = s.session(key).poll(WAIT_MS, TimeUnit.MILLISECONDS);
        if (got == null) {
            throw new AssertionError("nothing delivered; wanted " + print(wanted));
        }
        if (!wanted.equals(got)) {
            throw new AssertionError("delivered message differs:\n  want " + print(wanted) + "\n  got  " + print(got));
        }
    }

    private static void expectClosed(StreamState s, Status.Code code, String error) throws Exception {
        Status st;
        try {
            st = s.closed.get(WAIT_MS, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new AssertionError("stream of " + s.replicaId + " should have ended with " + code, e);
        }
        if (st.getCode() != code || (error != null && !error.equals(st.getDescription()))) {
            throw new AssertionError("stream of " + s.replicaId + " ended " + st.getCode() + " / " + st.getDescription()
                    + ", expected " + code + " / " + error);
        }
    }

    private static void checkStatus(JsonNode expect, Status status, Metadata trailers) {
        String code = trailers == null ? null : trailers.get(PARITY_ERROR);
        if (!status.getCode().name().equals(expect.get("status").asString())
                || !expect.get("parity_error").asString().equals(code)) {
            throw new AssertionError("expected " + expect.get("status").asString() + " / " + expect.get("parity_error").asString()
                    + ", got " + status.getCode() + " / " + code);
        }
    }

    private static String trailer(StatusRuntimeException e) {
        return e.getTrailers() == null ? null : e.getTrailers().get(PARITY_ERROR);
    }

    private static Message prototype(MethodDescriptor<?, ?> md) {
        return (Message) ((MethodDescriptor.PrototypeMarshaller<?>) md.getRequestMarshaller()).getMessagePrototype();
    }

    private static Message parse(Message.Builder b, JsonNode body) throws Exception {
        JsonFormat.parser().merge(body.toString(), b);
        return b.build();
    }

    /** Every {@code owner_epoch} in a body, mapped onto the target's real create revisions. */
    private JsonNode mapEpochs(JsonNode body) {
        JsonNode copy = body.deepCopy();
        List<ObjectNode> objects = new ArrayList<>();
        collect(copy, objects);
        for (ObjectNode o : objects) {
            JsonNode e = o.get("owner_epoch");
            if (e != null && delta != null) {
                long v = Long.parseLong(e.asString());
                o.put("owner_epoch", Long.toString(v == 0 ? 0 : v + delta));
            }
        }
        return copy;
    }

    private static void collect(JsonNode n, List<ObjectNode> out) {
        if (n instanceof ObjectNode o) {
            out.add(o);
            o.properties().forEach(e -> collect(e.getValue(), out));
        } else if (n.isArray()) {
            n.forEach(c -> collect(c, out));
        }
    }

    private static String print(Message m) {
        try {
            return JsonFormat.printer().omittingInsignificantWhitespace().print(m);
        } catch (Exception e) {
            return m.getClass().getSimpleName();
        }
    }
}
