package parity.telephonycontroller.api;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import parity.telephonycontroller.session.Requests.Caller;

/**
 * Who calls, and for which tenant. The identity is the bin of a {@code parity-interaction-engine.<bin>}
 * certificate; until mTLS (step 8, TC ADR 0001 §6) it is read from the test-only metadata
 * {@code x-parity-test-peer}, and only when the {@code test} or {@code fake} profile turned that on.
 * Without either, the caller has no bin and every call is refused ({@code identity_mismatch}).
 */
final class CallerInterceptor implements ServerInterceptor {

    static final Context.Key<Caller> CALLER = Context.key("parity-caller");
    static final Context.Key<String> TENANT = Context.key("parity-tenant");
    /** {@code parity-attempt-expires-ms} (TC ADR 0002 §2.5), as sent; parsed by the service. */
    static final Context.Key<String> ATTEMPT_EXPIRES = Context.key("parity-attempt-expires");
    private static final Metadata.Key<String> ATTEMPT_EXPIRES_MS =
            Metadata.Key.of("parity-attempt-expires-ms", Metadata.ASCII_STRING_MARSHALLER);
    static final String IDENTITY_PREFIX = "parity-interaction-engine.";
    private static final Metadata.Key<String> TENANT_ID = Metadata.Key.of("tenant-id", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> TEST_PEER =
            Metadata.Key.of("x-parity-test-peer", Metadata.ASCII_STRING_MARSHALLER);

    private final boolean testPeerHeader;

    CallerInterceptor(boolean testPeerHeader) {
        this.testPeerHeader = testPeerHeader;
    }

    @Override
    public <Q, R> ServerCall.Listener<Q> interceptCall(ServerCall<Q, R> call, Metadata headers,
            ServerCallHandler<Q, R> next) {
        String identity = testPeerHeader ? headers.get(TEST_PEER) : null;
        String bin = identity != null && identity.startsWith(IDENTITY_PREFIX)
                ? identity.substring(IDENTITY_PREFIX.length()) : null;
        Context ctx = Context.current().withValue(CALLER, new Caller(bin)).withValue(TENANT, headers.get(TENANT_ID))
                .withValue(ATTEMPT_EXPIRES, headers.get(ATTEMPT_EXPIRES_MS));
        return Contexts.interceptCall(ctx, call, headers, next);
    }
}
