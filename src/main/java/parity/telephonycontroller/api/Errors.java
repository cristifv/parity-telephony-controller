package parity.telephonycontroller.api;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import parity.telephonycontroller.session.Code;

/** A code's canonical status and its {@code parity-error} trailer (TC ADR 0001 §4.1). */
final class Errors {

    static final Metadata.Key<String> PARITY_ERROR = Metadata.Key.of("parity-error", Metadata.ASCII_STRING_MARSHALLER);

    private Errors() {}

    static StatusRuntimeException status(Code code) {
        Metadata trailers = new Metadata();
        trailers.put(PARITY_ERROR, code.trailer());
        return grpc(code).asRuntimeException(trailers);
    }

    static Status grpc(Code code) {
        return switch (code) {
            case STALE_OWNER, STREAM_SUPERSEDED -> Status.ABORTED;
            case OWNER_UNVERIFIED, IDENTITY_MISMATCH -> Status.PERMISSION_DENIED;
            case TENANT_MISMATCH, INVALID_REQUEST -> Status.INVALID_ARGUMENT;
            case SESSION_UNKNOWN, LEG_UNKNOWN -> Status.NOT_FOUND;
            case LEG_CONFLICT, COMMAND_CONFLICT -> Status.ALREADY_EXISTS;
            case WRONG_NODE, INVALID_STATE, SESSION_ENDED, ARRIVAL_CLOSED, PROMPT_UNKNOWN -> Status.FAILED_PRECONDITION;
            case NO_CAPACITY -> Status.RESOURCE_EXHAUSTED;
            case OWNER_CHECK_UNAVAILABLE, DRAINING, REBUILDING, FREESWITCH_UNAVAILABLE, PARITY_MS_UNAVAILABLE,
                    SESSION_BUSY -> Status.UNAVAILABLE;
            case ATTEMPT_EXPIRED -> Status.DEADLINE_EXCEEDED;
            case INTERNAL -> Status.INTERNAL;
        };
    }
}
