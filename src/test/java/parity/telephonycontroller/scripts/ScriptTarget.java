package parity.telephonycontroller.scripts;

import io.grpc.ManagedChannel;
import java.util.List;
import java.util.Map;
import tools.jackson.databind.JsonNode;

/** Where a script runs: fake-telephony-controller, or TC's own server. */
interface ScriptTarget extends AutoCloseable {

    /** A fresh node from {@code given}; script epoch → real create revision of the given owners. */
    Map<Long, Long> reset(JsonNode given) throws Exception;

    /** One inject; for {@code owner_claim}, the record's real create revision. */
    long inject(String what, JsonNode args) throws Exception;

    void advance(long ms) throws Exception;

    List<String> invariants() throws Exception;

    ManagedChannel channel();

    String name();

    @Override
    void close();
}
