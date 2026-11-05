package parity.telephonycontroller.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * {@code parity.telephony-controller.*}. A value that fails validation, or a key this record
 * doesn't know (a misspelling), stops the start with exit code 2.
 *
 * @param nodeId the media node this controller runs on
 * @param zone the zone (cell)
 * @param instance this process, for logs and metrics
 * @param version the release, for logs and metrics
 * @param lockDir the host directory holding the node lock (bind-mounted into the container)
 * @param lifecycle the healthy and drained files, and the drain's timings
 * @param readiness what must answer before the controller is healthy
 * @param bins the zone's deployment bins: only their IE replicas are served
 * @param grpc the {@code telephony.v1} port
 * @param etcd where IE's owner records are read (TC ADR 0001 R1-14)
 * @param journal the hangup journal's directory (a host volume)
 * @param fake {@code fake-telephony-controller}'s control endpoint (the {@code fake} profile only)
 * @param node the media node: freeswitch over ESL and parity-ms, or simulated (TC ADR 0002)
 */
@Validated
@ConfigurationProperties(prefix = "parity.telephony-controller", ignoreUnknownFields = false)
public record ControllerProperties(
        @NotBlank @Pattern(regexp = "^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$") String nodeId,
        @NotBlank String zone,
        @NotBlank String instance,
        @NotBlank String version,
        @NotBlank String lockDir,
        @NotNull @Valid Lifecycle lifecycle,
        @NotNull @Valid Readiness readiness,
        @NotNull @Size(min = 1) List<@Pattern(regexp = "^[a-z0-9][a-z0-9-]{0,62}$") String> bins,
        @NotNull @Valid Grpc grpc,
        @NotNull @Valid Etcd etcd,
        @NotNull @Valid JournalDir journal,
        @NotNull @Valid Fake fake,
        @NotNull @Valid Node node) {

    /**
     * The gRPC server.
     *
     * @param port the {@code telephony.v1} port (0: any free port, for tests)
     * @param testPeerHeader test only: the caller's identity from {@code x-parity-test-peer}, until
     *     mTLS (step 8); refused at start unless the {@code test} or {@code fake} profile is active
     */
    public record Grpc(@Min(0) @Max(65535) int port, boolean testPeerHeader) {}

    /**
     * etcd, read-only.
     *
     * @param endpoint etcd's client URL (the {@code /v3} JSON gateway)
     * @param timeout one read's budget; past it, {@code owner_check_unavailable}
     */
    public record Etcd(@NotNull URI endpoint, @NotNull Duration timeout) {}

    /**
     * The hangup journal.
     *
     * @param dir the journal's directory
     */
    public record JournalDir(@NotBlank String dir) {}

    /**
     * fake-telephony-controller's control endpoint.
     *
     * @param controlHost the control endpoint's address
     * @param controlPort its port (0: any free port, for tests)
     */
    public record Fake(@NotBlank String controlHost, @Min(0) @Max(65535) int controlPort) {}

    /**
     * The lifecycle files and the drain (ARCHITECTURE.md §7.3).
     *
     * @param healthyFile created once the readiness checks first pass; removed when draining
     * @param drainedFile created once the drain finished in time
     * @param deregistrationDelay waited after the healthy file is removed (ARCH §7.3 step 2)
     * @param drainTimeout the whole drain, the delay included; past it, no drained file
     * @param drainHold test only: holds the drain this long, to prove the timeout path; refused
     *     at start unless the {@code test} profile is active ({@code DrainHold})
     */
    public record Lifecycle(
            @NotBlank String healthyFile,
            @NotBlank String drainedFile,
            @NotNull Duration deregistrationDelay,
            @NotNull Duration drainTimeout,
            Duration drainHold) {

        public Duration drainHoldOrZero() {
            return drainHold == null ? Duration.ZERO : drainHold;
        }
    }

    /**
     * The readiness checks.
     *
     * @param pollInterval how often the checks run
     * @param connectTimeout a TCP check's connect timeout
     * @param endpoints name → {@code host:port} that must accept a TCP connection; an empty
     *     value turns that endpoint's check off (a map entry can't be removed by a higher
     *     property source)
     */
    public record Readiness(
            @NotNull Duration pollInterval,
            @NotNull Duration connectTimeout,
            Map<String, @Pattern(regexp = "^$|^[^:\\s]+:\\d{1,5}$") String> endpoints) {

        public Map<String, String> endpointsOrEmpty() {
            return endpoints == null ? Map.of() : endpoints;
        }
    }

    /**
     * The media node (TC ADR 0002).
     *
     * @param mode {@code real} (freeswitch over ESL) or {@code simulated} (the fake, tests)
     * @param esl freeswitch's event socket, on loopback
     * @param maxLegs TC's own limit on live legs
     * @param answerTimeout how long an answer waits for freeswitch's CHANNEL_ANSWER
     * @param claimedBackstop a claimed arrival's backstop hangup until its answer or reject (R1-4)
     * @param agentProxy where agent legs are sent (opensips; {@code fs_path}); empty: directly
     * @param agentRegistrarDomain agents that registered with freeswitch itself (test setups): a
     *     domain ({@code acme.agents.localhost}, one tenant), or a per-tenant template
     *     {@code {tenant}.<domain>} ({@code *.<domain>} too) whose first label is the leg's tenant id:
     *     a contact outside the tenant's own domain is then refused (TC-056). Empty in production
     * @param trunks trunk id → {@code host:port} for PSTN legs
     * @param cdrDir freeswitch's {@code mod_json_cdr} directory, read-only
     * @param canaryLow the first of freeswitch's RTP ports (the canary, §7)
     * @param canaryHigh the last
     * @param testHeaders test only: X-Test-Leg-ID on originated legs; refused outside the test profile
     * @param media {@code parity-ms} or {@code simulated}
     * @param parityMs parity-ms's control channel
     * @param recordingSink {@code none} (StartRecording refused), or {@code null}: recordings run
     *     into parity-ms's null sink, which writes nothing (MS-012), until R9's encrypted sink. Local
     *     test setups only: refused at start outside the {@code local}, {@code test} and {@code fake}
     *     profiles, and with {@code prod} or {@code staging} active
     */
    public record Node(
            @NotBlank @Pattern(regexp = "^(real|simulated)$") String mode,
            @NotNull @Valid Esl esl,
            @Min(0) int maxLegs,
            @NotNull Duration answerTimeout,
            @NotNull Duration claimedBackstop,
            String agentProxy,
            String agentRegistrarDomain,
            Map<String, @Pattern(regexp = "^[A-Za-z0-9.-]{1,253}(:[0-9]{1,5})?$") String> trunks,
            @NotBlank String cdrDir,
            @Min(1) @Max(65535) int canaryLow,
            @Min(1) @Max(65535) int canaryHigh,
            boolean testHeaders,
            @NotBlank @Pattern(regexp = "^(parity-ms|simulated)$") String media,
            @NotNull @Valid ParityMs parityMs,
            @NotBlank @Pattern(regexp = "^(none|null)$") String recordingSink) {

        /** The null recording sink is configured. */
        public boolean nullSink() {
            return "null".equals(recordingSink);
        }

        public Map<String, String> trunksOrEmpty() {
            return trunks == null ? Map.of() : trunks;
        }
    }

    /**
     * freeswitch's event socket.
     *
     * @param host its address (loopback)
     * @param port its port
     * @param passwordFile the secret's file (mounted only into TC and freeswitch); never logged
     * @param startupTimeout how long the start waits for ESL before giving up (exit 1)
     */
    public record Esl(@NotBlank String host, @Min(1) @Max(65535) int port, @NotBlank String passwordFile,
            @NotNull Duration startupTimeout) {}

    /**
     * parity-ms's control channel (newline JSON over TCP, loopback).
     *
     * @param host its address
     * @param port its port
     * @param timeout one request's budget in all (the pool's slot, the connect, the answer); parity-ms's
     *     prompt wait must be shorter, and IE's command deadline longer (MS-042)
     * @param connections the pool's size
     * @param sweepGrace how long after the node's last full sync (a start or an ESL reconnect) the
     *     orphan sweep waits (TC ADR 0003, must-fix 1; minutes, not seconds)
     */
    public record ParityMs(@NotBlank String host, @Min(1) @Max(65535) int port, @NotNull Duration timeout,
            @Min(1) int connections, @NotNull Duration sweepGrace) {}
}
