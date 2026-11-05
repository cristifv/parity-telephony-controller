package parity.telephonycontroller.config;

import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.List;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import parity.telephonycontroller.fake.FakeControlServer;
import parity.telephonycontroller.fake.ScriptHooks;
import parity.telephonycontroller.journal.FileJournal;
import parity.telephonycontroller.journal.InMemoryJournal;
import parity.telephonycontroller.journal.Journal;
import parity.telephonycontroller.lifecycle.ConfigurationRefusedException;
import parity.telephonycontroller.node.NodePort;
import parity.telephonycontroller.node.SimulatedNode;
import parity.telephonycontroller.owner.EtcdOwnerDirectory;
import parity.telephonycontroller.owner.InMemoryOwnerDirectory;
import parity.telephonycontroller.owner.OwnerDirectory;
import parity.telephonycontroller.session.EngineHolder;
import parity.telephonycontroller.session.TelephonyNode;
import parity.telephonycontroller.time.ManualClock;
import parity.telephonycontroller.time.NodeClock;
import parity.telephonycontroller.time.SystemNodeClock;

/**
 * The {@code telephony.v1} server and what it runs on. The default wiring is production's (system
 * clock, etcd's gateway, the file journal); the {@code fake} profile is
 * {@code fake-telephony-controller} (a clock that moves only when told, in-memory owners and
 * journal, and the control endpoint). The node is freeswitch over ESL with parity-ms
 * ({@code node.mode: real}, TC ADR 0002), or the simulated one (the fake, tests).
 */
@Configuration(proxyBeanMethods = false)
public class TelephonyWiring {

    /** parity-ms legs the simulated node offers (the capacity floor is 2,000 calls, ~4,400 legs). */
    static final int SIMULATED_FREE_LEGS = 5_000;

    @Bean
    @Profile("!fake")
    SystemNodeClock systemClock() {
        return new SystemNodeClock();
    }

    @Bean
    @Profile("fake")
    ManualClock manualClock() {
        return new ManualClock(System.currentTimeMillis());
    }

    @Bean
    @Profile("!fake")
    EtcdOwnerDirectory etcdOwners(ControllerProperties p) {
        return new EtcdOwnerDirectory(p.etcd().endpoint(), p.etcd().timeout());
    }

    @Bean
    @Profile("fake")
    InMemoryOwnerDirectory memoryOwners() {
        return new InMemoryOwnerDirectory();
    }

    @Bean
    @Profile("!fake")
    FileJournal fileJournal(ControllerProperties p) {
        return new FileJournal(Path.of(p.journal().dir()));
    }

    @Bean
    @Profile("fake")
    InMemoryJournal memoryJournal() {
        return new InMemoryJournal();
    }

    /**
     * The media node. Real: ESL up and its first snapshot in before the engine starts (TC ADR 0002
     * §2.4), or the start fails (exit 1) after {@code esl.startup-timeout}: TC can't serve a node it
     * can't see.
     */
    @Bean(destroyMethod = "")
    NodePort node(ControllerProperties p, NodeClock clock, Environment env) throws Exception {
        ControllerProperties.Node n = p.node();
        checkRecordingSink(n.nullSink(), env);
        if (n.mode().equals("simulated")) {
            SimulatedNode sim = new SimulatedNode(clock, SIMULATED_FREE_LEGS);
            sim.setRecordingNullSink(n.nullSink());
            return sim;
        }
        if (n.testHeaders() && !env.matchesProfiles("test")) {
            throw new ConfigurationRefusedException(
                    "parity.telephony-controller.node.test-headers is test only: set it only with the 'test' profile");
        }
        String password;
        try {
            password = java.nio.file.Files.readString(Path.of(n.esl().passwordFile())).trim();
        } catch (java.io.IOException e) {
            throw new ConfigurationRefusedException("the ESL password file can't be read");
        }
        parity.telephonycontroller.esl.EslClient esl = new parity.telephonycontroller.esl.EslClient(
                parity.telephonycontroller.esl.EslClient.Config.defaults(n.esl().host(), n.esl().port(), password));
        parity.telephonycontroller.node.ParityMsClient parityMs = n.media().equals("simulated") ? null
                : new parity.telephonycontroller.node.ParityMsClient(new parity.telephonycontroller.node.ParityMsClient.Config(
                        n.parityMs().host(), n.parityMs().port(), n.parityMs().timeout(), n.parityMs().connections(),
                        n.parityMs().sweepGrace(), n.nullSink()));
        parity.telephonycontroller.node.SimulatedMedia simMedia = parityMs != null ? null
                : new parity.telephonycontroller.node.SimulatedMedia();
        if (simMedia != null) {
            simMedia.setNullSink(n.nullSink());
        }
        parity.telephonycontroller.node.MediaPort media = parityMs != null ? parityMs : simMedia;
        // Before the node's snapshot and the engine's rebuild: which sessions still have media.
        media.start();
        if (parityMs != null) {
            checkPromptWait(parityMs.promptWaitMs(), n.parityMs().timeout());
        }
        parity.telephonycontroller.node.RealNode real = new parity.telephonycontroller.node.RealNode(
                new parity.telephonycontroller.node.RealNode.Config(n.maxLegs(), n.answerTimeout(),
                        (int) n.claimedBackstop().toSeconds(), blankToNull(n.agentProxy()),
                        agentRegistrar(n.agentRegistrarDomain()), n.trunksOrEmpty(), Path.of(n.cdrDir()), n.canaryLow(),
                        n.canaryHigh(), n.testHeaders()),
                esl, media, clock);
        if (!real.start(n.esl().startupTimeout())) {
            real.close();
            throw new IllegalStateException("freeswitch's ESL didn't come up in time");
        }
        if (parityMs != null) {
            // parity-ms restarted: every session's media is gone, and the engine hears it.
            parityMs.onLost(real::mediaLost);
        }
        return real;
    }

    /**
     * The null recording sink records nothing: a call "recorded" after a consent announcement would be
     * a compliance bug anywhere but a local test setup (MS-012; R9 brings the real sink). Refused
     * outside the {@code local}, {@code test} and {@code fake} profiles, and with any production or
     * staging profile active.
     */
    static void checkRecordingSink(boolean nullSink, Environment env) {
        if (!nullSink) {
            return;
        }
        if (!env.matchesProfiles("local | test | fake") || env.matchesProfiles("prod | production | staging | stage")) {
            throw new ConfigurationRefusedException(
                    "parity.telephony-controller.node.recording-sink=null is for local test setups only: set it only with the 'local', 'test' or 'fake' profile, never with 'prod' or 'staging'");
        }
        org.slf4j.LoggerFactory.getLogger(TelephonyWiring.class).atWarn().addKeyValue("event", "recording.null_sink_enabled")
                .addKeyValue("sink", "null")
                .log("recording sink null: StartRecording records nothing (local test setups only, until R9)");
    }

    /**
     * MS-042: parity-ms must give up on a prompt load before TC gives up on parity-ms, or a play TC
     * reported failed (and IE retried) could still start, a loop nobody tracks. Refused at start when
     * parity-ms's {@code hello.prompt_wait_ms} isn't shorter than {@code node.parity-ms.timeout}; a
     * parity-ms that doesn't say (an older one, or down at start) is let through, and a later
     * {@code hello} that says so makes TC refuse plays ({@code ParityMsClient.promptWaitTooLong}).
     * The order to keep: parity-ms's wait (200 ms) &lt; TC's timeout (250 ms) &lt; IE's command
     * deadline (300 ms).
     */
    static void checkPromptWait(long promptWaitMs, java.time.Duration timeout) {
        if (promptWaitMs >= 0 && promptWaitMs >= timeout.toMillis()) {
            org.slf4j.LoggerFactory.getLogger(TelephonyWiring.class).atError().addKeyValue("event", "media.prompt_wait_refused")
                    .addKeyValue("prompt_wait_ms", promptWaitMs)
                    .addKeyValue("timeout_ms", timeout.toMillis()).log("parity-ms's prompt wait isn't shorter than TC's timeout");
            throw new ConfigurationRefusedException(
                    "parity-ms's prompt wait (hello.prompt_wait_ms, --prompt-wait-ms) must be shorter than parity.telephony-controller.node.parity-ms.timeout (MS-042)");
        }
    }

    /** {@code node.agent-registrar-domain} (TC-056): empty, a domain, or {@code {tenant}.<domain>}; else refused at start. */
    static parity.telephonycontroller.node.AgentRegistrar agentRegistrar(String setting) {
        try {
            return parity.telephonycontroller.node.AgentRegistrar.parse(setting);
        } catch (IllegalArgumentException e) {
            throw new ConfigurationRefusedException(
                    "node.agent-registrar-domain is neither a domain nor a template {tenant}.<lowercase domain>");
        }
    }

    private static String blankToNull(String s) {
        return s == null || s.isBlank() ? null : s;
    }

    @Bean
    EngineHolder engineHolder(ControllerProperties p, NodeClock clock, OwnerDirectory owners, Journal journal,
            NodePort node, Environment env) {
        if (p.grpc().testPeerHeader() && !env.matchesProfiles("test | fake")) {
            throw new ConfigurationRefusedException(
                    "parity.telephony-controller.grpc.test-peer-header is test only: set it only with the 'test' or 'fake' profile");
        }
        // A UUIDv7 after the last run's, whatever the clock did (TC ADR 0002 §2.6): the logs' own
        // (log-record.v1 incarnation) whenever it sorts after it, so both name this run alike.
        String incarnation = parity.telephonycontroller.obs.LogProcess.engineIncarnation(journal.lastIncarnation().orElse(null));
        journal.incarnation(incarnation);
        org.slf4j.LoggerFactory.getLogger(TelephonyWiring.class).atInfo().addKeyValue("event", "engine.started")
                .addKeyValue("engine_incarnation", incarnation).addKeyValue("bins", p.bins())
                .log("the telephony engine started");
        TelephonyNode.Config config = new TelephonyNode.Config(p.nodeId(), p.version(),
                new LinkedHashSet<>(p.bins()), incarnation, List.of());
        return new EngineHolder(config, clock, owners, journal, node);
    }

    /** Delivery under §4.4's limits: events dropped (a counter that should stay 0) and bytes held. */
    @Bean
    io.micrometer.core.instrument.binder.MeterBinder telephonyMetrics(EngineHolder holder) {
        return registry -> {
            io.micrometer.core.instrument.FunctionCounter.builder("telephony_controller_events_dropped",
                            TelephonyNode.class, c -> TelephonyNode.droppedEvents())
                    .description("Unacknowledged events dropped by the delivery limits (IE resyncs from the gap)")
                    .register(registry);
            io.micrometer.core.instrument.Gauge.builder("telephony_controller_buffered_bytes", holder,
                            h -> h.get() == null ? 0 : h.get().bufferedBytes())
                    .description("Unacknowledged events' estimated size")
                    .register(registry);
            io.micrometer.core.instrument.FunctionCounter.builder("telephony_controller_attempts_expired",
                            TelephonyNode.class, c -> TelephonyNode.expiredAttempts())
                    .description("Commands refused because their attempt had expired (TC ADR 0002 §2.5)")
                    .register(registry);
            io.micrometer.core.instrument.FunctionCounter.builder("telephony_controller_digits_off_failed",
                            TelephonyNode.class, c -> TelephonyNode.digitsOffFailed())
                    .description("Fence raises that couldn't turn a leg's digit reporting off in parity-ms (PCI)")
                    .register(registry);
            io.micrometer.core.instrument.FunctionCounter.builder("telephony_controller_mailbox_overflows",
                            TelephonyNode.class, c -> TelephonyNode.mailboxOverflows())
                    .description("Node facts dropped by a full session mailbox (the session resynced)")
                    .register(registry);
            io.micrometer.core.instrument.Gauge.builder("telephony_controller_sessions", holder,
                            h -> h.get() == null ? 0 : h.get().sessionCount())
                    .description("Sessions and tombstones held (TC-036: ownerless tombstones are short-lived and capped)")
                    .register(registry);
            io.micrometer.core.instrument.FunctionCounter.builder("telephony_controller_ownerless_tombstones_evicted",
                            TelephonyNode.class, c -> TelephonyNode.ownerlessEvicted())
                    .description("Ownerless tombstones released early by the node's cap (TC-036: a flood of unclaimed arrivals)")
                    .register(registry);
            io.micrometer.core.instrument.FunctionCounter.builder("telephony_controller_arrivals_duplicate",
                            TelephonyNode.class, c -> TelephonyNode.duplicateArrivals())
                    .description("Arrivals refused (503): their interaction already had a session")
                    .register(registry);
            io.micrometer.core.instrument.Gauge.builder("telephony_controller_clock_offset_ms",
                            parity.telephonycontroller.api.TelephonyGrpcService.class,
                            c -> parity.telephonycontroller.api.TelephonyGrpcService.clockOffsetMs())
                    .description("IE's clock minus TC's, from the attempt expiry header (alerted above 100)")
                    .register(registry);
            // Recording (the null sink until R9): sink="null" says nothing is written.
            String sink = holder.node().recordingNullSink() ? "null" : "none";
            io.micrometer.core.instrument.FunctionCounter.builder("telephony_controller_recordings_started",
                            TelephonyNode.class, c -> TelephonyNode.recordingsStarted())
                    .tag("sink", sink).description("Recordings started (sink=null: nothing written, local test setups only)")
                    .register(registry);
            io.micrometer.core.instrument.FunctionCounter.builder("telephony_controller_recordings_stopped",
                            TelephonyNode.class, c -> TelephonyNode.recordingsStopped())
                    .tag("sink", sink).description("Recordings finished (stopped or their leg ended)")
                    .register(registry);
            for (boolean mismatch : new boolean[] {true, false}) {
                io.micrometer.core.instrument.FunctionCounter.builder("telephony_controller_agent_legs_refused",
                                TelephonyNode.class, c -> TelephonyNode.agentLegsRefused(mismatch))
                        .tag("reason", mismatch ? "tenant_mismatch" : "invalid_target")
                        .description("AddLegs refused before dialling: an agent contact not the session tenant's (TC-056)")
                        .register(registry);
            }
            // The play saga (ADR 0004): compensations (stop_play for a play TC no longer wants, or a
            // late start), retries of unanswered sends, plays that ended without starting.
            for (String reason : TelephonyNode.COMPENSATIONS) {
                io.micrometer.core.instrument.FunctionCounter.builder("telephony_controller_plays_compensated",
                                TelephonyNode.class, c -> TelephonyNode.playsCompensated(reason))
                        .tag("reason", reason)
                        .description("stop_play sent for a play TC no longer wants, or one that started after TC gave it up")
                        .register(registry);
            }
            for (String reason : TelephonyNode.PLAY_FAILURES) {
                io.micrometer.core.instrument.FunctionCounter.builder("telephony_controller_plays_failed",
                                TelephonyNode.class, c -> TelephonyNode.playsFailed(reason))
                        .tag("reason", reason.toLowerCase(java.util.Locale.ROOT))
                        .description("Plays (hold music included) that ended without starting")
                        .register(registry);
            }
            io.micrometer.core.instrument.FunctionCounter.builder("telephony_controller_plays_retried",
                            TelephonyNode.class, c -> TelephonyNode.playsRetried())
                    .description("Play sends repeated (the same id) after parity-ms's answer was lost")
                    .register(registry);
            io.micrometer.core.instrument.FunctionCounter.builder("telephony_controller_recordings_refused",
                            TelephonyNode.class, c -> TelephonyNode.recordingsRefused())
                    .tag("sink", sink).description("StartRecording refused: no recording sink for the request on this node")
                    .register(registry);
            if (holder.node() instanceof parity.telephonycontroller.node.RealNode real) {
                real.metrics(registry);
            }
        };
    }

    @Bean
    TelephonyServerLifecycle telephonyServer(ControllerProperties p, EngineHolder holder) {
        return new TelephonyServerLifecycle(p.grpc().port(), holder, p.grpc().testPeerHeader());
    }

    @Bean
    @Profile("fake")
    FakeControlLifecycle fakeControl(ControllerProperties p, EngineHolder holder, ManualClock clock,
            InMemoryOwnerDirectory owners, InMemoryJournal journal) {
        ScriptHooks hooks = new ScriptHooks(holder, clock, owners, owners, journal::clear);
        return new FakeControlLifecycle(() -> new FakeControlServer(p.fake().controlHost(), p.fake().controlPort(), hooks));
    }

    /** The control endpoint's lifecycle. */
    public static final class FakeControlLifecycle implements org.springframework.context.SmartLifecycle {
        private final java.util.function.Supplier<FakeControlServer> factory;
        private volatile FakeControlServer server;

        FakeControlLifecycle(java.util.function.Supplier<FakeControlServer> factory) {
            this.factory = factory;
        }

        @Override
        public void start() {
            server = factory.get().start();
        }

        @Override
        public void stop() {
            FakeControlServer s = server;
            if (s != null) {
                s.close();
            }
            server = null;
        }

        @Override
        public boolean isRunning() {
            return server != null;
        }

        /** The bound port. */
        public int port() {
            return server.port();
        }
    }
}
