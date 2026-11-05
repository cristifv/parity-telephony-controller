# parity-telephony-controller

**Step 3 built (2026-10-27; ADR 0002, revision 1):** TC drives **real freeswitch over ESL** (`esl/`,
`node/RealNode`: arrivals, park, tokens and hangup hooks, the loopback carrier for agent legs, (c+)
re-INVITEs, the safety hangup and backstop, the rebuild) and **parity-ms over its TCP API**
(`node/ParityMsTcp`, its two-party API until step 4), with per-session mailboxes, Error Prone and a
pinned-thread check. The 62 `telephony.v1` scripts (parity-contracts 0.2.4) pass on the fake, the
server and real freeswitch. **Step 4 (ADR 0003):** parity-ms's own API (`ParityMsClient`: the
half-open call, its events, the sweep) and the media commands (Play, SetMute, SendDtmf,
SetDigitReporting, SetHold, SetDtmfActions; recording waits for parity-ms's encryption). Steps 0
and 5 are accepted; steps 3 and 4 are reviewed together.

The telephony controller of the contact-center platform: the one service, per media node,
that drives freeswitch (SIP signalling, over ESL) and parity-ms (all media, over its TCP API)
for each call, on behalf of parity-interaction-engine. It is the `ie-telephony` module of
interaction-engine's PLAN, extracted into a service of its own by the user's decision of
2026-10-25 ("separate service now"), in Java 21 + Spring Boot 4.1.1 (the user, 2026-10-25).

| Path | What |
|---|---|
| `PLAN.md` | The implementation plan: assessment, design, questions and answers, steps, risks, doc changes |
| `docs/parity-ms-api-proposal.md` | What the controller needs added to parity-ms's API, for the user's approval |
| `PROGRESS.md` | Each step's results and evidence |
| `docs/adr/0001-telephony-contract.md` | The IE ↔ TC contract (`parity.telephony.v1`), revision 1, and step 5's implementation notes |
| `docs/adr/0002-esl-side.md` | Step 3: the ESL side, revision 1 (R1-1..R1-14) |
| `freeswitch/conf/`, `compose/freeswitch-test/` | freeswitch's configuration (TC owns it), and the tests' overlay (test password, test headers, short desk-phone timer) |
| `tools/` | Our Error Prone checks (`NoDataInMessages`, `NoSynchronized`) and the pinned-thread probe `ci.sh` must catch |
| `pom.xml`, `src/`, `Dockerfile` | The service (Java 21, Spring Boot 4.1.1) |
| `scripts/ci.sh` | The local CI: `mvn verify` (lint, unit and integration tests with real processes and containers, licences), the log check (`scripts/check-logs.sh`) and an OSV advisory scan |
| `logging/events.parity-telephony-controller.v1.json` | TC's log event catalogue (parity-contracts `log-record.v1`, CONVENTIONS "Logging"); see "Logs" below |
| `evidence/step0/`, `evidence/step5/`, `evidence/step5-design/`, `evidence/step3/` | CI logs, mutation runs, proofs, and step 3's real-node results |
| `experiments/` | Steps 1 and 2: the spike's compose, freeswitch configuration, scenarios and throwaway driver, copied from parity-interaction-engine; thrown away after review |

The contract with interaction-engine lives in `../parity-contracts/proto/parity/telephony/v1/`;
its golden scripts in `../parity-contracts/fixtures/telephony.v1/` run against both the fake and
this server in `mvn verify`.

## Running

```
scripts/ci.sh                    # the whole check (needs docker, Java 21, Maven >= 3.9)
CI_OFFLINE=1 scripts/ci.sh       # offline: no SBOM, no advisory scan (said so)
mvn -B -ntp -DskipTests package && java -jar target/parity-telephony-controller-0.1.0.jar \
  --parity.telephony-controller.lock-dir=/tmp/tc-lock \
  --parity.telephony-controller.journal.dir=/tmp/tc-journal \
  --parity.telephony-controller.node.mode=simulated --parity.telephony-controller.node.media=simulated \
  --parity.telephony-controller.readiness.endpoints.parity-ms=
```

Without the two `node` overrides TC needs freeswitch's ESL on 127.0.0.1:8021 (the password from
`node.esl.password-file`) and parity-ms on 127.0.0.1:2223, and exits if ESL isn't up within
`node.esl.startup-timeout`.

**Step 3's evidence runs** (one run each, a few minutes; docker, the freeswitch and Playwright
images, and a parity-ms release binary) are integration tests switched on by `-Dtc.evidence=true`;
each writes `target/tc-evidence/<name>.txt`. Run them one at a time (they share container names
and host ports):

```
mvn -B -ntp -Dtest=NONE -Dsurefire.failIfNoSpecifiedTests=false -Dtc.evidence=true \
  -Dtc.parityMs=../parity-ms/target/release/media-server -Dit.test=RealNodeMediaIT verify
# also: RealNodeScaleIT (restart with 100 calls, G-3), RealNodeBrowserIT (a browser agent past 2 min)
```

**fake-telephony-controller** is the same jar with the `fake` profile: a clock that moves only
when told, in-memory owners and journal, and a test-only control endpoint on 127.0.0.1:9702
(`POST /_fake/v1/reset` with a script's `given`, `/_fake/v1/inject` `{what, args}`,
`/_fake/v1/advance` `{ms}`, `GET /_fake/v1/invariants`). The caller's identity comes from the
metadata `x-parity-test-peer: parity-interaction-engine.<bin>`.

```
java -jar target/parity-telephony-controller-0.1.0.jar --spring.profiles.active=fake \
  --parity.telephony-controller.grpc.port=<free port> \
  --parity.telephony-controller.fake.control-port=<free port> \
  --server.port=<free port> --parity.telephony-controller.lock-dir=<a temporary directory>
```

Every port can be overridden, so tests can spawn the jar on free ports (0 lets the system pick,
but then the test can't learn the port: pick free ports and pass them). The control endpoint binds
127.0.0.1 (`parity.telephony-controller.fake.control-host`); inside a container, set it to
`0.0.0.0`.

| Port and file | What |
|---|---|
| 9700 | `telephony.v1` (gRPC; mTLS with step 8) |
| 9701 | `/livez`, `/readyz`, `/metrics` (Prometheus) |
| 9702 | The fake's control endpoint (the `fake` profile only) |
| `/var/lib/parity-telephony-controller/journal` | The hangup journal: a host volume |
| `/tmp/parity-telephony-controller.healthy`, `.drained` | ARCH §7.3 |
| `/run/parity-telephony-controller/lock` | The node lock: a host directory, bind-mounted |

Exit codes: 0 drained; 1 the drain ran out of time, or a failure; 2 a configuration error; 3
the node lock is held; 128 + n a second signal.

## Logs

TC writes parity-contracts' **log-record.v1** (CONVENTIONS "Logging" and "The xsid"): one JSON
object per line on stdout, at most 16000 bytes, masked (`obs.Pii`), through a bounded queue that
drops (and counts, and reports as `log.dropped`) rather than block a call path
(`obs.JsonLogFormatter`, `obs.LogRecordEncoder`, `obs.CountingAsyncAppender`, `logback-spring.xml`).

- **Every log call names its event**, from `logging/events.parity-telephony-controller.v1.json`
  (or parity-contracts' common catalogue), with a constant message; values go in key-value pairs:

  ```java
  LOG.atInfo().addKeyValue("event", "session.arrived")
          .addKeyValue("tenant_id", s.tenantId).addKeyValue("interaction_id", s.interactionId)
          .addKeyValue("leg_id", legId).log("a call arrived");
  ```

  A new event is a new catalogue entry (its levels, `call_scoped`, `requires`, `attrs`);
  `LogEventsTest` fails a call without an event, an event in no catalogue, a level the entry
  doesn't allow, and an entry nothing writes. A record of TC's own code without an event is
  `log.unclassified`, which no catalogue lists; a library's is `log.external`.
- **Correlation**: pairs named like a log-record.v1 field (`tenant_id`, `leg_id`, `bin`,
  `owner_epoch`, `op`, `outcome`, ...) are written at the top level when their value fits it;
  everything else goes under `attrs`. `interaction_id` is written as **`xsid`** when it is a
  lowercase UUIDv7. A call-scoped event (about one call) carries `tenant_id` and the interaction id.
- **Errors**: `.setCause(e)` (kind, message; the stack only at `error`), or `error_kind`/`error_code`
  pairs where a message could carry data.
- `incarnation` is one UUIDv7 per run, and the Watch stream's own unless the clock went back
  (`engine.started` names the engine's); `seq` counts every record made, dropped ones included.
- The tests write the same lines to `target/test-logs/*.ndjson` (`src/test/resources/logback-test.xml`);
  `scripts/check-logs.sh` checks them, and the controllers the integration tests started, with
  parity-contracts' `contract-check --logs`.
