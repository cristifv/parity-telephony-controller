package parity.telephonycontroller.journal;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import parity.telephonycontroller.model.Kinds.EndCause;
import parity.telephonycontroller.model.Kinds.EndedBy;
import parity.telephonycontroller.model.Kinds.Endpoint;
import parity.telephonycontroller.model.Kinds.Role;
import parity.telephonycontroller.model.Kinds.SessionEndReason;
import parity.telephonycontroller.model.UuidV7;

/**
 * The hangup journal (TC ADR 0002 §2.7): it survives a restart and skips a torn last line; it
 * commits in groups; a release drops a session's records but remembers the release; compaction
 * keeps only the live set, and a crash at any step of it leaves a consistent journal; the last
 * incarnation is kept; no digit is ever in it (R1-4).
 */
class FileJournalTest {

    @TempDir
    Path dir;

    private static Journal.LegEnded leg(String interaction, String legId) {
        return new Journal.LegEnded("acme", interaction, legId, Role.AGENT, Endpoint.WEBRTC, legId, EndCause.BUSY, 486,
                EndedBy.REMOTE, 0, 1000, "fp");
    }

    private static Journal.SessionEnded end(String interaction) {
        return new Journal.SessionEnded("acme", interaction, SessionEndReason.ALL_LEGS_GONE, 0, 1000, 1042, "r1", "bin-a");
    }

    private List<Path> files(String prefix) throws IOException {
        try (Stream<Path> s = Files.list(dir)) {
            return s.filter(p -> p.getFileName().toString().startsWith(prefix)).sorted().toList();
        }
    }

    @Test
    void whatIsWrittenComesBackAfterARestart() throws Exception {
        try (FileJournal j = new FileJournal(dir)) {
            j.legEnded(leg("i-1", "leg-1"));
            j.sessionEnded(end("i-1"));
            j.acked(new Journal.Acked("acme", "i-1", 2000));
        }
        Path last = files("journal-").getLast();
        Files.writeString(last, "{\"type\":\"leg_en", StandardOpenOption.APPEND);

        try (FileJournal again = new FileJournal(dir)) {
            Journal.Contents c = again.load();
            assertThat(c.legs()).singleElement().satisfies(l -> {
                assertThat(l.cause()).isEqualTo(EndCause.BUSY);
                assertThat(l.addLegFingerprint()).isEqualTo("fp");
            });
            assertThat(c.sessions()).singleElement().satisfies(s -> assertThat(s.ownerEpoch()).isEqualTo(1042));
            assertThat(c.acks()).hasSize(1);
        }
        for (Path p : files("journal-")) {
            assertThat(Files.readString(p)).doesNotContain("digit", "dtmf");
        }
    }

    @Test
    void stepFivesSingleFileBecomesTheFirstSegment() throws Exception {
        Files.writeString(dir.resolve("journal.jsonl"), "{\"type\":\"session_ended\",\"tenant_id\":\"acme\","
                + "\"interaction_id\":\"i-9\",\"reason\":\"ALL_LEGS_GONE\",\"sip_status\":0,\"ended_at_ms\":1,"
                + "\"owner_epoch\":7,\"owner_replica_id\":\"r1\",\"owner_bin\":\"bin-a\"}\n");
        try (FileJournal j = new FileJournal(dir)) {
            assertThat(j.load().sessions()).singleElement().satisfies(s -> assertThat(s.interactionId()).isEqualTo("i-9"));
        }
        assertThat(dir.resolve("journal.jsonl")).doesNotExist();
    }

    @Test
    void appendsReturnAtOnceAndAFlushForcesThemAll() throws Exception {
        try (FileJournal j = new FileJournal(dir)) {
            long t0 = System.nanoTime();
            for (int i = 0; i < 2_000; i++) {
                j.legEnded(leg("i-" + i, "leg-" + i));
            }
            long tookMs = (System.nanoTime() - t0) / 1_000_000;
            assertThat(tookMs).as("2,000 appends queue without waiting for a force").isLessThan(1_000);
            j.flush();
            assertThat(j.load().legs()).hasSize(2_000);
            assertThat(j.dropped()).isZero();
        }
        try (FileJournal again = new FileJournal(dir)) {
            assertThat(again.load().legs()).hasSize(2_000);
        }
    }

    @Test
    void aReleaseDropsTheSessionsRecordsAndRemembersTheRelease() throws Exception {
        try (FileJournal j = new FileJournal(dir)) {
            j.legEnded(leg("gone", "leg-1"));
            j.sessionEnded(end("gone"));
            j.released(new Journal.Released("acme", "gone"));
            j.legEnded(leg("kept", "leg-2"));
            Journal.Contents c = j.load();
            assertThat(c.legs()).extracting(Journal.LegEnded::interactionId).containsExactly("kept");
            assertThat(c.sessions()).isEmpty();
            assertThat(c.released()).extracting(Journal.Released::interactionId).containsExactly("gone");
        }
    }

    @Test
    void compactionKeepsTheLiveSetAndRemovesWhatItReplaced() throws Exception {
        try (FileJournal j = new FileJournal(dir, 2_048, Long.MAX_VALUE, step -> { })) {
            for (int i = 0; i < 300; i++) {
                j.legEnded(leg("s-" + i, "leg-" + i));
                j.sessionEnded(end("s-" + i));
                if (i % 10 != 0) {
                    j.released(new Journal.Released("acme", "s-" + i));
                }
                if (i % 25 == 0) {
                    j.flush();
                }
            }
            j.flush();
            Thread.sleep(300);
            assertThat(files("snapshot-")).as("a compaction ran").isNotEmpty();
            assertThat(files("journal-").size()).as("old segments deleted").isLessThan(10);
        }
        try (FileJournal again = new FileJournal(dir)) {
            Journal.Contents c = again.load();
            assertThat(c.legs()).extracting(Journal.LegEnded::interactionId).containsExactlyInAnyOrderElementsOf(
                    java.util.stream.IntStream.range(0, 300).filter(i -> i % 10 == 0).mapToObj(i -> "s-" + i).toList());
            assertThat(c.sessions()).hasSize(30);
            assertThat(c.released()).hasSize(270);
        }
    }

    /** A crash ({@code kill -9}) at a step of a compaction: the writer stops dead there. */
    static final class Crash extends Error {
        private static final long serialVersionUID = 1L;

        Crash() {
            super("simulated kill -9", null, false, false);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"before-rename", "after-rename", "during-deletes"})
    void aCrashAtAnyStepOfACompactionLeavesAConsistentJournal(String point) throws Exception {
        // Armed only once every record is forced (TC-040): a crash at an earlier compaction would
        // leave later releases unwritten, and the check below expects all 200 sessions on disk.
        java.util.concurrent.atomic.AtomicBoolean armed = new java.util.concurrent.atomic.AtomicBoolean(false);
        java.util.concurrent.atomic.AtomicBoolean crashed = new java.util.concurrent.atomic.AtomicBoolean(false);
        FileJournal j = new FileJournal(dir, 2_048, Long.MAX_VALUE, step -> {
            if (step.equals(point) && armed.compareAndSet(true, false)) {
                crashed.set(true);
                throw new Crash();
            }
        });
        for (int i = 0; i < 200; i++) {
            j.legEnded(leg("s-" + i, "leg-" + i));
            j.sessionEnded(end("s-" + i));
            j.acked(new Journal.Acked("acme", "s-" + i, 2000));
            if (i % 4 != 0) {
                j.released(new Journal.Released("acme", "s-" + i));
            }
        }
        j.flush();
        armed.set(true);
        // More releases of released sessions (no change to what is live) roll a segment and so
        // start a compaction, which crashes at the point.
        long until = System.currentTimeMillis() + 5_000;
        for (int i = 1; !crashed.get() && System.currentTimeMillis() < until; i = i % 199 + 1) {
            if (i % 4 != 0) {
                j.released(new Journal.Released("acme", "s-" + i));
            }
            Thread.sleep(1);
        }
        armed.set(false);
        assertThat(crashed).as("the compaction reached " + point).isTrue();
        // The process is gone: whatever was forced before the crash is on disk; a new run reads it.
        Thread.sleep(100);
        try (FileJournal again = new FileJournal(dir)) {
            Journal.Contents c = again.load();
            List<String> legs = c.legs().stream().map(Journal.LegEnded::interactionId).toList();
            assertThat(legs).as("no record twice").doesNotHaveDuplicates();
            assertThat(legs).as("every live record kept").allSatisfy(id ->
                    assertThat(Integer.parseInt(id.substring(2)) % 4).isZero());
            assertThat(c.released().stream().map(Journal.Released::interactionId).toList())
                    .doesNotContainAnyElementsOf(legs);
            assertThat(files("snapshot-").stream().filter(p -> p.toString().endsWith(".tmp")).toList()).isEmpty();
        }
    }

    @Test
    void anOldReleaseIsForgottenAtTheNextCompaction() throws Exception {
        Files.writeString(dir.resolve("journal-0000000000.jsonl"),
                "{\"type\":\"released\",\"tenant_id\":\"acme\",\"interaction_id\":\"ancient\",\"at_ms\":1}\n");
        try (FileJournal j = new FileJournal(dir, 2_048, Long.MAX_VALUE, step -> { })) {
            assertThat(j.load().released()).extracting(Journal.Released::interactionId).contains("ancient");
            for (int i = 0; i < 200; i++) {
                j.legEnded(leg("s-" + i, "leg-" + i));
                j.sessionEnded(end("s-" + i));
                j.acked(new Journal.Acked("acme", "s-" + i, 2000));
                j.released(new Journal.Released("acme", "s-" + i));
            }
            j.flush();
            Thread.sleep(300);
            assertThat(files("snapshot-")).isNotEmpty();
            assertThat(j.load().released()).extracting(Journal.Released::interactionId).doesNotContain("ancient");
        }
    }

    @Test
    void theLastIncarnationIsKeptAndTheNextSortsAfterItWhateverTheClock() throws Exception {
        String ahead = UuidV7.create(System.currentTimeMillis() + 60_000, new java.util.SplittableRandom(3));
        try (FileJournal j = new FileJournal(dir)) {
            assertThat(j.lastIncarnation()).isEmpty();
            j.incarnation(ahead);
        }
        try (FileJournal again = new FileJournal(dir)) {
            assertThat(again.lastIncarnation()).contains(ahead);
            String next = UuidV7.after(again.lastIncarnation().orElseThrow());
            assertThat(next).isGreaterThan(ahead);
        }
    }
}
