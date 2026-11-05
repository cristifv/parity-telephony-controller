package parity.telephonycontroller.journal;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parity.telephonycontroller.model.Kinds.EndCause;
import parity.telephonycontroller.model.Kinds.EndedBy;
import parity.telephonycontroller.model.Kinds.Endpoint;
import parity.telephonycontroller.model.Kinds.Role;
import parity.telephonycontroller.model.Kinds.SessionEndReason;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * The journal on the host volume (TC ADR 0002 §2.7): JSON lines, one per record.
 *
 * <ul>
 *   <li><b>Group commit.</b> Appends go to a queue ({@value #QUEUE} records) and return at once; one
 *       writer writes them and forces the file at most every {@value #FORCE_EVERY_MS} ms. After a
 *       crash, freeswitch's CDR files cover what the last window lost. A full queue (a stalled disk)
 *       drops the record and counts it. {@link #flush()} waits until everything queued is forced.
 *   <li><b>Segments.</b> {@code journal-<n>.jsonl}, rolled at {@code segmentBytes} or
 *       {@code segmentAgeMs}. The journal keeps an index of live records (those of sessions not yet
 *       released).
 *   <li><b>Compaction.</b> At a roll, if the closed segments hold more than twice the live records,
 *       the live set is written to {@code snapshot-<n>.jsonl.tmp}, forced, renamed to
 *       {@code snapshot-<n>.jsonl}, the directory forced, then older segments and snapshots
 *       deleted. A load reads the newest snapshot and the segments from its number on, so a crash at
 *       any step leaves a consistent set (duplicates merge).
 * </ul>
 *
 * <p>No field for a digit: DTMF never reaches it (R1-4). A torn last line is skipped on load.
 */
public final class FileJournal implements Journal, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(FileJournal.class);
    private static final JsonMapper JSON = JsonMapper.builder().build();
    static final int QUEUE = 65_536;
    static final long FORCE_EVERY_MS = 50;
    static final long DEFAULT_SEGMENT_BYTES = 64L << 20;
    static final long DEFAULT_SEGMENT_AGE_MS = 600_000;
    private static final String INCARNATION_KEY = "\u0000incarnation";

    private final Path dir;
    private final long segmentBytes;
    private final long segmentAgeMs;
    private final Consumer<String> crashPoint;
    private final LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>(QUEUE);
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition forced = lock.newCondition();
    private final AtomicLong dropped = new AtomicLong();
    private final AtomicLong enqueued = new AtomicLong();
    private final Thread writer;
    // Guarded by lock: the index of live records, the open segment, how much was forced.
    private final Map<String, List<String>> live = new LinkedHashMap<>();
    private long closedRecords;
    private long segment;
    private FileChannel open;
    private long openBytes;
    private long openedAtMs;
    private long written;
    private volatile boolean closed;

    public FileJournal(Path dir) {
        this(dir, DEFAULT_SEGMENT_BYTES, DEFAULT_SEGMENT_AGE_MS, step -> { });
    }

    /** Tests: small segments, and a hook called at each step of a compaction (it may throw: a crash). */
    FileJournal(Path dir, long segmentBytes, long segmentAgeMs, Consumer<String> crashPoint) {
        this.dir = dir;
        this.segmentBytes = segmentBytes;
        this.segmentAgeMs = segmentAgeMs;
        this.crashPoint = crashPoint;
        try {
            Files.createDirectories(dir);
            migrateStepFiveFile();
            recover();
            openSegment(nextSegmentNumber());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        writer = Thread.ofPlatform().name("tc-journal").daemon(true).start(this::write);
    }

    // ---- Journal ---------------------------------------------------------------------------------

    @Override
    public void legEnded(LegEnded e) {
        ObjectNode o = JSON.createObjectNode().put("type", "leg_ended").put("tenant_id", e.tenantId())
                .put("interaction_id", e.interactionId()).put("leg_id", e.legId()).put("role", e.role().name())
                .put("endpoint", e.endpoint() == null ? null : e.endpoint().name()).put("sip_call_id", e.sipCallId())
                .put("cause", e.cause().name()).put("sip_status", e.sipStatus()).put("ended_by", e.endedBy().name())
                .put("answered_at_ms", e.answeredAtMs()).put("ended_at_ms", e.endedAtMs())
                .put("add_leg_fingerprint", e.addLegFingerprint());
        append(o);
    }

    @Override
    public void sessionEnded(SessionEnded e) {
        ObjectNode o = JSON.createObjectNode().put("type", "session_ended").put("tenant_id", e.tenantId())
                .put("interaction_id", e.interactionId()).put("reason", e.reason().name())
                .put("sip_status", e.sipStatus()).put("ended_at_ms", e.endedAtMs()).put("owner_epoch", e.ownerEpoch())
                .put("owner_replica_id", e.ownerReplicaId()).put("owner_bin", e.ownerBin());
        if (e.ownerless()) {
            o.put("ownerless", true);
        }
        append(o);
    }

    @Override
    public void acked(Acked e) {
        append(JSON.createObjectNode().put("type", "acked").put("tenant_id", e.tenantId())
                .put("interaction_id", e.interactionId()).put("at_ms", e.atMs()));
    }

    @Override
    public void released(Released e) {
        append(JSON.createObjectNode().put("type", "released").put("tenant_id", e.tenantId())
                .put("interaction_id", e.interactionId()).put("at_ms", System.currentTimeMillis()));
    }

    /**
     * How long a release is remembered (compaction forgets older ones). A rebuild reads only the CDR
     * files freeswitch wrote after the journal's newest record (TC ADR 0002 §2.4), so a released
     * session can't come back from an older one; the marker covers a restart soon after a release
     * and stays small (about 60,000 at 100 calls/s).
     */
    static final long RELEASE_MEMORY_MS = 600_000;

    @Override
    public void incarnation(String incarnation) {
        append(JSON.createObjectNode().put("type", "incarnation").put("incarnation", incarnation));
        flush();
    }

    @Override
    public Optional<String> lastIncarnation() {
        lock.lock();
        try {
            List<String> l = live.get(INCARNATION_KEY);
            if (l == null || l.isEmpty()) {
                return Optional.empty();
            }
            return Optional.ofNullable(s(JSON.readTree(l.get(l.size() - 1)), "incarnation"));
        } finally {
            lock.unlock();
        }
    }

    /** What the journal holds now: the live records, in the order written. */
    @Override
    public Contents load() {
        flush();
        List<String> lines = new ArrayList<>();
        lock.lock();
        try {
            live.values().forEach(lines::addAll);
        } finally {
            lock.unlock();
        }
        return parse(lines);
    }

    /** Waits until every record queued so far is written and forced (a drain, tests). */
    public void flush() {
        long target = enqueued.get();
        lock.lock();
        try {
            while (written < target && !closed) {
                forced.await(100, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            lock.unlock();
        }
    }

    /** Test hook (a script's reset): everything goes, and the journal starts again empty. */
    public void clear() {
        flush();
        lock.lock();
        try {
            open.close();
            try (Stream<Path> files = Files.list(dir)) {
                for (Path p : files.toList()) {
                    String n = p.getFileName().toString();
                    if (n.startsWith("journal-") || n.startsWith("snapshot-")) {
                        Files.deleteIfExists(p);
                    }
                }
            }
            live.clear();
            closedRecords = 0;
            openSegment(1);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            lock.unlock();
        }
    }

    /** Records dropped because the queue was full (a metric, alerted). */
    public long dropped() {
        return dropped.get();
    }

    @Override
    public void close() {
        flush();
        closed = true;
        writer.interrupt();
        try {
            writer.join(2_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        lock.lock();
        try {
            if (open != null) {
                open.close();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            lock.unlock();
        }
    }

    // ---- writing ---------------------------------------------------------------------------------

    private void append(ObjectNode o) {
        String line = JSON.writeValueAsString(o);
        if (!queue.offer(line)) {
            dropped.incrementAndGet();
            LOG.atError().addKeyValue("event", "journal.record_dropped")
                    .log("the journal's queue is full: a record was dropped; freeswitch's CDR files remain");
            return;
        }
        enqueued.incrementAndGet();
    }

    private void write() {
        long lastForce = 0;
        List<String> batch = new ArrayList<>();
        while (!closed) {
            try {
                String first = queue.poll(100, TimeUnit.MILLISECONDS);
                if (first == null) {
                    maybeRoll();
                    continue;
                }
                long wait = lastForce + FORCE_EVERY_MS - System.currentTimeMillis();
                if (wait > 0) {
                    Thread.sleep(wait);
                }
                batch.clear();
                batch.add(first);
                queue.drainTo(batch);
                writeBatch(batch);
                lastForce = System.currentTimeMillis();
                maybeRoll();
            } catch (InterruptedException e) {
                if (closed) {
                    return;
                }
            } catch (IOException | RuntimeException e) {
                LOG.atError().addKeyValue("event", "journal.write_failed").addKeyValue("error_kind", e.getClass().getSimpleName())
                        .log("the journal couldn't write");
            }
        }
    }

    private void writeBatch(List<String> batch) throws IOException {
        StringBuilder sb = new StringBuilder();
        for (String l : batch) {
            sb.append(l).append('\n');
        }
        ByteBuffer b = ByteBuffer.wrap(sb.toString().getBytes(StandardCharsets.UTF_8));
        lock.lock();
        try {
            int n = b.remaining();
            while (b.hasRemaining()) {
                open.write(b);
            }
            open.force(false);
            openBytes += n;
            for (String l : batch) {
                index(l);
            }
            written += batch.size();
            forced.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /** Lock held: a record joins its session's live records; a release drops them all. */
    private void index(String line) {
        JsonNode o;
        try {
            o = JSON.readTree(line);
        } catch (RuntimeException torn) {
            return;
        }
        String type = s(o, "type");
        if ("incarnation".equals(type)) {
            live.put(INCARNATION_KEY, new ArrayList<>(List.of(line)));
            return;
        }
        String key = s(o, "tenant_id") + "/" + s(o, "interaction_id");
        if ("released".equals(type)) {
            live.remove(key);
            live.put("released/" + key, new ArrayList<>(List.of(line)));
            return;
        }
        live.computeIfAbsent(key, k -> new ArrayList<>()).add(line);
    }

    private void maybeRoll() throws IOException {
        lock.lock();
        try {
            boolean big = openBytes >= segmentBytes;
            boolean old = openBytes > 0 && System.currentTimeMillis() - openedAtMs >= segmentAgeMs;
            if (!big && !old) {
                return;
            }
            closedRecords += lineCount(segmentFile(segment));
            open.close();
            long next = segment + 1;
            long liveRecords = live.values().stream().mapToLong(List::size).sum();
            if (closedRecords > 2 * liveRecords) {
                compact(next);
                closedRecords = liveRecords;
            }
            openSegment(next);
        } finally {
            lock.unlock();
        }
    }

    /** Lock held: the live set as snapshot-{@code n}, then the files it replaces go. */
    private void compact(long n) throws IOException {
        Path tmp = dir.resolve(String.format("snapshot-%010d.jsonl.tmp", n));
        Path snap = dir.resolve(String.format("snapshot-%010d.jsonl", n));
        long forgetBefore = System.currentTimeMillis() - RELEASE_MEMORY_MS;
        live.entrySet().removeIf(e -> e.getKey().startsWith("released/") && releasedAt(e.getValue()) < forgetBefore);
        StringBuilder sb = new StringBuilder();
        live.values().forEach(lines -> lines.forEach(l -> sb.append(l).append('\n')));
        try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            ByteBuffer b = ByteBuffer.wrap(sb.toString().getBytes(StandardCharsets.UTF_8));
            while (b.hasRemaining()) {
                ch.write(b);
            }
            ch.force(true);
        }
        crashPoint.accept("before-rename");
        Files.move(tmp, snap, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        forceDirectory();
        crashPoint.accept("after-rename");
        deleteBefore(n);
    }

    private void deleteBefore(long n) throws IOException {
        try (Stream<Path> files = Files.list(dir)) {
            for (Path p : files.toList()) {
                long num = number(p);
                String name = p.getFileName().toString();
                boolean oldSegment = name.startsWith("journal-") && name.endsWith(".jsonl") && num >= 0 && num < n;
                boolean oldSnapshot = name.startsWith("snapshot-") && name.endsWith(".jsonl") && num >= 0 && num < n;
                if (oldSegment || oldSnapshot) {
                    Files.deleteIfExists(p);
                    crashPoint.accept("during-deletes");
                }
            }
        }
    }

    private void openSegment(long n) throws IOException {
        segment = n;
        open = FileChannel.open(segmentFile(n), StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.APPEND);
        openBytes = open.size();
        openedAtMs = System.currentTimeMillis();
    }

    private Path segmentFile(long n) {
        return dir.resolve(String.format("journal-%010d.jsonl", n));
    }

    private void forceDirectory() {
        try (FileChannel d = FileChannel.open(dir, StandardOpenOption.READ)) {
            d.force(true);
        } catch (IOException e) {
            // Some filesystems can't open a directory; the rename is still atomic.
        }
    }

    // ---- reading ---------------------------------------------------------------------------------

    /** The newest snapshot, then every segment from its number on, into the index. */
    private void recover() throws IOException {
        long snapshot = -1;
        List<Long> segments = new ArrayList<>();
        try (Stream<Path> files = Files.list(dir)) {
            for (Path p : files.toList()) {
                String name = p.getFileName().toString();
                long num = number(p);
                if (name.endsWith(".tmp")) {
                    Files.deleteIfExists(p);
                } else if (name.startsWith("snapshot-") && num > snapshot) {
                    snapshot = num;
                } else if (name.startsWith("journal-") && num >= 0) {
                    segments.add(num);
                }
            }
        }
        segments.sort(Long::compare);
        if (snapshot >= 0) {
            for (String l : readLines(dir.resolve(String.format("snapshot-%010d.jsonl", snapshot)))) {
                index(l);
            }
        }
        long last = Math.max(snapshot, 0);
        for (long n : segments) {
            if (n >= Math.max(snapshot, 0)) {
                List<String> lines = readLines(segmentFile(n));
                lines.forEach(this::index);
                closedRecords += lines.size();
                last = Math.max(last, n);
            }
        }
        segment = last;
        if (snapshot >= 0) {
            deleteBefore(snapshot);
        }
    }

    private long nextSegmentNumber() {
        return segment + 1;
    }

    /** Step 5's single {@code journal.jsonl} becomes the first segment. */
    private void migrateStepFiveFile() throws IOException {
        Path old = dir.resolve("journal.jsonl");
        if (Files.exists(old)) {
            Files.move(old, segmentFile(0), StandardCopyOption.ATOMIC_MOVE);
        }
    }

    private static List<String> readLines(Path p) throws IOException {
        if (!Files.exists(p)) {
            return List.of();
        }
        List<String> out = new ArrayList<>();
        for (String line : Files.readAllLines(p, StandardCharsets.UTF_8)) {
            if (!line.isBlank()) {
                out.add(line);
            }
        }
        return out;
    }

    private static long lineCount(Path p) throws IOException {
        return readLines(p).size();
    }

    private static long number(Path p) {
        String name = p.getFileName().toString();
        int dash = name.indexOf('-');
        int dot = name.indexOf('.');
        if (dash < 0 || dot < dash) {
            return -1;
        }
        try {
            return Long.parseLong(name.substring(dash + 1, dot));
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    static Contents parse(List<String> lines) {
        List<LegEnded> legs = new ArrayList<>();
        List<SessionEnded> sessions = new ArrayList<>();
        List<Acked> acks = new ArrayList<>();
        List<Released> released = new ArrayList<>();
        for (String line : lines) {
            JsonNode o;
            try {
                o = JSON.readTree(line);
            } catch (RuntimeException torn) {
                continue;
            }
            String t = s(o, "tenant_id");
            String i = s(o, "interaction_id");
            String type = s(o, "type");
            if (type == null) {
                continue;
            }
            switch (type) {
                case "leg_ended" -> legs.add(new LegEnded(t, i, s(o, "leg_id"), Role.valueOf(s(o, "role")),
                        o.get("endpoint") == null || o.get("endpoint").isNull() ? null : Endpoint.valueOf(s(o, "endpoint")),
                        s(o, "sip_call_id"), EndCause.valueOf(s(o, "cause")), o.get("sip_status").asInt(),
                        EndedBy.valueOf(s(o, "ended_by")), o.get("answered_at_ms").asLong(),
                        o.get("ended_at_ms").asLong(), s(o, "add_leg_fingerprint")));
                case "session_ended" -> sessions.add(new SessionEnded(t, i,
                        SessionEndReason.valueOf(s(o, "reason")), o.get("sip_status").asInt(),
                        o.get("ended_at_ms").asLong(), o.get("owner_epoch").asLong(),
                        s(o, "owner_replica_id"), s(o, "owner_bin"),
                        o.has("ownerless") && o.get("ownerless").asBoolean()));
                case "acked" -> acks.add(new Acked(t, i, o.get("at_ms").asLong()));
                case "released" -> released.add(new Released(t, i));
                default -> { }
            }
        }
        return new Contents(legs, sessions, acks, released);
    }

    private static long releasedAt(List<String> lines) {
        try {
            JsonNode o = JSON.readTree(lines.get(0));
            return o.has("at_ms") ? o.get("at_ms").asLong() : Long.MAX_VALUE;
        } catch (RuntimeException e) {
            return Long.MAX_VALUE;
        }
    }

    private static String s(JsonNode o, String name) {
        JsonNode n = o.get(name);
        return n == null || n.isNull() ? null : n.asString();
    }
}
