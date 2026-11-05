package parity.telephonycontroller.lifecycle;

import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import parity.telephonycontroller.config.ControllerProperties;

/**
 * One controller per media node (PLAN.md R1-5): an exclusive lock on {@code <lock-dir>/lock},
 * a host directory bind-mounted into every controller container on the host. It is taken
 * before anything else touches the node (even the stale healthy file), held for the process's
 * life and released by the kernel if the process dies.
 *
 * <p>On Linux, Java's {@link FileChannel#tryLock()} is a POSIX record lock ({@code fcntl}), not
 * {@code flock(2)}. Both exclude other processes, in other containers too, as long as they see
 * the same file. A POSIX lock is dropped when any descriptor of the file closes in this
 * process, so nothing else here may open the lock file.
 */
@Component
public class NodeLock {

    private static final Logger log = LoggerFactory.getLogger(NodeLock.class);

    private final Path lockFile;
    private final FileChannel channel;
    private final FileLock lock;

    public NodeLock(ControllerProperties properties) {
        Path dir = Path.of(properties.lockDir());
        this.lockFile = dir.resolve("lock");
        try {
            Files.createDirectories(dir);
            this.channel = FileChannel.open(lockFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        } catch (IOException e) {
            throw new UncheckedIOException("can't open the node lock " + lockFile, e);
        }
        FileLock taken;
        try {
            taken = channel.tryLock();
        } catch (OverlappingFileLockException e) {
            taken = null;
        } catch (IOException e) {
            closeQuietly();
            throw new UncheckedIOException("can't lock " + lockFile, e);
        }
        if (taken == null) {
            closeQuietly();
            log.atError().addKeyValue("event", "node_lock.refused").addKeyValue("lock_file", lockFile.toString())
                    .log("another telephony controller holds the node lock; refusing to start");
            throw new NodeLockedException(lockFile);
        }
        this.lock = taken;
        log.atInfo().addKeyValue("event", "node_lock.acquired").addKeyValue("lock_file", lockFile.toString())
                .log("node lock acquired");
    }

    public boolean held() {
        return lock.isValid();
    }

    public Path lockFile() {
        return lockFile;
    }

    /** After the drain (beans are destroyed after the lifecycle stops). */
    @PreDestroy
    void release() {
        try {
            if (lock.isValid()) {
                lock.release();
            }
            channel.close();
            log.atInfo().addKeyValue("event", "node_lock.released").addKeyValue("lock_file", lockFile.toString())
                    .log("node lock released");
        } catch (IOException e) {
            log.atWarn().addKeyValue("event", "node_lock.release_failed").setCause(e).log("releasing the node lock");
        }
    }

    private void closeQuietly() {
        try {
            channel.close();
        } catch (IOException ignored) {
            // the lock was not taken; nothing to undo
        }
    }
}
