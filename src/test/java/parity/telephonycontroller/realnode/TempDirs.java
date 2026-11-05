package parity.telephonycontroller.realnode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

/** The tests' temporary directories go when their owner closes (BUGS.md TC-019). */
public final class TempDirs {

    private TempDirs() {}

    /** Deletes a directory and what it holds; what can't be deleted (another user's file) stays. */
    public static void delete(Path dir) {
        if (dir == null || !Files.exists(dir)) {
            return;
        }
        try (Stream<Path> all = Files.walk(dir)) {
            for (Path p : all.sorted(Comparator.reverseOrder()).toList()) {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException e) {
                    // left for the system's own temp clean-up
                }
            }
        } catch (IOException e) {
            // as above
        }
    }

    /** Deletes the directory when the JVM exits (for a test class's static directory). */
    public static Path deleteOnExit(Path dir) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> delete(dir)));
        return dir;
    }
}
