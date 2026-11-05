package parity.errorprone;

import com.google.errorprone.BugPattern;
import com.google.errorprone.BugPattern.SeverityLevel;
import com.google.errorprone.VisitorState;
import com.google.errorprone.bugpatterns.BugChecker;
import com.google.errorprone.bugpatterns.BugChecker.MethodTreeMatcher;
import com.google.errorprone.bugpatterns.BugChecker.SynchronizedTreeMatcher;
import com.google.errorprone.matchers.Description;
import com.sun.source.tree.MethodTree;
import com.sun.source.tree.SynchronizedTree;
import javax.lang.model.element.Modifier;

/**
 * No {@code synchronized} (TC ADR 0002 R1-2): on Java 21 a virtual thread that blocks while it
 * holds a monitor pins its carrier thread (JEP 491 lifts that only from Java 24), and TC's turns,
 * ESL client and media client run on virtual threads and wait for replies. Use
 * {@code java.util.concurrent.locks.ReentrantLock}.
 */
@BugPattern(
        summary = "No synchronized: a virtual thread that waits inside it pins its carrier on Java 21; use ReentrantLock",
        severity = SeverityLevel.ERROR)
public final class NoSynchronized extends BugChecker implements SynchronizedTreeMatcher, MethodTreeMatcher {

    private static final long serialVersionUID = 1L;

    @Override
    public Description matchSynchronized(SynchronizedTree tree, VisitorState state) {
        return describeMatch(tree);
    }

    @Override
    public Description matchMethod(MethodTree tree, VisitorState state) {
        return tree.getModifiers().getFlags().contains(Modifier.SYNCHRONIZED) ? describeMatch(tree)
                : Description.NO_MATCH;
    }
}
