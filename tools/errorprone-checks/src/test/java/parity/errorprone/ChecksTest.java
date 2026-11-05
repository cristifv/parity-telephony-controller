package parity.errorprone;

import com.google.errorprone.CompilationTestHelper;
import org.junit.jupiter.api.Test;

/** Both checks on code that passes and code that must fail ({@code // BUG: Diagnostic contains:}). */
class ChecksTest {

    private final CompilationTestHelper messages = CompilationTestHelper.newInstance(NoDataInMessages.class, getClass());
    private final CompilationTestHelper sync = CompilationTestHelper.newInstance(NoSynchronized.class, getClass());

    @Test
    void constantsNumbersEnumsAndPathsMayBeInMessages() {
        messages.addSourceLines("Ok.java",
                "import java.nio.file.Path;",
                "import org.slf4j.Logger;",
                "import org.slf4j.LoggerFactory;",
                "class Ok {",
                "  enum Part { A, B }",
                "  static final String CONST = \"a constant\";",
                "  private static final Logger LOG = LoggerFactory.getLogger(Ok.class);",
                "  void f(Path p, int status, Part part, String number) {",
                "    LOG.info(\"started\");",
                "    LOG.info(CONST);",
                "    LOG.atWarn().addKeyValue(\"caller_number\", number).log(\"masked by the layer\");",
                "    LOG.warn(\"etcd answered {}\", status);",
                "    throw new IllegalStateException(\"can't lock \" + p + \" (\" + status + \", \" + part + \")\");",
                "  }",
                "  void g(boolean b) {",
                "    throw new IllegalArgumentException(b ? \"yes\" : \"no\", null);",
                "  }",
                "  static final class Mine extends Exception {",
                "    Mine(Part part) { super(part + \" is unavailable\"); }",
                "    Mine(Part part, int n) { super(part.name() + n); }",
                "  }",
                "  static final class Refused extends RuntimeException {",
                "    Refused(@com.google.errorprone.annotations.CompileTimeConstant String message) { super(message); }",
                "  }",
                "}")
                .doTest();
    }

    @Test
    void aStringInALogMessageFails() {
        messages.addSourceLines("Bad.java",
                "import org.slf4j.Logger;",
                "import org.slf4j.LoggerFactory;",
                "class Bad {",
                "  private static final Logger LOG = LoggerFactory.getLogger(Bad.class);",
                "  void f(String number) {",
                "    // BUG: Diagnostic contains: NoDataInMessages",
                "    LOG.info(\"caller \" + number);",
                "    // BUG: Diagnostic contains: NoDataInMessages",
                "    LOG.info(\"caller {}\", number);",
                "    // BUG: Diagnostic contains: NoDataInMessages",
                "    LOG.atInfo().log(number);",
                "    // BUG: Diagnostic contains: NoDataInMessages",
                "    LOG.atInfo().addArgument(number).log(\"x {}\");",
                "  }",
                "}")
                .doTest();
    }

    @Test
    void aStringInAnExceptionMessageFails() {
        messages.addSourceLines("Bad.java",
                "class Bad {",
                "  void f(String sdp) {",
                "    // BUG: Diagnostic contains: NoDataInMessages",
                "    throw new IllegalArgumentException(\"bad sdp \" + sdp);",
                "  }",
                "  void g(String what) {",
                "    // BUG: Diagnostic contains: NoDataInMessages",
                "    throw new IllegalStateException(String.format(\"unknown %s\", what));",
                "  }",
                "  static final class Mine extends RuntimeException {",
                "    // BUG: Diagnostic contains: NoDataInMessages",
                "    Mine(String digits) { super(\"digits \" + digits); }",
                "  }",
                "}")
                .doTest();
    }

    @Test
    void aProtobufMessageIsNeverConcatenatedOrLogged() {
        messages.addSourceLines("Bad.java",
                "import com.google.protobuf.Struct;",
                "import org.slf4j.Logger;",
                "import org.slf4j.LoggerFactory;",
                "class Bad {",
                "  private static final Logger LOG = LoggerFactory.getLogger(Bad.class);",
                "  String f(Struct request) {",
                "    // BUG: Diagnostic contains: NoDataInMessages",
                "    LOG.atInfo().addKeyValue(\"request\", request).log(\"got it\");",
                "    // BUG: Diagnostic contains: NoDataInMessages",
                "    return \"request: \" + request;",
                "  }",
                "}")
                .doTest();
    }

    @Test
    void synchronizedMethodsAndBlocksFail() {
        sync.addSourceLines("Bad.java",
                "class Bad {",
                "  private final Object lock = new Object();",
                "  // BUG: Diagnostic contains: NoSynchronized",
                "  synchronized void f() {}",
                "  void g() {",
                "    // BUG: Diagnostic contains: NoSynchronized",
                "    synchronized (lock) { lock.notifyAll(); }",
                "  }",
                "}")
                .doTest();
    }

    @Test
    void reentrantLocksPass() {
        sync.addSourceLines("Ok.java",
                "import java.util.concurrent.locks.ReentrantLock;",
                "class Ok {",
                "  private final ReentrantLock lock = new ReentrantLock();",
                "  void f() { lock.lock(); try { } finally { lock.unlock(); } }",
                "}")
                .doTest();
    }
}
