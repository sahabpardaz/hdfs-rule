package ir.sahab.hdfsrule;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.*;

/**
 * A JUnit 5 extension for starting an HDFS server on the local machine.
 */
public class HdfsExtension extends HdfsBase implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback {

    private final TestInstance.Lifecycle lifecycle;

    public HdfsExtension() {
        this(TestInstance.Lifecycle.PER_CLASS);
    }

    public HdfsExtension(TestInstance.Lifecycle lifecycle) {
        this.lifecycle = lifecycle;
    }

    @Override
    public void afterAll(ExtensionContext context) {
        if (lifecycle == TestInstance.Lifecycle.PER_CLASS) {
            teardown();
        }
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        if (lifecycle == TestInstance.Lifecycle.PER_CLASS) {
            setup();
        }
    }

    @Override
    public void afterEach(ExtensionContext context) {
        if (lifecycle == TestInstance.Lifecycle.PER_METHOD) {
            teardown();
        }
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        if (lifecycle == TestInstance.Lifecycle.PER_METHOD) {
            setup();
        }
    }
}
