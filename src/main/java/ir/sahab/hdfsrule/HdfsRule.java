package ir.sahab.hdfsrule;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.rules.ExternalResource;

import java.io.IOException;
import java.net.URI;

/**
 * A JUnit 4 rule for starting an HDFS server on the local machine.
 */
public class HdfsRule extends ExternalResource {

    private final HdfsBase base = new HdfsBase();

    @Override
    protected void before() throws Throwable {
        base.setup();
    }

    @Override
    protected void after() {
        base.teardown();
    }

    public URI getUri() {
        return base.getUri();
    }

    public void writeString(Path path, String content) throws IOException {
        base.writeString(path, content);
    }

    public void write(Path path, byte[] content) throws IOException {
        base.write(path, content);
    }

    public String readString(Path path) throws IOException {
        return base.readString(path);
    }

    public byte[] read(Path path) throws IOException {
        return base.read(path);
    }

    public String getNameNodeAddresses() {
        return base.getNameNodeAddresses();
    }

    public DistributedFileSystem getFileSystem() throws IOException {
        return base.getFileSystem();
    }
}
