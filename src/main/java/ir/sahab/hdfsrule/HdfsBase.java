package ir.sahab.hdfsrule;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class HdfsBase {

    private final Lock lock = new ReentrantLock();

    private MiniDFSCluster hdfsCluster = null;
    private File miniClusterDataDir = null;

    void setup() throws Exception {
        lock.lock();
        try {
            miniClusterDataDir = Files.createTempDirectory("hdfs-").toFile();
            Configuration conf = new Configuration();
            conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, miniClusterDataDir.getAbsolutePath());
            hdfsCluster = new MiniDFSCluster.Builder(conf).build();
            hdfsCluster.waitActive();
        } finally {
            lock.unlock();
        }
    }

    void teardown() {
        lock.lock();
        try {
            if (hdfsCluster != null && hdfsCluster.isClusterUp())
                hdfsCluster.shutdown();
            if (miniClusterDataDir != null) {
                try {
                    FileUtils.deleteDirectory(miniClusterDataDir);
                } catch (IOException e) {
                    throw new AssertionError("Failed to delete HDFS mini cluster's data directory.", e);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public URI getUri() {
        return hdfsCluster.getURI();
    }

    public void writeString(Path path, String content) throws IOException {
        write(path, content.getBytes());
    }

    public void write(Path path, byte[] content) throws IOException {
        try (FileSystem fs = getFileSystem(); FSDataOutputStream outputStream = fs.create(path)) {
            outputStream.write(content);
        }
    }

    public String readString(Path path) throws IOException {
        return new String(read(path), StandardCharsets.UTF_8);
    }

    public byte[] read(Path path) throws IOException {
        try (FileSystem fs = getFileSystem(); FSDataInputStream inputStream = fs.open(path)) {
            return IOUtils.toByteArray(inputStream);
        }
    }

    public String getNameNodeAddresses() {
        StringBuilder addresses = new StringBuilder();
        for (int i = 0; i < hdfsCluster.getNumNameNodes(); i++) {
            addresses.append("127.0.0.1:").append(hdfsCluster.getNameNodeServicePort(i)).append(",");
        }
        // Remove the last comma.
        addresses.deleteCharAt(addresses.length() - 1);
        return addresses.toString();
    }

    public DistributedFileSystem getFileSystem() throws IOException {
        return hdfsCluster.getFileSystem();
    }
}
