package ir.sahab.hdfsrule;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HdfsExtensionTest {
    @RegisterExtension
    static final HdfsExtension hdfsExtension = new HdfsExtension();

    @Test
    void testDirectReadWrite() throws IOException {
        // The file name and the content which is written to HDFS file system
        String fileName = "hello1.csv";
        String fileContent = "hello;world";
        Path path = new Path("/" + fileName);

        // Write the string to the specified file
        hdfsExtension.writeString(path, fileContent);

        // Read the file
        String retrieved = hdfsExtension.readString(path);

        // Check whether the retrieved content is the same as what is written before
        assertEquals(fileContent, retrieved);
    }

    @Test
    void testReadWriteViaFileSystem() throws IOException {
        // The file name and the content which is written to HDFS file system
        String fileName = "hello2.csv";
        String fileContent = "hello;world";
        Path path = new Path("/" + fileName);

        // Get the file system from HDFS rule
        FileSystem fs = hdfsExtension.getFileSystem();

        // Write file to HDFS
        FSDataOutputStream outputStream = fs.create(path);
        outputStream.writeBytes(fileContent);
        outputStream.close();

        // Read file from HDFS
        FSDataInputStream inputStream = fs.open(path);
        String retrieved = IOUtils.toString(inputStream);
        inputStream.close();

        // Close the file system.
        fs.close();

        // Check whether the retrieved content is the same as what is written before
        assertEquals(fileContent, retrieved);
    }

    @Test
    void testReadWriteFromScratch() throws IOException {
        // The file name and content which is written and read to and from HDFS
        String fileName = "hello3.csv";
        String fileContent = "hello;world";
        Path path = new Path("/" + fileName);

        // Create an HDFS filesystem
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", hdfsExtension.getUri().toString());
        FileSystem fs = FileSystem.get(configuration);

        // Write the file to HDFS
        FSDataOutputStream outputStream = fs.create(path);
        outputStream.writeBytes(fileContent);
        outputStream.close();

        // Read the file from HDFS
        FSDataInputStream inputStream = fs.open(path);
        String retrieved = IOUtils.toString(inputStream);
        inputStream.close();

        // Close the file system.
        fs.close();

        // Check whether the retrieved content is the same as what is written before
        assertEquals(fileContent, retrieved);
    }
}
