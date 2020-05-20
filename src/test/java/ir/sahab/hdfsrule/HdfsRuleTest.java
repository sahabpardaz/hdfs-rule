package ir.sahab.hdfsrule;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.ClassRule;
import org.junit.Test;

public class HdfsRuleTest {

    @ClassRule
    public static final HdfsRule hdfsRule = new HdfsRule();

    @Test
    public void testDirectReadWrite() throws IOException {
        // The file name and the content which is written to HDFS file system
        String fileName = "hello1.csv";
        String fileContent = "hello;world";
        Path path = new Path("/" + fileName);

        // Write the string to the specified file
        hdfsRule.writeString(path, fileContent);

        // Read the file
        String retreived = hdfsRule.readString(path);

        // Check whether the retreived content is the same as what is written before
        assertEquals(fileContent, retreived);
    }

    @Test
    public void testReadWriteViaFileSystem() throws IOException {
        // The file name and the content which is written to HDFS file system
        String fileName = "hello2.csv";
        String fileContent = "hello;world";
        Path path = new Path("/" + fileName);

        // Get the file system from HDFS rule
        FileSystem fs = hdfsRule.getFileSystem();

        // Write file to HDFS
        FSDataOutputStream outputStream = fs.create(path);
        outputStream.writeBytes(fileContent);
        outputStream.close();

        // Read file from HDFS
        FSDataInputStream inputStream = fs.open(path);
        String retreived = IOUtils.toString(inputStream);
        inputStream.close();

        // Close the file system.
        fs.close();

        // Check whether the retreived content is the same as what is written before
        assertEquals(fileContent, retreived);
    }

    @Test
    public void testReadWriteFromScratch() throws IOException {
        // The file name and content which is written and read to and from HDFS
        String fileName = "hello3.csv";
        String fileContent = "hello;world";
        Path path = new Path("/" + fileName);

        // Create an HDFS filesystem
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", hdfsRule.getUri().toString());
        FileSystem fs = FileSystem.get(configuration);

        // Write the file to HDFS
        FSDataOutputStream outputStream = fs.create(path);
        outputStream.writeBytes(fileContent);
        outputStream.close();

        // Read the file from HDFS
        FSDataInputStream inputStream = fs.open(path);
        String retreived = IOUtils.toString(inputStream);
        inputStream.close();

        // Close the file system.
        fs.close();

        // Check whether the retreived content is the same as what is written before
        assertEquals(fileContent, retreived);
    }
}