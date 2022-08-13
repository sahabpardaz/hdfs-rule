# HDFS Rule
A JUnit rule for starting an HDFS server on the local machine.

## Sample Usage

```java
@ClassRule
public static final HdfsRule hdfsRule = new HdfsRule();

@Test
public void test() throws IOException {
    // The file name and the content which is written to HDFS file system
    String fileName = "hello.csv";
    String fileContent = "hello;world";
    Path path = new Path("/" + fileName);

    // Write the string to the specified file
    hdfsRule.writeString(path, fileContent);

    // Read the file
    String retreived = hdfsRule.readString(path);

    // Check whether the retreived content is the same as what is written before
    assertEquals(fileContent, retreived);
}
```

It is also possible to get the file system and construct input/output streams.

```java
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
```

Moreover, it is possible to create the file system using the HDFS URI.

```java
// Create an HDFS filesystem
Configuration configuration = new Configuration();
configuration.set("fs.defaultFS", hdfsRule.getUri().toString());
FileSystem fs = FileSystem.get(configuration);

...

// Close the file system.
fs.close();
```

## JUnit 5

You can declare HdfsExtension as follows and use it in your tests.

```java
@RegisterExtension
static final HdfsExtension hdfsExtension = new HdfsExtension();
```

## Add it to your project
You can refer to this library by either of java build systems (Maven, Gradle, SBT or Leiningen) using snippets from this jitpack link:
[![](https://jitpack.io/v/sahabpardaz/hdfs-rule.svg)](https://jitpack.io/#sahabpardaz/hdfs-rule)

JUnit 4 and 5 dependencies are marked as optional, so you need to provide JUnit 4 or 5 dependency
(based on what version you need, and you use) in you project to make it work.
