package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;


public class TestUpload {
    static Configuration conf;
    static FileSystem fs;

    public static void main(String[] args) throws Exception {
        conf = new Configuration();
        // 设置块大小为1M
        conf.set("dfs.block.size", "1048576");
        fs = FileSystem.get(URI.create("hdfs://mycluster/"), conf, "root");
        BufferedInputStream fis = new BufferedInputStream(new FileInputStream("test.txt"));
        Path out = getFilePath("/hadoop/data.txt");
        FSDataOutputStream fsDataOutputStream = fs.create(out);
        IOUtils.copyBytes(fis, fsDataOutputStream, conf, true);
    }

    public static Path getFilePath(String filePath) throws IOException {
        Path path = new Path(filePath);
        if (fs.exists(path)) {
            fs.delete(path, true);
        }
        return path;
    }
}

