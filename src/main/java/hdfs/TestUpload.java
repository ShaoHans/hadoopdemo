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
        // 根据配置文件信息获取HDFS分布式文件系统，使用账号root
        fs = FileSystem.get(URI.create("hdfs://mycluster/"), conf, "root");
        // 打开本项目根目录下的test.txt文件，如果没有该文件，可以通过GenerateFile.java生成
        BufferedInputStream fis = new BufferedInputStream(new FileInputStream("test.txt"));
        // 在HDFS集群中创建 /hadoop/data.txt 路径信息
        Path out = getFilePath("/hadoop/data.txt");
        // 根据创建出来的路径信息打开一个文件数据输出流
        FSDataOutputStream fsDataOutputStream = fs.create(out);
        // 把本地文件的字节流写入HDFS的输出流中
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

