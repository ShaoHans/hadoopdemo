package com.shz.hadoop.bigdata.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.Properties;

public class TopN {
    public static void main(String[] args) throws Exception {
        Properties properties = System.getProperties();
        properties.setProperty("HADOOP_USER_NAME", "root");
        Configuration conf = new Configuration(true);
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("mapreduce.app-submission.cross-platform", "true");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = Job.getInstance(conf);
        job.setJar("C:\\work\\github\\hadoopdemo\\target\\hadoopdemo-1.0.jar");
        job.setJobName("TopN");

        //map task
        // 1.设置数据文件输入目录
        TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
        // 2.设置map计算后数据输出目录
        Path output = new Path(otherArgs[1]);
        if (output.getFileSystem(conf).exists(output)) {
            output.getFileSystem(conf).delete(output, true);
        }
        TextOutputFormat.setOutputPath(job, output);
        // 3.设置温度mapper计算类
        job.setMapperClass(WdMapper.class);
        // 4.设置map计算输出key类
        job.setMapOutputKeyClass(WdKey.class);
        // 5.设置map计算输出value类
        job.setMapOutputValueClass(IntWritable.class);
        // 6.设置分区器类
        job.setPartitionerClass(WdPartitioner.class);
        // 7.设置输出排序类
        job.setSortComparatorClass(WdSortComparator.class);

        //reduce task
        // 1.设置reducer计算类
        job.setReducerClass(WdReducer.class);
        // 2.设置分组排序类
        job.setGroupingComparatorClass(WdGroupingComparator.class);

        job.waitForCompletion(true);
    }
}
