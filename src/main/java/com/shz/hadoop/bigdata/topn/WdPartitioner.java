package com.shz.hadoop.bigdata.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class WdPartitioner extends Partitioner<WdKey, IntWritable> {

    @Override
    public int getPartition(WdKey wdKey, IntWritable intWritable, int numPartitions) {
        return wdKey.getYear() % numPartitions;
    }
}
