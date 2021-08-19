package com.shz.hadoop.bigdata.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class WdReducer extends Reducer<WdKey, IntWritable, Text, IntWritable> {
    Text outputKey = new Text();
    IntWritable outputValue = new IntWritable();

    @Override
    protected void reduce(WdKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<IntWritable> iter = values.iterator();
        boolean firstLine = true;
        int day = 0;
        while (iter.hasNext()) {
            IntWritable wd = iter.next();
            if (firstLine) {
                outputKey.set(key.getYear() + "-" + key.getMonth() + "-" + key.getDay());
                outputValue.set(key.getWd());
                context.write(outputKey, outputValue);
                firstLine = false;
                day = key.getDay();
            }

            if (!firstLine && key.getDay() != day) {
                outputKey.set(key.getYear() + "-" + key.getMonth() + "-" + key.getDay());
                outputValue.set(key.getWd());
                context.write(outputKey, outputValue);
                break;
            }
        }
    }
}
