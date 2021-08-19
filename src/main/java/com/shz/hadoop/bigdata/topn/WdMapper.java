package com.shz.hadoop.bigdata.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class WdMapper extends Mapper<LongWritable, Text, WdKey, IntWritable> {
    WdKey outputKey = new WdKey();
    IntWritable outputValue = new IntWritable();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 2020-11-18 12:22:00 12   30 => 2020-11-18/30/30
        String[] items = StringUtils.split(value.toString(), ',');
        try {
            Date date = sdf.parse(items[0]);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            outputKey.setYear(calendar.get(Calendar.YEAR));
            outputKey.setMonth(calendar.get(Calendar.MONTH) + 1);
            outputKey.setDay(calendar.get(Calendar.DAY_OF_MONTH));
            outputValue.set(Integer.parseInt(items[2]));
            outputKey.setWd(outputValue.get());

            context.write(outputKey, outputValue);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
