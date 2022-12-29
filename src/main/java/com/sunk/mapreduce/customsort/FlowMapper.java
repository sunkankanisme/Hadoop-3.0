package com.sunk.mapreduce.customsort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
    private final FlowBean outKey = new FlowBean();
    private final Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FlowBean, Text>.Context context) throws IOException, InterruptedException {
        final String string = value.toString();
        final String[] strings = string.split("\t");

        // 封装输出
        outValue.set(strings[0]);

        outKey.setUpFlow(Long.valueOf(strings[1]));
        outKey.setDownFlow(Long.valueOf(strings[2]));
        outKey.setSumFlow();

        context.write(outKey, outValue);

    }

}
