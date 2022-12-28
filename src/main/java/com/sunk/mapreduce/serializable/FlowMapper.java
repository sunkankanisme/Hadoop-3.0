package com.sunk.mapreduce.serializable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    private final Text outKey = new Text();
    private final FlowBean outValue = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        // 获取一行 [ 1	13736230513	192.196.100.1	www.atguigu.com	2481	24681	200 ]
        final String[] strings = value.toString().split("\t");

        // 封装数据输出
        final String phoneNumber = strings[1];
        final String upFlow = strings[strings.length - 3];
        final String downFlow = strings[strings.length - 2];

        outKey.set(phoneNumber);
        outValue.setUpFlow(Long.parseLong(upFlow));
        outValue.setDownFlow(Long.parseLong(downFlow));
        outValue.setSumFlow();

        context.write(outKey, outValue);
    }
}
