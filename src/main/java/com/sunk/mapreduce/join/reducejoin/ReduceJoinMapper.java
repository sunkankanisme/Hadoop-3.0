package com.sunk.mapreduce.join.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author sunk
 * @since 2023/1/3
 **/
public class ReduceJoinMapper extends Mapper<LongWritable, Text, Text, TableBean> {

    private String fileName;
    private final Text outKey = new Text();
    private final TableBean outValue = new TableBean();

    /*
     * 初始化方法，区分输入的两个文件
     */
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, TableBean>.Context context) throws IOException, InterruptedException {
        final FileSplit inputSplit = (FileSplit) context.getInputSplit();
        fileName = inputSplit.getPath().getName();

    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, TableBean>.Context context) throws IOException, InterruptedException {
        // 1 获取一行
        final String line = value.toString();

        // 2 判断是哪个文件
        if (fileName.contains("order")) {
            // 订单表
            final String[] strings = line.split("\t");

            // 封装 kv
            outKey.set(strings[1]);
            outValue.setId(strings[0]);
            outValue.setPid(strings[1]);
            outValue.setAmount(Integer.parseInt(strings[2]));
            outValue.setPname("");
            outValue.setFlag("order");
        } else {
            // 品牌表
            final String[] strings = line.split("\t");

            // 封装 kv
            outKey.set(strings[0]);
            outValue.setId("");
            outValue.setPid(strings[0]);
            outValue.setAmount(0);
            outValue.setPname(strings[1]);
            outValue.setFlag("pd");
        }

        context.write(outKey, outValue);
    }
}
