package com.sunk.mapreduce.combiner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
 * Mapper
 * - MapKeyIn
 * - MapValueIn
 * - MapKeyOut
 * - MapValueOut
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private final Text text = new Text();
    private final LongWritable longWritable = new LongWritable(1);


    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {

        // 对 value 进行处理
        final String valueStr = value.toString();
        final String[] strings = valueStr.split(" ");

        // 循环写出
        for (String string : strings) {
            text.set(string);
            context.write(text, longWritable);
        }

    }

}
