package com.sunk.mapreduce.combiner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {

    private final LongWritable outValue = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {

        int sum = 0;

        for (LongWritable value : values) {
            sum += value.get();
        }

        outValue.set(sum);
        context.write(key, outValue);
    }
}
