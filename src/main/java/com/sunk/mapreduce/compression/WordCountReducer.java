package com.sunk.mapreduce.compression;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        // 累加单词量
        int cnt = 0;
        for (LongWritable value : values) {
            cnt += value.get();
        }

        context.write(key, new LongWritable(cnt));
    }

}
