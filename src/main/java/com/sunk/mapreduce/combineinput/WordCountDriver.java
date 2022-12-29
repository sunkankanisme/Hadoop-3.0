package com.sunk.mapreduce.combineinput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        // 1 获取 job
        final Configuration conf = new Configuration();
        final Job job = Job.getInstance(conf);

        // 2 设置 jar 路径
        job.setJarByClass(WordCountDriver.class);

        // 3 关联 Mapper，Reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 4 设置 Mapper 输出的 kv 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // 5 设置最终输出的 kv 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // 设置 combineTextInputFormat
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job, 20971520);

        // 6 设置输入输出路径
        FileInputFormat.setInputPaths(job, "D:\\workspace\\my\\Hadoop-3.0\\src\\main\\resources\\small");
        FileOutputFormat.setOutputPath(job, new Path("D:\\system\\desktop\\mapreduce"));

        // 7 提交 job
        job.waitForCompletion(true);
    }

}
