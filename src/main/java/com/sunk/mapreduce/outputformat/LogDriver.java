package com.sunk.mapreduce.outputformat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LogDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        final Job job = Job.getInstance();

        job.setJarByClass(LogDriver.class);

        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        /*
         * 设置 output formatter
         */
        job.setOutputFormatClass(LogOutputFormatter.class);

        FileInputFormat.setInputPaths(job, "D:\\workspace\\my\\Hadoop-3.0\\src\\main\\resources\\log.txt");
        // 此处只输出 _SUCCESS 文件
        FileOutputFormat.setOutputPath(job, new Path("D:\\system\\desktop\\mapreduce\\" + System.currentTimeMillis()));

        job.waitForCompletion(true);
    }

}
