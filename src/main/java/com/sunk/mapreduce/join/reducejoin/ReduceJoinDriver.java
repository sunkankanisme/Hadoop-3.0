package com.sunk.mapreduce.join.reducejoin;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author sunk
 * @since 2023/1/3
 **/
public class ReduceJoinDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        final Job job = Job.getInstance();

        job.setJarByClass(ReduceJoinDriver.class);
        job.setMapperClass(ReduceJoinMapper.class);
        job.setReducerClass(ReduceJoinReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableBean.class);
        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, "D:\\workspace\\Java\\Sunk\\Hadoop-3.0\\src\\main\\resources\\join");
        FileOutputFormat.setOutputPath(job, new Path("D:\\System\\Desktop\\mapReduce\\" + System.currentTimeMillis()));

        job.waitForCompletion(true);

    }

}
