package com.sunk.mapreduce.etl;

import org.apache.hadoop.conf.Configuration;
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
public class EtlDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(EtlDriver.class);
        job.setMapperClass(EtlMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(0);
        FileInputFormat.setInputPaths(job, new Path("D:\\workspace\\Java\\Sunk\\Hadoop-3.0\\src\\main\\resources\\etl\\web.log"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\System\\Desktop\\mapReduce\\" + System.currentTimeMillis()));

        job.waitForCompletion(true);
    }
}
