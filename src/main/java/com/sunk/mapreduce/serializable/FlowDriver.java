package com.sunk.mapreduce.serializable;

import com.sunk.mapreduce.partitioner.ProvincePartitioner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        final Job job = Job.getInstance();

        job.setJarByClass(FlowDriver.class);

        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job, "D:\\workspace\\my\\Hadoop-3.0\\src\\main\\resources\\phone_data.txt");
        FileOutputFormat.setOutputPath(job, new Path("D:\\system\\desktop\\mapreduce"));

        /*
         * 使用自定义的分区器
         */
        job.setPartitionerClass(ProvincePartitioner.class);

        /*
         * 设置 Reduce 个数
         */
        job.setNumReduceTasks(5);

        job.waitForCompletion(true);
    }

}
