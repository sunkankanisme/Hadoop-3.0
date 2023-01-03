package com.sunk.mapreduce.join.mapjoin;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author sunk
 * @since 2023/1/3
 **/
public class MapJoinDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        final Job job = Job.getInstance();

        job.setJarByClass(MapJoinDriver.class);
        job.setMapperClass(MapJoinMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        /*
         * 设置缓存
         */
        job.addCacheFile(new URI("file:///D:/workspace/Java/Sunk/Hadoop-3.0/src/main/resources/join/pd.txt"));

        /*
         * 设置 reducer 个数
         */
        job.setNumReduceTasks(0);


        FileInputFormat.setInputPaths(job, "D:\\workspace\\Java\\Sunk\\Hadoop-3.0\\src\\main\\resources\\join\\order.txt");
        FileOutputFormat.setOutputPath(job, new Path("D:\\System\\Desktop\\mapReduce\\" + System.currentTimeMillis()));

        job.waitForCompletion(true);

    }

}
