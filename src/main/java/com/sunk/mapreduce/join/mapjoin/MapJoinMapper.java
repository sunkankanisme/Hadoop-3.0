package com.sunk.mapreduce.join.mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

/**
 * @author sunk
 * @since 2023/1/3
 **/
public class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private final HashMap<String, String> pdMap = new HashMap<>();
    private final Text outputKey = new Text();

    /*
     * 获取缓存的文件，并将内容封装到集合
     */
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        final URI[] cacheFiles = context.getCacheFiles();

        final FileSystem fs = FileSystem.get(context.getConfiguration());
        final FSDataInputStream fis = fs.open(new Path(cacheFiles[0]));

        // 从流中读取数据
        final BufferedReader reader = new BufferedReader(new InputStreamReader(fis, StandardCharsets.UTF_8));

        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())) {
            final String[] strings = line.split("\t");
            pdMap.put(strings[0], strings[1]);
        }

        IOUtils.closeStream(reader);
    }

    /*
     * 在 map 中进行join
     */
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        final String[] fields = value.toString().split("\t");

        final String pid = fields[1];
        final String pname = pdMap.get(pid);
        final String id = fields[0];
        final String amount = fields[2];

        // 封装输出
        outputKey.set(id + "\t" + pname + "\t" + amount);
        context.write(outputKey, NullWritable.get());
    }
}
