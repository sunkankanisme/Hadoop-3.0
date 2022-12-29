package com.sunk.mapreduce.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LogOutputFormatter extends FileOutputFormat<Text, NullWritable> {
    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        return new LogRecordWriter(job);
    }

    private static class LogRecordWriter extends RecordWriter<Text, NullWritable> {

        private final FSDataOutputStream stream1;
        private final FSDataOutputStream stream2;

        public LogRecordWriter(TaskAttemptContext job) {
            // 创建 2 条流
            try {
                final FileSystem fileSystem = FileSystem.get(job.getConfiguration());

                stream1 = fileSystem.create(new Path("D:\\system\\desktop\\mapreduce\\output-atguigu.log"));
                stream2 = fileSystem.create(new Path("D:\\system\\desktop\\mapreduce\\output-other.log"));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }

        @Override
        public void write(Text key, NullWritable value) throws IOException, InterruptedException {
            final String url = key.toString();

            if (url.contains("atguigu")) {
                stream1.writeBytes(url + "\n");
            } else {
                stream2.writeBytes(url + "\n");
            }
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            stream1.close();
            IOUtils.closeStream(stream2);
        }
    }
}
