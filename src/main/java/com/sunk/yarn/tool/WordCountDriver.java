package com.sunk.yarn.tool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Arrays;

/**
 * @author sunk
 * @since 2023/1/6
 **/
public class WordCountDriver {

    public static void main(String[] args) throws Exception {
        // 创建配置
        final Configuration conf = new Configuration();

        // 参数校验
        Tool tool;
        if (args[0].equals("wordcount")) {
            tool = new WordCount();
        } else {
            throw new RuntimeException("no such tool " + args[0]);
        }

        // 执行程序
        ToolRunner.run(conf, tool, Arrays.copyOfRange(args, 1, args.length));
    }
}
