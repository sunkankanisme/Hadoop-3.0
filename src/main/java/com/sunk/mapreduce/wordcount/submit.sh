#!/bin/bash

# 向集群提交 mapReduce 任务
hadoop jar Hadoop-3.0-1.0-SNAPSHOT.jar com.sunk.mapreduce.wordcount.WordCountDriver "/input" "/output"
