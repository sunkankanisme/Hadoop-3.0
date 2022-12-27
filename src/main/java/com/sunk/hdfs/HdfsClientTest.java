package com.sunk.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

/*
 * 客户端代码套路
 * 1 获取客户端对象
 * 2 执行相关操作
 * 3 关闭资源
 */
public class HdfsClientTest {

    private FileSystem fileSystem;

    @Before
    public void init() throws Exception {
        final URI uri = new URI("hdfs://fmidcbackup06:8020");
        final Configuration configuration = new Configuration();
        configuration.set("dfs.replications", "1");

        fileSystem = FileSystem.get(uri, configuration, "bigdata");
    }

    @After
    public void close() throws IOException {
        fileSystem.close();
    }

    @Test
    public void testMkdir() throws IOException {
        final boolean b = fileSystem.mkdirs(new Path("/api_test"));
        System.out.println(b);
    }

    @Test
    public void testUpload() throws IOException {
        final Path src = new Path("D:\\workspace\\my\\Hadoop-3.0\\pom.xml");
        final Path dst = new Path("/api_test/pom.xml");
        fileSystem.copyFromLocalFile(false, true, src, dst);
    }

    @Test
    public void testDownload() throws IOException {
        final Path dst = new Path("D:\\system\\desktop\\pom.xml");
        final Path src = new Path("/api_test/pom.xml");
        fileSystem.copyToLocalFile(false, src, dst, false);
    }

    @Test
    public void testDelete() throws IOException {
        final boolean delete = fileSystem.delete(new Path("/api_test/hdp.sh"));
        System.out.println(delete);
    }

    @Test
    public void testRename() throws IOException {
        final Path src = new Path("/api_test/pom.xml");
        final Path dst = new Path("/api_test/pom-2.xml");
        final boolean rename = fileSystem.rename(src, dst);
        System.out.println(rename);
    }

    @Test
    public void testDetail() throws Exception {
        final RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path("/api_test"), false);
        while (files.hasNext()) {
            final LocatedFileStatus fileStatus = files.next();

            System.out.println("== getName: " + fileStatus.getPath().getName());
            System.out.println("== getPath: " + fileStatus.getPath());
            System.out.println("== getPermission: " + fileStatus.getPermission());
            System.out.println("== getOwner: " + fileStatus.getOwner());
            System.out.println("== getLen: " + fileStatus.getLen());
            System.out.println("== getModificationTime: " + fileStatus.getModificationTime());
            System.out.println("== getReplication: " + fileStatus.getReplication());

            System.out.println("=================");
            final BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                System.out.println(blockLocation);
            }
        }
    }

    @Test
    public void testIter() throws Exception {
        final FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isFile()) {
                final Path path = fileStatus.getPath();
                System.out.println("---- " + path);
            } else {
                final Path path = fileStatus.getPath();
                System.out.println("==== " + path);
            }
        }

    }

}