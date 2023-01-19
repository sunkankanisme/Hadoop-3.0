package com.sunk.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

/**
 * 实现通信接口
 *
 * @author sunk
 * @since 2023/1/19
 **/
public class NNServer implements RPCProtocol {

    public static void main(String[] args) throws IOException {
        final RPC.Server server = new RPC.Builder(new Configuration())
                .setBindAddress("localhost")
                .setPort(9999)
                .setProtocol(RPCProtocol.class)
                .setInstance(new NNServer())
                .build();

        System.out.println("服务器开始工作");

        server.start();
    }

    @Override
    public void mkdirs(String path) {
        System.out.println("服务器接收了客户端请求：" + path);
    }
}
