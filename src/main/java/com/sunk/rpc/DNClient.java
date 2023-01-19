package com.sunk.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * @author sunk
 * @since 2023/1/19
 **/
public class DNClient {

    public static void main(String[] args) throws IOException {
        // 获取客户端代理
        final RPCProtocol proxy = RPC.getProxy(RPCProtocol.class, RPCProtocol.versionID, new InetSocketAddress("localhost", 9999), new Configuration());

        System.out.println("客户端开始工作");
        proxy.mkdirs("/input");
    }

}
