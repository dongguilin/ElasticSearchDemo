package com.guilin.elasticsearch.demo;

import com.google.common.collect.Sets;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.plugin.deletebyquery.DeleteByQueryPlugin;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Set;

/**
 * Created by guilin1 on 16/3/17.
 */
public class Base {

    protected static Client esClient;

    @BeforeClass
    public static void beforeClass() {
        Settings settings = Settings.settingsBuilder()
                .put("client.transport.ping.timeout", 1000 * 60)
                        //指定集群名称
                .put("cluster.name", "elasticsearch")
                        //探测集群中机器状态
                .put("client.transport.sniff", true)
                .build();

        Set<String> eshost = Sets.newHashSet("localhost");
        int esport = 9300;

        InetSocketTransportAddress[] address = new InetSocketTransportAddress[eshost.size()];
        int i = 0;
        for (String host : eshost) {
            try {
                address[i] = new InetSocketTransportAddress(InetAddress.getByName(host), esport);
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
            i++;
        }
        esClient = TransportClient.builder().settings(settings).addPlugin(DeleteByQueryPlugin.class).build().addTransportAddresses(address);
    }


    @AfterClass
    public static void afterClass() {
        if (esClient != null) {
            esClient.close();
        }
    }
}
