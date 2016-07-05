package com.guilin.elasticsearch.demo;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Maps;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.cluster.routing.allocation.command.CancelAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.shard.ShardId;
import org.junit.Test;

import java.util.Map;

/**
 * Created by guilin1 on 16/5/28.
 * 集群管理API
 */
public class ClusterManagerDemo extends Base {

    @Test
    public void testBuilder() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
        builder.startObject();
        builder.field("name", "张三");
        builder.field("age", 12);
        builder.startArray("friends");
        builder.array("names", "小明", "小红");
        builder.endArray();
        builder.endObject();
        System.out.println(builder.string());
    }

    /**
     * 集群和索引健康状态API
     * （如集群状态、已分配分片数、总分片数、特定索引的副本数等信息）
     */
    @Test
    public void testHealth() {
        ClusterAdminClient adminClient = esClient.admin().cluster();

        //整个集群的状态
        ClusterHealthResponse response = adminClient.prepareHealth()
                .execute().actionGet();

        System.out.println(response);


        System.out.println(adminClient.prepareHealth("aleiye*").execute().actionGet());

        System.out.println(adminClient.prepareHealth("fault*").execute().actionGet());

        //指定索引
        System.out.println(adminClient.prepareHealth("aleiye-0-2015-12-02-1").execute().actionGet());

    }

    /**
     * 集群状态API
     * （如路由、分片分配情况以及映射）
     */
    @Test
    public void testClusterState() {
        ClusterAdminClient adminClient = esClient.admin().cluster();

        ClusterStateResponse response = adminClient.prepareState().execute().actionGet();

        System.out.println(response.getState());
    }

    /**
     * 设置更新API
     * （分持久化的设置和非持久化的设置）
     */
    @Test
    public void testUpdateSetting() {

        Map<String, Object> map = Maps.newHashMap();

        map.put("indices.ttl.interval", "10m");
        map.put("cluster.name", "guilin");

        ClusterUpdateSettingsResponse response = esClient.admin().cluster()
                .prepareUpdateSettings()
                .setTransientSettings(map)
                .execute().actionGet();

        System.out.println(response.getTransientSettings().toDelimitedString(' '));
    }

    /**
     * 重新路由API
     * （可以在节点间移动分片，以及取消或强制进行分片分配行为）
     */
    @Test
    public void testReroute() {
        ClusterRerouteResponse response = esClient.admin().cluster()
                .prepareReroute()
                .setDryRun(true)//该方法能阻止分配命令的运行，仅仅检查给定命令的可行性
                .setExplain(true)
                .add(new MoveAllocationCommand(new ShardId("aleiye-0-2015-12-02-1", 0), "xeXdIWVmRYav0WVfPDnNCQ", "xeXdIWVmRYav0WVfPDnNCQ"),
                        new CancelAllocationCommand(new ShardId("aleiye-0-2015-12-02-1", 1), "xeXdIWVmRYav0WVfPDnNCQ", true))
                .execute().actionGet();

        System.out.println(response.getState());
    }


    /**
     * 节点信息API
     * （输出信息涵盖Java虚拟机、操作系统以及网络(如IP地址或局域网地址以及插件信息等)）
     */
    @Test
    public void testNodeInfo() {
        NodesInfoResponse response = esClient.admin().cluster()
                .prepareNodesInfo()
                .all()
                .execute().actionGet();

        System.out.println(response);
    }


    /**
     * 节点统计API
     * （输出如索引统计、文件系统、HTTP模块、Java虚拟机等）
     */
    @Test
    public void testNodeStats() {
        NodesStatsResponse response = esClient.admin().cluster()
                .prepareNodesStats("xeXdIWVmRYav0WVfPDnNCQ")
                .setIndices(true)
                .setScript(true)
                .execute()
                .actionGet();

        System.out.println(response);
    }

    /**
     * 节点热点线程API
     * （用于在Elasticsearch出故障或CPU使用率超过正常值时检查节点状态）
     */
    @Test
    public void testNodesHotThreads() {
        NodesHotThreadsResponse response = esClient.admin().cluster()
                .prepareNodesHotThreads()
                .execute()
                .actionGet();
        System.out.println(response.getNodes()[0].getHotThreads());
    }

    /**
     * 查询分片API
     * （允许我们检查哪些节点将用于处理查询）
     */
    @Test
    public void testSearchShards() {
        ClusterSearchShardsResponse response = esClient.admin().cluster()
                .prepareSearchShards()
                .setIndices("aleiye*")
                .execute().actionGet();
        System.out.println(JSON.toJSONString(response.getGroups()));
    }


}
