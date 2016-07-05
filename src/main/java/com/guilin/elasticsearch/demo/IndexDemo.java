package com.guilin.elasticsearch.demo;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static junit.framework.Assert.assertEquals;

/**
 * Created by guilin1 on 16/3/21.
 */
public class IndexDemo extends Base {

    @Test
    public void test1(){
        IndicesStatsResponse isr = esClient.admin().indices()
                .prepareExecute(IndicesStatsAction.INSTANCE).execute()
                .actionGet();

//        System.out.println(isr);

        // 结构：aleiye 2015-4-06 0 1232434
        Map<String, Map<String, Map<Integer, Long>>> maps = Maps.newHashMap();

        Map<String, IndexStats> mapIS = isr.getIndices();
        if (mapIS != null) {
            for (Map.Entry<String, IndexStats> entry : mapIS.entrySet()) {
                String indexName = entry.getKey();

                if (indexName.split("-").length <= 1||indexName.startsWith("stat")) {
                    continue;
                }

                String indexPrefix = indexName.split("-")[0];

                IndexStats is = entry.getValue();//索引
                long docNumber = is.getPrimaries().docs.getCount();//索引文档数量
                String date = StringUtils.substring(indexName,
                        indexName.length() - 10, indexName.length());//索引日期
                Integer seq = Integer.parseInt(StringUtils.substring(indexName,
                        indexPrefix.length() + 1, indexName.length() - 11));//索引分块编号

                //获取相应索引下 日期->(分块编号->文档数量) 映射
                Map<String, Map<Integer, Long>> m2 = maps.get(indexPrefix);
                if (m2 == null) {
                    m2 = Maps.newHashMap();
                    maps.put(indexPrefix, m2);
                }

                //分块编号->文档数量
                Map<Integer, Long> m3 = m2.get(date);
                if (m3 == null) {
                    m3 = Maps.newHashMap();
                    m2.put(date, m3);
                }

                if (m3.size() > 0) {
                    //索引分块编号集合
                    Set<Integer> setKeys = m3.keySet();
                    for (Integer seqint : setKeys) {
                        if (seqint < seq)
                            m3.put(seq, docNumber);
                        break;
                    }
                } else {
                    m3.put(seq, docNumber);
                }
            }

        }

        System.out.println(JSON.toJSONString(maps));

    }

    private static final String EXPECTED_SOURCE = "{\"SomeKey\":\"SomeValue\"}";


    @Test
    public void test2() throws IOException {
        IndexRequestBuilder indexRequestBuilder = new IndexRequestBuilder(esClient, IndexAction.INSTANCE);
        Map<String, String> source = new HashMap<>();
        source.put("SomeKey", "SomeValue");
        indexRequestBuilder.setSource(source);
        assertEquals(EXPECTED_SOURCE, XContentHelper.convertToJson(indexRequestBuilder.request().source(), true));
    }

    @Test
    public void testCreateIndex(){
        esClient.admin().indices().prepareCreate("twitter").get();
    }

    /**
     *create index with settings
     */
    @Test
    public void testCreateIndexWithSetting(){
        esClient.admin().indices().prepareCreate("twitter3")
                .setSettings(Settings.builder()
                                .put("index.number_of_shards", 3)
                                .put("index.number_of_replicas", 2)
                )
                .get();
    }

    /**
     * create index with mapping
     */
    @Test
    public void testCreateIndexWithMapping(){
        esClient.admin().indices().prepareCreate("twitter4")
                .addMapping("tweet", "{\n" +
                        "    \"tweet\": {\n" +
                        "      \"properties\": {\n" +
                        "        \"message\": {\n" +
                        "          \"type\": \"string\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }")
                .get();
    }

    /**
     * put mapping
     */
    @Test
    public void testAddMapping(){
        esClient.admin().indices().preparePutMapping("twitter")
                .setType("user")
                .setSource("{\n" +
                        "  \"properties\": {\n" +
                        "    \"name\": {\n" +
                        "      \"type\": \"string\"\n" +
                        "    }\n" +
                        "  }\n" +
                        "}")
                .get();

        // You can also provide the type in the source document
        esClient.admin().indices().preparePutMapping("twitter")
                .setType("user")
                .setSource("{\n" +
                        "    \"user\":{\n" +
                        "        \"properties\": {\n" +
                        "            \"name\": {\n" +
                        "                \"type\": \"string\"\n" +
                        "            }\n" +
                        "        }\n" +
                        "    }\n" +
                        "}")
                .get();
    }

    @Test
    public void test3(){



    }

}
