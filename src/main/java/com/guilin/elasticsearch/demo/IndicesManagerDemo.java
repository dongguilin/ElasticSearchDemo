package com.guilin.elasticsearch.demo;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.alias.exists.AliasesExistResponse;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsResponse;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryResponse;
import org.elasticsearch.action.admin.indices.warmer.delete.DeleteWarmerResponse;
import org.elasticsearch.action.admin.indices.warmer.put.PutWarmerResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by guilin1 on 16/5/28.
 * 索引管理API
 */
public class IndicesManagerDemo extends Base {

    /**
     * 索引存在API
     */
    @Test
    public void testExists() {
        IndicesAdminClient adminClient = esClient.admin().indices();

        IndicesExistsResponse response = adminClient.prepareExists("aleiye-0-2015-12-02-1")
                .execute().actionGet();
        System.out.println(response.isExists());
    }

    /**
     * 类型存在API
     */
    @Test
    public void testTypesExists() {
        TypesExistsResponse response = esClient.admin().indices()
                .prepareTypesExists("aleiye-0-2015-12-02-1")
                .setTypes("Nginx")
                .execute().actionGet();

        System.out.println(response.isExists());
    }

    /**
     * 索引统计API
     * 可以提供关于索引、文档、存储以及操作的信息，如获取、查询、索引、预热器、合并过程、清空缓冲区、刷新等
     */
    @Test
    public void testStats() {
        IndicesStatsResponse response = esClient.admin().indices()
                .prepareStats("aleiye-0-2015-12-02-1")
                .all()
                .execute().actionGet();

        System.out.println(response);
    }

    /**
     * 索引段信息API
     * 返回指定索引段的低层次信息
     */
    @Test
    public void testStatus() {
        IndicesSegmentResponse response = esClient.admin().indices()
                .prepareSegments("aleiye-0-2015-12-02-1")
                .execute().actionGet();

        System.out.println(response.getSuccessfulShards());
    }

    /**
     * 创建索引API
     *
     * @throws IOException
     */
    @Test
    public void testIndices() throws IOException {

        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("news")
                .startObject("properties")
                .startObject("title")
                .field("analyzer", "whitespace")
                .field("type", "string")
                .endObject()
                .endObject()
                .endObject()
                .endObject();

        CreateIndexResponse response = esClient.admin().indices()
                .prepareCreate("news")
                .setSettings(Settings.settingsBuilder().put("number_of_shards", 1))
                .addMapping("news", builder)
                .execute().actionGet();
    }


    /**
     * 删除索引API
     */
    @Test
    public void testDelete() {
        DeleteIndexResponse response = esClient.admin().indices()
                .prepareDelete("news")
                .execute().actionGet();
    }

    /**
     * 关闭索引
     */
    @Test
    public void testClose() {
        CloseIndexResponse response = esClient.admin().indices()
                .prepareClose("aleiye-0-2015-12-02-1")
                .execute().actionGet();
    }

    /**
     * 打开索引
     */
    @Test
    public void testOpen() {
        OpenIndexResponse response = esClient.admin().indices()
                .prepareOpen("aleiye-0-2015-12-02-1")
                .execute().actionGet();
    }

    /**
     * 刷新API
     */
    @Test
    public void testRefresh() {
        RefreshResponse response = esClient.admin().indices()
                .prepareRefresh("aleiye-0-2015-12-02-1")
                .execute().actionGet();
    }

    /**
     * 清空缓冲区API
     */
    @Test
    public void testFlush() {
        FlushResponse response = esClient.admin().indices()
                .prepareFlush("aleiye-0-2015-12-02-1")
                .setForce(false)
                .execute().actionGet();
    }


    /**
     * 设置映射API
     *
     * @throws IOException
     */
    @Test
    public void testPutMapping() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("news")
                .startObject("properties")
                .startObject("title")
                .field("analyzer", "whitespace")
                .field("type", "string")
                .endObject()
                .endObject()
                .endObject()
                .endObject();

        esClient.admin().indices()
                .preparePutMapping("news")
                .setType("news")
                .setSource(builder)
                .execute().actionGet();
    }


    /**
     * 别名API
     */
    @Test
    public void testAlias() {
        IndicesAliasesResponse response = esClient.admin().indices().prepareAliases()
                .addAlias("news", "n")
//                .addAlias("library","elastic_books",FilterBuilders.termFilter("title","elasticsearch"))
                .removeAlias("news", "current_news")
                .execute().actionGet();
    }

    /**
     * 获取别名API
     */
    @Test
    public void testGetAlias() {
        GetAliasesResponse response = esClient.admin().indices()
                .prepareGetAliases("elastic_books", "process*")
                .execute().actionGet();
        System.out.println(response.getAliases());
    }

    /**
     * 别名存在API
     */
    @Test
    public void testAliasExist() {
        AliasesExistResponse response = esClient.admin().indices()
                .prepareAliasesExist("process*")
                .execute().actionGet();
        System.out.println(response.exists());
    }

    /**
     * 清空缓存API
     */
    @Test
    public void testClearCache() {
        ClearIndicesCacheResponse response = esClient.admin().indices()
                .prepareClearCache("aleiye*")
                .setFieldDataCache(true)
                .setFields("title")
                .execute().actionGet();
    }

    /**
     * 更新设置API
     */
    @Test
    public void testUpdateSettings() {
        UpdateSettingsResponse response = esClient.admin().indices()
                .prepareUpdateSettings("news")
                .setSettings(Settings.builder().put("index.number_of_replicas", 2))
                .execute().actionGet();
    }

    /**
     * 分析API
     * 查看指定分析器、分词器和过滤器的分析处理过程时非常有用
     */
    @Test
    public void testAnalyze() {
        AnalyzeResponse response = esClient.admin().indices()
                .prepareAnalyze("aleiye-0-2015-08-09-1", "hello world")
                .setTokenizer("whitespace")
//                .setTokenFilters("nGram")
                .execute().actionGet();
        System.out.println(response.detail());
    }

    /**
     * 设置模板API
     */
    @Test
    public void testPutTemplate() throws IOException {
        PutIndexTemplateResponse response = esClient.admin().indices()
                .preparePutTemplate("my_template")
                .setTemplate("product*")
                .setSettings(Settings.builder()
                        .put("index.number_of_replicas", 2).put("index.number_of_shards", 1))
                .addMapping("item", XContentFactory.jsonBuilder()
                                .startObject()
                                .startObject("item")
                                .startObject("properties")
                                .startObject("title")
                                .field("type", "string")
                                .endObject()
                                .endObject()
                                .endObject()
                                .endObject()
                ).execute().actionGet();
    }

    /**
     * 删除模板API
     */
    @Test
    public void testDeleteTemplate() {
        DeleteIndexTemplateResponse response = esClient.admin().indices()
                .prepareDeleteTemplate("my_*")
                .execute().actionGet();
    }

    /**
     * 查询验证API
     * 可以用来检查发送给ElasticSearch的查询是否合法有效
     *
     * @throws Exception
     */
    @Test
    public void testValidateQuery() throws Exception {
        ValidateQueryResponse response = esClient.admin().indices()
                .prepareValidateQuery("aleiye*")
                .setExplain(true)//可以返回查询中发生错误的确切位置
                .setQuery(QueryBuilders.existsQuery("zhang san"))
                .execute().actionGet();
        System.out.println(response.isValid());
    }

    /**
     * 设置预热器API
     */
    @Test
    public void testPutWarmer() {
        PutWarmerResponse response = esClient.admin().indices()
                .preparePutWarmer("aleiye_warmer")//预热器名称
                .setSearchRequest(esClient.prepareSearch("aleiye*"))
                .execute().actionGet();
    }

    /**
     * 删除预热器API
     */
    @Test
    public void testDeleteWarmer() {
        DeleteWarmerResponse response = esClient.admin().indices()
                .prepareDeleteWarmer()
                .setNames("aleiye*")
                .execute().actionGet();
    }

    /**
     * 获取Mapping
     */
    @Test
    public void testGetMappings() {
        GetMappingsResponse response = esClient.admin().indices()
                .prepareGetMappings()
                .execute().actionGet();

        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = response.getMappings();
        Object[] keysArr = mappings.keys().toArray();
        Object[] valuesArr = mappings.values().toArray();

        for (int i = 0; i < keysArr.length; i++) {
            String index = (String) keysArr[i];
            System.out.println(index);
            if (!index.endsWith("1")) {
                continue;
            }
            ImmutableOpenMap<String, MappingMetaData> map = (ImmutableOpenMap<String, MappingMetaData>) valuesArr[i];
            Iterator<MappingMetaData> iterator1 = map.valuesIt();

            while (iterator1.hasNext()) {
                MappingMetaData metaData = iterator1.next();
                System.out.println(metaData.type());
            }

        }
    }


}
