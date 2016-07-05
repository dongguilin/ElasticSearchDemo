package com.guilin.elasticsearch.demo;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang.RandomStringUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.filters.Filters;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.bucket.missing.Missing;
import org.elasticsearch.search.aggregations.bucket.nested.Nested;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.joda.time.DateTime;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

/**
 * Created by guilin1 on 16/3/17.
 */
public class AggregationDemo extends Base {

    String date = new DateTime("2016-03-10").toString("yyyy-MM-dd");
    String indexName = "aleiye-0-" + date;

    public static void main(String[] args) {
        System.out.println(RandomStringUtils.randomAlphabetic(5));
        System.out.println(RandomStringUtils.randomNumeric(2));
        System.out.println(RandomStringUtils.randomAscii(5));
        System.out.println(RandomStringUtils.randomAlphanumeric(5));
    }

    @Test
    public void testCreateData() throws IOException {
        BulkRequestBuilder bulkRequestBuilder = esClient.prepareBulk();

        for (int i = 0; i < 10; i++) {
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
                    .startObject("student")
                    .field("name").value(RandomStringUtils.randomAlphabetic(3))
                    .field("age").value(RandomStringUtils.randomNumeric(2))
                    .field("weight").value(RandomStringUtils.randomNumeric(2))
                    .field("year").value(2000 + Integer.parseInt(RandomStringUtils.randomNumeric(2)))
                    .field("isMale").value(new Random().nextBoolean())
                    .array("scores", RandomStringUtils.randomNumeric(2), RandomStringUtils.randomNumeric(2), RandomStringUtils.randomNumeric(2), RandomStringUtils.randomNumeric(2), RandomStringUtils.randomNumeric(2))
                    .array("friends", RandomStringUtils.randomAlphabetic(3), RandomStringUtils.randomAlphabetic(3), RandomStringUtils.randomAlphabetic(3), RandomStringUtils.randomAlphabetic(3))
                    .endObject()
                    .endObject();

            IndexRequestBuilder requestBuilder = esClient.prepareIndex(indexName, "1").setSource(builder.bytes());
            bulkRequestBuilder.add(requestBuilder);
        }

        BulkResponse response = bulkRequestBuilder.execute().actionGet();
        System.out.println("操作" + (response.hasFailures() ? "失败" : "成功") + " 影响记录数：" + response.getItems().length + " 花费：" + response.getTookInMillis() + "毫秒");
    }

    @Test
    public void test1() {

        SearchResponse sr = esClient.prepareSearch(indexName)
                .addAggregation(AggregationBuilders.min("min_weight").field("student.weight"))
                .addAggregation(AggregationBuilders.max("max_weight").field("student.weight"))
                .addAggregation(AggregationBuilders.avg("avg_weight").field("student.weight"))
                .addAggregation(AggregationBuilders.sum("sum_weight").field("student.weight"))
                .addAggregation(AggregationBuilders.count("count_weight").field("student.weight"))
                .execute().actionGet();

        System.out.println(JSON.toJSONString(sr.getAggregations()));

        sr = esClient.prepareSearch(indexName)
                .addAggregation(AggregationBuilders.stats("agg").field("student.weight"))
                .execute().actionGet();
        System.out.println(JSON.toJSONString(sr.getAggregations()));

        System.out.println(sr.getAggregations().asList().get(0).getProperty("min"));

        sr = esClient.prepareSearch(indexName)
                .addAggregation(AggregationBuilders.percentiles("agg").field("student.weight").percentiles(1.0, 5.0, 10.0, 20.0, 30.0, 75.0, 95.0, 99.0))
                .execute().actionGet();
        System.out.println(JSON.toJSONString(sr.getAggregations()));

    }

    @Test
    public void test2() {

        SearchResponse sr = esClient.prepareSearch(indexName)
                .addAggregation(AggregationBuilders
                        .terms("agg").field("student.d_date")
                        .subAggregation(
                                AggregationBuilders.topHits("top")
//                                        .setExplain(true)
//                                        .setSize(1)
//                                        .setFrom(10)
                        ))
                .execute().actionGet();
        System.out.println(JSON.toJSONString(sr.getAggregations()));
    }

    /**
     * Global Aggregation
     */
    @Test
    public void testGlobalAgg() {
        SearchRequestBuilder builder = esClient.prepareSearch(indexName);

        builder.addAggregation(
                AggregationBuilders.global("agg")
                        .subAggregation(AggregationBuilders.terms("studentName").field("student.name")));

        SearchResponse sr = builder.execute().actionGet();

        Global agg = sr.getAggregations().get("agg");
        System.out.println(agg.getDocCount());
    }


    /**
     * Filter Aggregation
     */
    @Test
    public void testFilterAgg() {
        SearchRequestBuilder builder = esClient.prepareSearch(indexName);
        builder.addAggregation(AggregationBuilders.filter("agg")
                .filter(QueryBuilders.termQuery("student.isMale", true)));//student.isMale=true的记录
        SearchResponse sr = builder.execute().actionGet();
        Filter agg = sr.getAggregations().get("agg");
        System.out.println(JSON.toJSONString(sr));
        System.out.println(JSON.toJSONString(sr.getAggregations().get("agg")));
        System.out.println(agg.getDocCount());

        builder.addAggregation(AggregationBuilders.filter("agg2").filter(QueryBuilders.existsQuery("student.name")));
        sr = builder.execute().actionGet();
        agg = sr.getAggregations().get("agg2");
        System.out.println(agg.getDocCount());
    }

    /**
     * Filters Aggregation
     */
    @Test
    public void testFiltersAgg() {
        SearchRequestBuilder builder = esClient.prepareSearch(indexName);
        builder.addAggregation(AggregationBuilders.filters("agg")
                .filter("isMale", QueryBuilders.termQuery("student.isMale", true))//student.isMale=true的记录
                .filter("friends:小红", QueryBuilders.termQuery("student.friends", "小红"))
                .filter("woman", QueryBuilders.termQuery("student.isMale", false)));
        SearchResponse sr = builder.execute().actionGet();
        Filters agg = sr.getAggregations().get("agg");

        for (Filters.Bucket entry : agg.getBuckets()) {
            String key = entry.getKeyAsString();            // bucket key
            long docCount = entry.getDocCount();            // Doc count
            System.out.println(key + " " + docCount);
        }
    }

    /**
     * Missing Aggregation
     */
    @Test
    public void testMissingAgg() {
        SearchRequestBuilder builder = esClient.prepareSearch(indexName);
        builder.addAggregation(AggregationBuilders.missing("agg").field("student.name"));//不存在student.name字段的记录
        SearchResponse sr = builder.execute().actionGet();
        Missing agg = sr.getAggregations().get("agg");
        System.out.println(agg.getDocCount());

        builder.addAggregation(AggregationBuilders.missing("agg2").field("student.name2"));
        sr = builder.execute().actionGet();
        agg = sr.getAggregations().get("agg2");
        System.out.println(agg.getDocCount());
    }


    /**
     * Nested Aggregation
     * TODO
     */
    @Test
    public void testNestedAgg() {
        SearchRequestBuilder builder = esClient.prepareSearch(indexName);
        builder.addAggregation(AggregationBuilders.nested("agg").path("student.name"));
        SearchResponse sr = builder.execute().actionGet();
        Nested agg = sr.getAggregations().get("agg");
        System.out.println(agg.getDocCount());
    }

    @Test
    public void testReverseNestedAgg() {

    }

    /**
     * Children Aggregation
     * TODO
     */
    @Test
    public void testChildrenAgg() {
        SearchRequestBuilder builder = esClient.prepareSearch(indexName);
        builder.addAggregation(AggregationBuilders.children("agg").childType("student.name"));
        builder.addAggregation(AggregationBuilders.nested("agg").path("student.name"));
        SearchResponse sr = builder.execute().actionGet();
        Nested agg = sr.getAggregations().get("agg");
        System.out.println(agg.getDocCount());
    }

    /**
     * Terms Aggregation
     */
    @Test
    public void testTermsAgg() {
        SearchRequestBuilder builder = esClient.prepareSearch(indexName);
        builder.addAggregation(AggregationBuilders.terms("agg").field("student.name"));
        SearchResponse sr = builder.execute().actionGet();

        Terms agg = sr.getAggregations().get("agg");
        for (Terms.Bucket entry : agg.getBuckets()) {
            System.out.println(entry.getKeyAsString() + " " + entry.getDocCount());
        }
    }

    @Test
    public void testOrder() {
        SearchRequestBuilder builder = esClient.prepareSearch(indexName);
        //按字母升序排序
        builder.addAggregation(AggregationBuilders.terms("agg").field("student.age").order(Terms.Order.term(true)));
        //按doc_count升序排序
        builder.addAggregation(AggregationBuilders.terms("agg2").field("student.age").order(Terms.Order.count(true)));

        //Ordering the buckets by single value metrics sub-aggregation (identified by the aggregation name)
        builder.addAggregation(AggregationBuilders.terms("agg3").field("student.age").order(Terms.Order.aggregation("avg_isMale", false))
                .subAggregation(AggregationBuilders.avg("avg_isMale").field("student.isMale")));

        SearchResponse sr = builder.execute().actionGet();

        Terms agg = sr.getAggregations().get("agg");
        for (Terms.Bucket entry : agg.getBuckets()) {
            System.out.println(entry.getKeyAsString() + " " + entry.getDocCount());
        }

        System.out.println();
        Terms agg2 = sr.getAggregations().get("agg2");
        for (Terms.Bucket entry : agg2.getBuckets()) {
            System.out.println(entry.getKeyAsString() + " " + entry.getDocCount());
        }

        System.out.println();
        Terms agg3 = sr.getAggregations().get("agg3");
        for (Terms.Bucket entry : agg3.getBuckets()) {
            System.out.println(entry.getKeyAsString() + " " + entry.getDocCount());
        }
    }


}
