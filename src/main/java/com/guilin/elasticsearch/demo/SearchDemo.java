package com.guilin.elasticsearch.demo;

import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by guilin1 on 16/3/18.
 */
public class SearchDemo extends Base {

    String date = new DateTime("2016-03-10").toString("yyyy-MM-dd");
    String indexName = "aleiye-0-" + date;


    @Test
    public void testScroll() {
        QueryBuilder qb = QueryBuilders.matchAllQuery();

        SearchResponse scrollResp = esClient.prepareSearch()
                .setSearchType(SearchType.SCAN)
                .setScroll(new TimeValue(60000))
//                .setQuery(qb)
                .setQuery(QueryBuilders.matchQuery("clientip","123.116.170.191"))
                .setSize(4).execute().actionGet(); //100 hits per shard will be returned for each scroll
        //Scroll until no hits are returned
        int i=0,j=0;
        while (true) {
            for (SearchHit hit : scrollResp.getHits().getHits()) {
                //Handle the hit...
                System.out.println(hit.getId());
                i++;
            }
            scrollResp = esClient.prepareSearchScroll(scrollResp.getScrollId())
                    .setScroll(new TimeValue(60000))
                    .execute().actionGet();
            //Break condition: No hits are returned
            if (scrollResp.getHits().getHits().length == 0) {
                break;
            }
            j++;
        }
        System.out.println(i+" "+j);
    }

    /**
     * MultiSearch
     */
    @Test
    public void testMultiSearch() {
        SearchRequestBuilder srb1 = esClient
                .prepareSearch(indexName).setQuery(QueryBuilders.queryStringQuery("lWFc")).setSize(1);
        SearchRequestBuilder srb2 = esClient
//                .prepareSearch().setQuery(QueryBuilders.matchQuery("student.friends", "QOhs")).setSize(1);
                .prepareSearch().setQuery(QueryBuilders.matchQuery("*", "*")).setSize(1);

        MultiSearchResponse sr = esClient.prepareMultiSearch()
                .add(srb1)
                .add(srb2)
                .execute().actionGet();

        // You will get all individual responses from MultiSearchResponse#getResponses()
        long nbHits = 0;
        for (MultiSearchResponse.Item item : sr.getResponses()) {
            SearchResponse response = item.getResponse();
            nbHits += response.getHits().getTotalHits();
        }
        System.out.println(nbHits);
    }

    //TODO
    @Test
    public void testUsingAgg() {
        SearchResponse sr = esClient.prepareSearch(indexName)
                .setQuery(QueryBuilders.matchAllQuery())
                .addAggregation(
                        AggregationBuilders.terms("agg1").field("student.name")
                )
                .addAggregation(
                        AggregationBuilders.dateHistogram("agg2")
                                .field("student.year")
                                .interval(DateHistogramInterval.YEAR)
                )
                .execute().actionGet();

        // Get your facet results
        Terms agg1 = sr.getAggregations().get("agg1");
        Aggregations agg2 = sr.getAggregations().get("agg2");
    }

    //TODO
    @Test
    public void testTerminateAfter(){
        SearchResponse sr = esClient.prepareSearch(indexName)
                .setTerminateAfter(10000)
                .get();

        if (sr.isTerminatedEarly()) {
            // We finished early
            System.out.println("finished early");
        }

        System.out.println(sr.getHits().getTotalHits());
    }

    //TODO
    @Test
    public void testCount(){
        CountResponse response = esClient.prepareCount(indexName)
                .setQuery(QueryBuilders.termQuery("name", "student.name"))
                .execute()
                .actionGet();
        System.out.println(response.getCount());


    }

    @Test
    public void testNode(){
        Node node = NodeBuilder.nodeBuilder().settings(Settings.settingsBuilder()
                .put("client.transport.ping.timeout", 1000 * 60)
                        //指定集群名称
                .put("cluster.name", "guilin-app")
                        //探测集群中机器状态
                .put("client.transport.sniff", true)
                .build()).node();

        System.out.println(node.client());
    }

    public static void searchDocument(Client client, String index, String type,
                                      String field, String value){

        SearchResponse response = client.prepareSearch(index)
                .setTypes(type)
                .setSearchType(SearchType.QUERY_AND_FETCH)
//                .setQuery(fieldQuery(field, value))
                .setFrom(0).setSize(60).setExplain(true)
                .execute()
                .actionGet();

        SearchHit[] results = response.getHits().getHits();

        System.out.println("Current results: " + results.length);
        for (SearchHit hit : results) {
            System.out.println("------------------------------");
            Map<String,Object> result = hit.getSource();
            System.out.println(result);
        }

    }


    public static void updateDocument(Client client, String index, String type,
                                      String id, String field, String newValue){

        Map<String, Object> updateObject = new HashMap<String, Object>();
        updateObject.put(field, newValue);

//        client.prepareUpdate(index, type, id)
//                .setScript("ctx._source." + field + "=" + field)
//                .setScriptParams(updateObject).execute().actionGet();
    }

    public static void deleteDocument(Client client, String index, String type, String id){

        DeleteResponse response = client.prepareDelete(index, type, id).execute().actionGet();
        System.out.println("Information on the deleted document:");
        System.out.println("Index: " + response.getIndex());
        System.out.println("Type: " + response.getType());
        System.out.println("Id: " + response.getId());
        System.out.println("Version: " + response.getVersion());
    }




}
