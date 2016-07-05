package com.guilin.elasticsearch.demo;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.lucene.queryparser.xml.builders.RangeQueryBuilder;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryAction;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.plugin.deletebyquery.DeleteByQueryPlugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.joda.time.DateTime;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by guilin1 on 16/3/15.
 */
public class CrudDemo extends Base{

    @Test
    public void testCreate() {

        String date = DateTime.now().toString("yyyy-MM-dd");

        List<Student> students = buildStudentsData();
        List<Map<String, Object>> list = new ArrayList<>();
        for (Student student : students) {
            Map<String,Object> map = student.toMap();
            StringBuffer buffer = new StringBuffer();
            for(String key: map.keySet()){
                String value = JSON.toJSONString(map.get(key));
                if(StringUtils.isNotBlank(value)){
                    buffer.append(value).append("\t");
                }
            }
            map.put("A_message",buffer.toString());
            list.add(map);
        }
//        insert("aleiye-0-" + date, "1", "student", list);
        batchInsert("aleiye-1-" + date, "1", "student", list);

//        Map<String,Object> rec1 = students.get(0).toMap();
//        Map<String,Object> rec2 = students.get(1).toMap();
//        rec2.put("scores", 12);
//        rec2.put("weight", "45公斤");
//        batchInsert2("aleiye-1-" + date, "1", "student", rec1, rec2);
//        batchInsert2("aleiye-1-"+date, "2", "student", rec1, rec2);

//        insert("aleiye-0-" + date, "1", "date", buildData());


    }

    @Test
    public void testQuery() {
        String date = DateTime.now().toString("yyyy-MM-dd");
        String indexName = "aleiye-1-" + date;
        SearchRequestBuilder builder = esClient.prepareSearch(indexName).setTypes();
//        QueryStringQueryBuilder queryBuilder = QueryBuilders.queryStringQuery("student.height:>170");
//        QueryStringQueryBuilder queryBuilder = QueryBuilders.queryStringQuery("student.height:>160 AND student.weight:>55");
//        QueryStringQueryBuilder queryBuilder = QueryBuilders.queryStringQuery("student.name:\"小文\" OR student2.name:\"小文\"");
//        QueryStringQueryBuilder queryBuilder = QueryBuilders.queryStringQuery("student.scores2:82.5 AND student2.scores2:>90 AND student.intendedTime:>" + new DateTime("2019-09-08").getMillis());
//        QueryStringQueryBuilder queryBuilder = QueryBuilders.queryStringQuery("date.d_long:>" + new DateTime("2016-03-08").getMillis());
//        QueryStringQueryBuilder queryBuilder = QueryBuilders.queryStringQuery("date.d_date:>" + new DateTime("2016-03-08").toString("yyyy-MM-dd"));
//        QueryStringQueryBuilder queryBuilder = QueryBuilders.queryStringQuery("date.u_d_l:[\"2016-03-08 00:00:00\" TO \"2016-03-17 00:00:00\"]");
//        QueryStringQueryBuilder queryBuilder = QueryBuilders.queryStringQuery("date.d_date:[\"2016-03-08 00:00:00\" TO \"2016-03-17 00:00:00\"]");
//        QueryStringQueryBuilder queryBuilder = QueryBuilders.queryStringQuery("date.d_date:>\"2016-03-08 00:00:00\"");
//        QueryStringQueryBuilder queryBuilder = QueryBuilders.queryStringQuery("date.u_d_l:>\"2016-03-08 00:00:00\"");

        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("student.d_date").gte(new DateTime("2016-03-07").getMillis()));
//        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("student.d_date").gte("2016-03-07 00:00:00"));
//        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("student.d_date").gte("2016-03-07"));


        builder.setQuery(queryBuilder);
        builder.setSize(10000);
        SearchResponse response = builder.execute().actionGet();
        SearchHits hits = response.getHits();
//        System.out.println(JSON.toJSONString(hits));
        System.out.println(hits.getTotalHits()+" " + response.getTookInMillis());

    }

    @Test
    public void testUpsert() throws IOException {
        String date = DateTime.now().toString("yyyy-MM-dd");
        String indexName = "aleiye-0-" + date;

        String type = "1";

        String table = "date";

        List<Map<String, Object>> list = query(indexName, "*:*", table, type);

        System.out.println(list.size());

        Map<String, Object> map = list.get(0);
        System.out.println(JSON.toJSONString(map));

        map.put("u_d_l", new DateTime().toString("yyyy-MM-dd HH:mm:ss"));

        upsert(indexName, type, table, map.get("_id").toString(), map);


    }

    @Test
    public void testDelete() throws Exception {
        String date = DateTime.now().toString("yyyy-MM-dd");
        String indexName = "aleiye-0-" + date;

        String queryString = null;

        DeleteByQueryRequestBuilder delete = new DeleteByQueryRequestBuilder(esClient, DeleteByQueryAction.INSTANCE);
        delete.setIndicesOptions(IndicesOptions.fromOptions(false, true, true, false));
        delete.setIndices(indexName).setQuery(StringUtils.isEmpty(queryString) ? QueryBuilders.matchAllQuery() : QueryBuilders.queryStringQuery(queryString));
        DeleteByQueryResponse response = delete.execute().actionGet();
        System.out.println(JSON.toJSONString(response));
    }

    public static void main(String[] args) {
        System.out.println(new DateTime("2016-03-08").toString("yyyy-MM-dd HH:mm:ss"));
    }

    private List<Map<String, Object>> query(String indexName, String queryStr, String table, String... type) {
        SearchRequestBuilder builder = esClient.prepareSearch(indexName).setTypes(type);
        QueryStringQueryBuilder queryBuilder = QueryBuilders.queryStringQuery(queryStr);
        builder.setQuery(queryBuilder);
        builder.setSize(10000);
        SearchResponse response = builder.execute().actionGet();
        SearchHits hits = response.getHits();
        if (hits.getTotalHits() > 0) {
            List<Map<String, Object>> list = new ArrayList<>((int) hits.getTotalHits());
            SearchHit[] hitarr = hits.getHits();
            for (SearchHit hit : hitarr) {
                String id = hit.getId();
                Map<String, Object> map = (Map<String, Object>) hit.getSource().get(table);
                map.put("_id", id);
                list.add(map);
            }
            return list;
        }
        return null;
    }

    private static List<Map<String, Object>> buildData() {
        List<Map<String, Object>> list = new ArrayList<>();
        Map<String, Object> map = new HashMap<>();
        DateTime dateTime = new DateTime(2016, 3, 5, 5, 12, 55);
        map.put("d_long", dateTime.getMillis());
        map.put("d_date", dateTime.toString("yyyy-MM-dd"));
        map.put("d_datetime", dateTime.toString("yyyy-MM-dd HH:mm:ss"));
        list.add(map);

        map = new HashMap<>();
        dateTime = new DateTime(2016, 3, 6, 7, 22, 35);
        map.put("d_long", dateTime.getMillis());
        map.put("d_date", dateTime.toString("yyyy-MM-dd"));
        map.put("d_datetime", dateTime.toString("yyyy-MM-dd HH:mm:ss"));
        list.add(map);

        map = new HashMap<>();
        dateTime = new DateTime(2016, 3, 7, 22, 22, 15);
        map.put("d_long", dateTime.getMillis());
        map.put("d_date", dateTime.toString("yyyy-MM-dd"));
        map.put("d_datetime", dateTime.toString("yyyy-MM-dd HH:mm:ss"));
        list.add(map);

        map = new HashMap<>();
        dateTime = new DateTime(2016, 3, 8, 12, 22, 33);
        map.put("d_long", dateTime.getMillis());
        map.put("d_date", dateTime.toString("yyyy-MM-dd"));
        map.put("d_datetime", dateTime.toString("yyyy-MM-dd HH:mm:ss"));
        list.add(map);

        map = new HashMap<>();
        dateTime = new DateTime(2016, 3, 8, 7, 25, 35);
        map.put("d_long", dateTime.getMillis());
        map.put("d_date", dateTime.toString("yyyy-MM-dd"));
        map.put("d_datetime", dateTime.toString("yyyy-MM-dd HH:mm:ss"));
        list.add(map);

        return list;
    }

    private static List<Student> buildStudentsData() {
        List<Student> list = new ArrayList<>();

        DateTime dateTime = new DateTime(2016, 3, 5, 5, 12, 55);
        List<Course> courses = Lists.newArrayList(new Course("Chinese", 88), new Course("English", 82.5), new Course("math", 93));
        Student student = new Student("2009090901", "张三", 172, 56.2, new DateTime("2009-09-09").toDate(),
                new DateTime("2013-09-09").getMillis(), true, courses, Lists.newArrayList("小文", "李四", "小刚"));
        student.setD_long(dateTime.getMillis());
        student.setD_date(dateTime.toString("yyyy-MM-dd"));
        student.setD_datetime(dateTime.toString("yyyy-MM-dd HH:mm:ss"));
        list.add(student);

        dateTime = new DateTime(2016, 3, 7, 22, 22, 15);
        courses = Lists.newArrayList(new Course("Chinese", 89.5), new Course("English", 95), new Course("math", 76), new Course("physical", 82));
        student = new Student("2009090902", "小文", 155, 48, new DateTime("2009-09-08").toDate(),
                new DateTime("2013-09-09").getMillis(), false, courses, Lists.newArrayList("张三", "小强"));
        student.setD_long(dateTime.getMillis());
        student.setD_date(dateTime.toString("yyyy-MM-dd"));
        student.setD_datetime(dateTime.toString("yyyy-MM-dd HH:mm:ss"));
        list.add(student);

        dateTime = new DateTime(2016, 3, 7, 22, 22, 15);
        courses = Lists.newArrayList(new Course("Chinese", 83.5), new Course("English", 89), new Course("math", 91), new Course("physical", 92));
        student = new Student("2008090902", "李四", 162, 52.5, new DateTime("2008-09-09").toDate(),
                new DateTime("2012-09-09").getMillis(), true, courses, Lists.newArrayList("张三", "小红"));
        student.setD_long(dateTime.getMillis());
        student.setD_date(dateTime.toString("yyyy-MM-dd"));
        student.setD_datetime(dateTime.toString("yyyy-MM-dd HH:mm:ss"));
        list.add(student);

        dateTime = new DateTime(2016, 3, 8, 12, 22, 33);
        courses = Lists.newArrayList(new Course("Chinese", 81.5), new Course("English", 79.5), new Course("math", 81), new Course("physical", 87));
        student = new Student("2008090903", "小红", 165, 55.5, new DateTime("2008-09-08").toDate(),
                new DateTime("2012-09-09").getMillis(), false, courses, Lists.newArrayList("小文"));
        student.setD_long(dateTime.getMillis());
        student.setD_date(dateTime.toString("yyyy-MM-dd"));
        student.setD_datetime(dateTime.toString("yyyy-MM-dd HH:mm:ss"));
        list.add(student);

        return list;
    }

    private static void batchInsert2(String index, String userId, String table, Map<String, Object> recs, Map<String, Object> recs2) {
        BulkRequestBuilder builder = esClient.prepareBulk();

        recs.put("A_timestamp", System.currentTimeMillis());
        IndexRequest request = esClient.prepareIndex(index, userId).request();

        Map<String, Map<String, Object>> realRec = new HashMap<>(1);
        realRec.put(table, recs);
        realRec.put(table + "2", recs2);
        request.source(realRec);
        request.refresh();
        builder.add(request);

        BulkResponse response = builder.execute().actionGet();
        System.out.println("操作" + (response.hasFailures() ? "失败" : "成功") + " 影响记录数：" + response.getItems().length + " 花费：" + response.getTookInMillis() + "毫秒");
    }

    private static void batchInsert(String index, String userId, String table, List<Map<String, Object>> recs) {
        BulkRequestBuilder builder = esClient.prepareBulk();

        for (Map<String, Object> map : recs) {
            if (MapUtils.isEmpty(map)) {
                continue;
            }
            map.put("A_timestamp", System.currentTimeMillis());

            IndexRequest request = esClient.prepareIndex(index, userId).request();

            Map<String, Map<String, Object>> realRec = new HashMap<>(1);
            realRec.put(table, map);
            request.source(realRec);
//            request.refresh();
            builder.add(request);
        }

        BulkResponse response = builder.execute().actionGet();
        System.out.println("操作" + (response.hasFailures() ? "失败" : "成功") + " 影响记录数：" + response.getItems().length + " 花费：" + response.getTookInMillis() + "毫秒");
    }


    private static void insert(String index, String userId, String table, List<Map<String, Object>> recs) {
        for (Map<String, Object> map : recs) {
            if (MapUtils.isEmpty(map)) {
                continue;
            }
            map.put("A_timestamp", System.currentTimeMillis());

            Map<String, Map<String, Object>> realRec = new HashMap<>(1);
            realRec.put(table, map);

            IndexResponse response = esClient.prepareIndex(index, userId)
                    .setSource(realRec)
                    .execute()
                    .actionGet();
            System.out.println(response.getId() + " " + response.getIndex() + " " + response.getType() + " " + response.getVersion());
        }
    }

    private static void upsert(String index, String userId, String table, String id, Map<String, Object> rec) throws IOException {
        rec.put("A_utimestamp", System.currentTimeMillis());
        rec.remove("_id");
        Map<String,Map<String,Object>> map = new HashMap<>(1);
        map.put(table,rec);
        UpdateResponse response = esClient.prepareUpdate(index, userId, id)
                .setDoc(map)
                .execute()
                .actionGet(15, TimeUnit.SECONDS);
        System.out.println(response.getId() + " " + response.getIndex() + " " + response.getType() + " " + response.getVersion());
    }

    private static void batchUpsert(String index, String userId, String table, List<Map<String, Object>> recs) throws IOException {
        BulkRequestBuilder bulk = esClient.prepareBulk();
        for (Map<String, Object> map : recs) {
            UpdateRequestBuilder updateRequest = esClient.prepareUpdate(index, userId, map.get("_id").toString());

            XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
            map.remove("_id");
            map.put("A_utimestamp", System.currentTimeMillis());

            builder.field(table, map);

            builder.endObject();
            updateRequest.setDoc(builder);
            bulk.add(updateRequest);
        }
        BulkResponse response = bulk.execute().actionGet();
        System.out.println("操作" + (response.hasFailures() ? "失败" : "成功") + " 影响记录数：" + response.getItems().length + " 花费：" + response.getTookInMillis() + "毫秒");
    }

    @Test
    public void testCreateTemplate() throws IOException {
        Settings settings = Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.refresh_interval", "90s")
                .build();

        esClient.admin().indices().preparePutTemplate("template_aleiye")
                .setTemplate("aleiye*")
                .setSettings(settings)
                .addMapping("_default_", tt()).get();
    }

    /**
     * 查询ES中的所有索引
     *
     * @return
     */
    private List<String> getOpenIndecies() {
        ClusterStateResponse csr = esClient.admin().cluster().prepareState().execute().actionGet();
        ClusterState cs = csr.getState();
        MetaData md = cs.getMetaData();
        String[] openIndex = md.getConcreteAllOpenIndices();
        return Arrays.asList(openIndex);
    }

    private static XContentBuilder tt() throws IOException {
        Map<String, String> message = new HashMap<>();
        message.put("type", "string");
        message.put("analyzer", "dotstd");

        Map<String, String> field = new HashMap<>();
        field.put("type", "string");
        field.put("index", "not_analyzed");

        Map<String, String> date = new HashMap<>();
        date.put("type", "date");
        date.put("format", "yyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis");

        Map<String, String> location = new HashMap<>();
        location.put("type", "geo_point");

        XContentBuilder x = XContentFactory.jsonBuilder().startObject()
                .startObject("_all").field("enabled", false).endObject()
                .startObject("_source").field("enabled", true).endObject()
                .startArray("dynamic_templates")
                .startObject().field("t1_1")
                .startObject()
                .field("match").value("A_mess*")
                .field("match_mapping_type").value("string")
                .field("mapping").value(message)
                .endObject()
                .endObject()
                .startObject().field("t2_1")
                .startObject()
                .field("match").value("d_*")
//                .field("match_mapping_type").value("string")
                .field("mapping").value(date)
                .endObject()
                .endObject()
//                .startObject().field("t2_d_1")
//                .startObject()
//                .field("match").value("student.d_*")
//                .field("match_mapping_type").value("string")
//                .field("mapping").value(date)
//                .endObject()
//                .endObject()
                .startObject().field("t5_1")
                .startObject()
                .field("match").value("A_timestamp")
                .field("mapping").value(date)
                .endObject()
                .endObject()
                .startObject().field("t3_1")
                .startObject()
                .field("match").value("l_*")
                .field("mapping").value(location)
                .endObject()
                .endObject()
                .startObject().field("t4_1")
                .startObject()
                .field("match").value("*")
                .field("match_mapping_type").value("string")
                .field("mapping").value(field)
                .endObject()
                .endObject()
                .endArray()
                .endObject();
        return x;
    }


}
