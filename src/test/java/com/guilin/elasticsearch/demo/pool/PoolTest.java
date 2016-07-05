package com.guilin.elasticsearch.demo.pool;

import org.elasticsearch.client.Client;
import org.junit.Test;

import java.util.Collections;

/**
 * Created by guilin1 on 16/7/5.
 */
public class PoolTest {

    @Test
    public void test1() throws Exception {
        ESPool pool = ESPool.generalEsPool(Collections.singleton("10.0.1.136"), 9300, "6s", "elasticsearch",
                10000, 10, 5);
        for (int i = 0; i < 15; i++) {
            ESClient esClient = pool.borrowObject();
            Client client = esClient.illusion();
            System.out.println(esClient);

            ESClient esClient2 = pool.borrowObject();
            Client client2 = esClient.illusion();
            System.out.println(esClient2);


            if (client != null) {
                pool.returnObject(esClient);
            }

            if (client2 != null) {
                pool.returnObject(esClient2);
            }
        }
    }
}
