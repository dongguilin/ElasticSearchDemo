package com.guilin.elasticsearch.demo.pool;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class ESPool {
    private static Logger _LOG = LoggerFactory.getLogger(ESPool.class);
    private volatile static ESPool esPool;

    private static GenericObjectPool<ESClient> pool = null;

    private Set<String> eshost;
    private int esport;
    private String timeout;
    private String esName;
    private int poolWaitTime;
    private int maxActive;
    private int maxIdle;

    private ESPool(Set<String> eshost, int esport, String timeout,
                   String esName, int poolWaitTime, int maxActive, int maxIdle) {
        this.eshost = eshost;
        this.esport = esport;
        this.timeout = timeout;
        this.esName = esName;
        this.poolWaitTime = poolWaitTime;
        this.maxActive = maxActive;
        this.maxIdle = maxIdle;
    }

    public static ESPool generalEsPool(Set<String> eshost, int esport, String timeout,
                                       String esName, int poolWaitTime, int maxActive, int maxIdle) {
        if (esPool == null) {
            synchronized (ESPool.class) {
                if (esPool == null) {
                    esPool = new ESPool(eshost, esport, timeout, esName, poolWaitTime, maxActive, maxIdle);
                    esPool.init();
                }
            }
        }
        return esPool;
    }

    private void init() {
        try {
            GenericObjectPoolConfig cfg = new GenericObjectPoolConfig();
            cfg.setMaxTotal(maxActive); // 整个池最大值
            cfg.setMaxIdle(maxIdle); // 最大空闲数
            cfg.setMaxWaitMillis(poolWaitTime); // 最长等街时间,-1为获取不到永远等待
            cfg.setMinEvictableIdleTimeMillis(10 * 60000L); // 最大空闲时间,10mins
            cfg.setBlockWhenExhausted(true);
            cfg.setTestOnBorrow(true);
            cfg.setNumTestsPerEvictionRun(Integer.MAX_VALUE); // always test all idle objects
            cfg.setTimeBetweenEvictionRunsMillis(1 * 60000L); // -1不启动。默认1min一次
            pool = PoolFactory.<ESClient>builder().creator(new EsClientCreator(eshost, esport, timeout, esName)).config(cfg).build();
        } catch (Exception e) {
            _LOG.error("ES Pool init failure", e);
        }
    }

    public ESClient borrowObject() throws Exception {
        return pool.borrowObject();
    }

    public ESClient borrowObject(long millis) throws Exception {
        return pool.borrowObject(millis);
    }

    public void returnObject(ESClient obj) {
        if (obj != null) {
            pool.returnObject(obj);
        }
    }

    public void destroy() throws Exception {
        pool.close();
    }

    private class EsClientCreator extends AbstractDropletCreator<ESClient> {
        private Set<String> eshost;
        private int esport;
        private String timeout;
        private String esName;

        public EsClientCreator(Set<String> eshost, int esport, String timeout,
                               String esName) {
            this.eshost = eshost;
            this.esport = esport;
            this.timeout = timeout;
            this.esName = esName;
        }

        @Override
        public ESClient create() throws Exception {
            ESClient esClient = new ESClient(eshost, esport, timeout, esName);
            return esClient;
        }

    }

}