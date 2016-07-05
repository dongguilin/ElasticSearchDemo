package com.guilin.elasticsearch.demo.pool;

import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;

public class PoolKeyedFactory {

	private static final int TIME_OUT = 30 * 1000;

	public static <K, V> Builder<K, V> builder() {
		return new Builder<K, V>();
	}

	public static class Builder<K, V> {

		GenericKeyedObjectPoolConfig config = null;

		BaseKeyedPooledObjectFactory<K, V> factory = null;

		DropletKeyedCreator<K, V> creator;

		public Builder() {}

		public GenericKeyedObjectPool<K, V> build() throws Exception {

			if (factory == null) {
				if (creator == null) {
					throw new Exception("池对像创建者不能为null!");
				} else {
					factory = new DropletKeyedFactory<K, V>(creator);
				}
			}

			if (this.config == null) {
				return new GenericKeyedObjectPool<K, V>(factory);
			} else {
				return new GenericKeyedObjectPool<K, V>(factory, config);
			}

		}

		public Builder<K, V> config(GenericKeyedObjectPoolConfig config) {
			if (config == null) {
				GenericKeyedObjectPoolConfig cfg = new GenericKeyedObjectPoolConfig();
				cfg.setMaxTotal(50); // 整个池最大值
				cfg.setMaxIdlePerKey(3);
				cfg.setMaxWaitMillis(TIME_OUT); // 最长等街时间,-1为获取不到永远等待
				cfg.setMinEvictableIdleTimeMillis(10 * 60000L); // 最大空闲时间,10mins
				cfg.setBlockWhenExhausted(true);
				cfg.setTestOnBorrow(true);
				cfg.setNumTestsPerEvictionRun(Integer.MAX_VALUE); // always test
																	// all idle
																	// objects
				cfg.setTimeBetweenEvictionRunsMillis(1 * 60000L); // -1不启动。默认1min一次
				// config.setTestWhileIdle(true); // 发呆过长移除的时候是否test一下先
				this.config = cfg;
			} else {
				this.config = config;
			}

			return this;
		}

		public Builder<K, V> factory(BaseKeyedPooledObjectFactory<K, V> factory) {
			this.factory = factory;
			return this;
		}

		public Builder<K, V> creator(DropletKeyedCreator<K, V> creator) {
			this.creator = creator;
			return this;
		}

		public GenericKeyedObjectPoolConfig getConfig() {
			return config;
		}

		public BaseKeyedPooledObjectFactory<K, V> getFactory() {
			return factory;
		}

		public DropletKeyedCreator<K, V> getCreator() {
			return creator;
		}
	}
}
