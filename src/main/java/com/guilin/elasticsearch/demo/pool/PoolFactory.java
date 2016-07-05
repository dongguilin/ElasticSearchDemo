package com.guilin.elasticsearch.demo.pool;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

public class PoolFactory {

	private static final int TIME_OUT = 30 * 1000;

	public static <T> Builder<T> builder() {
		return new Builder<T>();
	}

	public static class Builder<T> {

		GenericObjectPoolConfig config = null;

		BasePooledObjectFactory<T> factory = null;

		AbstractDropletCreator<T> creator;

		public Builder() {}

		public GenericObjectPool<T> build() throws Exception {

			if (factory == null) {
				if (creator == null) {
					throw new Exception("池对像创建者不能为null!");
				} else {
					factory = new DropletFactory<T>(this.creator);
				}
			}

			if (this.config == null) {
				return new GenericObjectPool<T>(factory);
			} else {
				return new GenericObjectPool<T>(factory, config);
			}

		}

		public Builder<T> config(GenericObjectPoolConfig config) {
			if (config == null) {
				GenericObjectPoolConfig cfg = new GenericObjectPoolConfig();
				cfg.setMaxTotal(50); // 整个池最大值
				cfg.setMaxIdle(3); // 最大空闲数
				cfg.setMaxWaitMillis(TIME_OUT); // 最长等街时间,-1为获取不到永远等待
				cfg.setMinEvictableIdleTimeMillis(10 * 60000L); // 最大空闲时间,10mins
				cfg.setBlockWhenExhausted(true);
				cfg.setTestOnBorrow(true);
				cfg.setNumTestsPerEvictionRun(Integer.MAX_VALUE); // always test all idle objects
				cfg.setTimeBetweenEvictionRunsMillis(1 * 60000L); // -1不启动。默认1min一次
				// config.setTestWhileIdle(true); // 发呆过长移除的时候是否test一下先
				this.config = cfg;
			} else {
				this.config = config;
			}

			return this;
		}

		public Builder<T> factory(BasePooledObjectFactory<T> factory) {
			this.factory = factory;
			return this;
		}

		public Builder<T> creator(AbstractDropletCreator<T> creator) {
			this.creator = creator;
			return this;
		}

		public GenericObjectPoolConfig getConfig() {
			return config;
		}

		public BasePooledObjectFactory<T> getFactory() {
			return factory;
		}

		public AbstractDropletCreator<T> getCreator() {
			return creator;
		}
	}
}
