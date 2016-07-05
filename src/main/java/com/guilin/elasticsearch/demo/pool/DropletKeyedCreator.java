package com.guilin.elasticsearch.demo.pool;

import java.io.Closeable;
import java.io.IOException;

/**
 * 池对像创建者接口
 * 
 * @author ruibing.zhao
 *         2015年3月18日
 * @param <T>
 */
public abstract class DropletKeyedCreator<K, T> implements Closeable {

	public T create(K key) throws Exception {
		return null;
	}

	public boolean valid(T t) {
		return true;
	}

	public void destroy(T t) throws Exception {
		;
	}

	@Override
	public void close() throws IOException {
		;
	}
}
