package com.guilin.elasticsearch.demo.pool;

import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import java.io.Closeable;

public class DropletKeyedFactory<K, V> extends BaseKeyedPooledObjectFactory<K, V> {

	DropletKeyedCreator<K, V> creator;

	public DropletKeyedFactory(DropletKeyedCreator<K, V> creator) {
		this.creator = creator;
	}

	@Override
	public V create(K key) throws Exception {
		return creator.create(key);
	}

	@Override
	public PooledObject<V> wrap(V value) {
		return new DefaultPooledObject<V>(value);
	}

	@Override
	public boolean validateObject(K key, PooledObject<V> p) {
		V t = p.getObject();
		if (t instanceof IDroplet) {
			if (((IDroplet<?>) t).valid())
				return true;
			else
				return false;
		}
		return creator.valid(t);
	}

	@Override
	public void destroyObject(K key, PooledObject<V> p) throws Exception {
		V v = p.getObject();
		if (v instanceof Closeable) {
			Closeable c = (Closeable) v;
			c.close();
		} else {
			creator.destroy(v);
		}
	}

}
