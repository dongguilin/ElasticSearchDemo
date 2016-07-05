package com.guilin.elasticsearch.demo.pool;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import java.io.Closeable;

public class DropletFactory<T> extends BasePooledObjectFactory<T> {
	AbstractDropletCreator<T> creator;

	public DropletFactory(AbstractDropletCreator<T> creator) {
		this.creator = creator;
	}

	@Override
	public T create() throws Exception {
		return creator.create();
	}

	@Override
	public PooledObject<T> wrap(T bud) {
		return new DefaultPooledObject<T>(bud);
	}

	@Override
	public void destroyObject(PooledObject<T> p) throws Exception {
		T t = p.getObject();
		if (t instanceof Closeable) {
			Closeable c = (Closeable) t;
			c.close();
		} else {
			creator.destroy(t);
		}
	}

	@Override
	public boolean validateObject(PooledObject<T> p) {
		T t = p.getObject();
		if (t instanceof IDroplet) {
			if (((IDroplet<?>) t).valid())
				return true;
			else return false;
		}
		return creator.valid(t);

	}
}
