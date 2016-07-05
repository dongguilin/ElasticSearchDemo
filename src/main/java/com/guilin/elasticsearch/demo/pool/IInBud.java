package com.guilin.elasticsearch.demo.pool;

public interface IInBud<T> {
	void returnObject(T t);

	T borrowObject() throws Exception;

	T borrowObject(long millis) throws Exception;
}
