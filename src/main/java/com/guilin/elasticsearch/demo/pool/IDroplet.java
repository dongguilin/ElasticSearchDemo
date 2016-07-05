package com.guilin.elasticsearch.demo.pool;

import java.io.Closeable;

/**
 * 水滴(池保存对像接口)
 * <p>
 * 任凭弱水三千，我只取一瓢饮
 * </p>
 * 
 * @author ruibing.zhao
 *         2015年3月13日
 * @param <T>
 */
public interface IDroplet<T> extends Closeable {

	// 幻化原对像
	public T illusion();

	// 当前对像是否是活动状态
	boolean valid();
}
