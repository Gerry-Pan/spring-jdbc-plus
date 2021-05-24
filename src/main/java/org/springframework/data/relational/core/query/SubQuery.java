package org.springframework.data.relational.core.query;

import lombok.Builder;

@Builder
public class SubQuery {

	/**
	 * 子查询from的表
	 */
	Class<?> table;

	/**
	 * 关联表的关联字段
	 */
	String inverseKey;

	/**
	 * from表的关联字段
	 */
	String localKey;

	/**
	 * from表查询字段
	 */
	String[] columns;

	/**
	 * 从表查询条件
	 */
	CriteriaDefinition criteria;

	public ExistsCriteria exists() {
		return ExistsCriteria.from(table).columns(columns).localKey(localKey).inverseKey(inverseKey).criteria(criteria);
	}

}
