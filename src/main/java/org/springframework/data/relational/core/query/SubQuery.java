package org.springframework.data.relational.core.query;

import java.util.List;

import org.springframework.data.relational.core.mapping.ManyToOne;

import lombok.Builder;
import lombok.Singular;

@Builder
public class SubQuery {

	/**
	 * 子查询from的表
	 */
	Class<?> from;

	/**
	 * 关联表的关联字段
	 */
	String inverseKey;

	/**
	 * from表的关联字段
	 */
	String localKey;

	/**
	 * 关联字段名，字段必须带有@{@link ManyToOne}注解，用这个配置可以不设置localKey和inverseKey
	 */
	String relation;

	/**
	 * from表查询字段
	 */
	@Singular("column")
	List<String> columns;

	/**
	 * 从表查询条件
	 */
	CriteriaDefinition criteria;

	public ExistsCriteria exists() {
		return ExistsCriteria.from(from).columns(columns).localKey(localKey).inverseKey(inverseKey).relation(relation)
				.criteria(criteria);
	}

}
