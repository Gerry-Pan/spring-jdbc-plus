package org.springframework.data.relational.core.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.data.relational.core.mapping.ManyToOne;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.util.Assert;

import lombok.Getter;

public class ExistsCriteria implements CriteriaDefinition {

	/**
	 * 子查询from的表
	 */
	@Getter
	final Class<?> from;

	/**
	 * 关联表的关联字段
	 */
	@Getter
	final String inverseKey;

	/**
	 * from表的关联字段
	 */
	@Getter
	final String localKey;

	/**
	 * 关联字段名，字段必须带有@{@link ManyToOne}注解，用这个配置可以不设置localKey和inverseKey
	 */
	@Getter
	final String relation;

	/**
	 * 从表查询字段
	 */
	@Getter
	final List<SqlIdentifier> columns;

	/**
	 * 从表查询条件
	 */
	@Getter
	final CriteriaDefinition criteria;

	private ExistsCriteria(Class<?> from) {
		this(null, from, null, null, Collections.emptyList(), Criteria.EMPTY);
	}

	private ExistsCriteria(String inverseKey, Class<?> from, String localKey, String relation,
			List<SqlIdentifier> columns, CriteriaDefinition criteria) {
		this.from = from;
		this.columns = columns;
		this.relation = relation;
		this.criteria = criteria;
		this.localKey = localKey;
		this.inverseKey = inverseKey;
	}

	public ExistsCriteria columns(String... columns) {
		Assert.notNull(columns, "Criteria must not be null");

		List<SqlIdentifier> _columns = Arrays.stream(columns).map(SqlIdentifier::unquoted).collect(Collectors.toList());
		List<SqlIdentifier> newColumns = new ArrayList<>(this.columns);
		newColumns.addAll(_columns);
		return new ExistsCriteria(inverseKey, from, localKey, relation, newColumns, criteria);
	}

	public ExistsCriteria columns(List<String> columns) {
		Assert.notNull(columns, "Criteria must not be null");
		Assert.noNullElements(columns, "Criteria must not contain null elements");

		List<SqlIdentifier> _columns = columns.stream().map(SqlIdentifier::unquoted).collect(Collectors.toList());
		List<SqlIdentifier> newColumns = new ArrayList<>(this.columns);
		newColumns.addAll(_columns);
		return new ExistsCriteria(inverseKey, from, localKey, relation, newColumns, criteria);
	}

	public static ExistsCriteria from(Class<?> from) {
		return new ExistsCriteria(from);
	}

	public ExistsCriteria inverseKey(String inverseKey) {
		return new ExistsCriteria(inverseKey, from, localKey, relation, columns, criteria);
	}

	public ExistsCriteria localKey(String localKey) {
		return new ExistsCriteria(inverseKey, from, localKey, relation, columns, criteria);
	}

	public ExistsCriteria relation(String relation) {
		return new ExistsCriteria(inverseKey, from, localKey, relation, columns, criteria);
	}

	public ExistsCriteria criteria(CriteriaDefinition criteria) {
		return new ExistsCriteria(inverseKey, from, localKey, relation, columns, criteria);
	}

	@Override
	public boolean isGroup() {
		return false;
	}

	@Override
	public List<CriteriaDefinition> getGroup() {
		return null;
	}

	@Override
	public SqlIdentifier getColumn() {
		return null;
	}

	@Override
	public Comparator getComparator() {
		return null;
	}

	@Override
	public Object getValue() {
		return null;
	}

	@Override
	public boolean isIgnoreCase() {
		return false;
	}

	@Override
	public CriteriaDefinition getPrevious() {
		return null;
	}

	@Override
	public boolean hasPrevious() {
		return false;
	}

	@Override
	public boolean isEmpty() {
		return false;
	}

	@Override
	public Combinator getCombinator() {
		return null;
	}

}
