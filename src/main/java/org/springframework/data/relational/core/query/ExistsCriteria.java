package org.springframework.data.relational.core.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.data.relational.core.sql.SqlIdentifier;

import lombok.Getter;

public class ExistsCriteria implements CriteriaDefinition {

	/**
	 * 子查询from的表
	 */
	@Getter
	final Class<?> table;

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
	 * 从表查询字段
	 */
	@Getter
	final List<SqlIdentifier> columns;

	/**
	 * 从表查询条件
	 */
	@Getter
	final CriteriaDefinition criteria;

	private ExistsCriteria(Class<?> table) {
		this(null, table, null, Collections.emptyList(), Criteria.EMPTY);
	}

	private ExistsCriteria(String inverseKey, Class<?> table, String localKey, List<SqlIdentifier> columns,
			CriteriaDefinition criteria) {
		this.table = table;
		this.columns = columns;
		this.criteria = criteria;
		this.localKey = localKey;
		this.inverseKey = inverseKey;
	}

	public ExistsCriteria columns(String... columns) {
		List<SqlIdentifier> _columns = Arrays.stream(columns).map(SqlIdentifier::unquoted).collect(Collectors.toList());
		List<SqlIdentifier> newColumns = new ArrayList<>(this.columns);
		newColumns.addAll(_columns);
		return new ExistsCriteria(inverseKey, table, localKey, newColumns, criteria);
	}

	public static ExistsCriteria from(Class<?> table) {
		return new ExistsCriteria(table);
	}

	public ExistsCriteria inverseKey(String inverseKey) {
		return new ExistsCriteria(inverseKey, table, localKey, columns, criteria);
	}

	public ExistsCriteria localKey(String localKey) {
		return new ExistsCriteria(inverseKey, table, localKey, columns, criteria);
	}

	public ExistsCriteria criteria(CriteriaDefinition criteria) {
		return new ExistsCriteria(inverseKey, table, localKey, columns, criteria);
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
