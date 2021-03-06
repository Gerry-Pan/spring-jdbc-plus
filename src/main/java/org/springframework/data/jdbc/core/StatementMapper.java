package org.springframework.data.jdbc.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.query.CriteriaDefinition;
import org.springframework.data.relational.core.query.Query;
import org.springframework.data.relational.core.sql.Expression;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.data.relational.core.sql.render.RenderContext;
import org.springframework.lang.Nullable;

public interface StatementMapper {

	<T> TypedStatementMapper<T> forType(Class<T> type);

	PreparedOperation<?> getMappedObject(SelectSpec selectSpec);
	
	PreparedOperation<?> getMappedObject(Query query);

	default SelectSpec createSelect(String table) {
		return SelectSpec.create(table);
	}

	default SelectSpec createSelect(SqlIdentifier table) {
		return SelectSpec.create(table);
	}

	default RenderContext getRenderContext() {
		return null;
	}

	interface TypedStatementMapper<T> extends StatementMapper {
	}

	/**
	 * {@code SELECT} SelectSpec.
	 */
	public static class SelectSpec {

		private final Table table;
		private final List<String> projectedFields;
		private final List<Expression> selectList;
		private final @Nullable CriteriaDefinition criteria;
		private final Sort sort;
		private final long offset;
		private final int limit;
		private final boolean distinct;

		protected SelectSpec(Table table, List<String> projectedFields, List<Expression> selectList,
				@Nullable CriteriaDefinition criteria, Sort sort, int limit, long offset, boolean distinct) {
			this.table = table;
			this.projectedFields = projectedFields;
			this.selectList = selectList;
			this.criteria = criteria;
			this.sort = sort;
			this.offset = offset;
			this.limit = limit;
			this.distinct = distinct;
		}

		/**
		 * Create an {@code SELECT} specification for {@code table}.
		 *
		 * @param table
		 * @return the {@link SelectSpec}.
		 */
		public static SelectSpec create(String table) {
			return create(SqlIdentifier.unquoted(table));
		}

		/**
		 * Create an {@code SELECT} specification for {@code table}.
		 *
		 * @param table
		 * @return the {@link SelectSpec}.
		 * @since 1.1
		 */
		public static SelectSpec create(SqlIdentifier table) {

			List<String> projectedFields = Collections.emptyList();
			List<Expression> selectList = Collections.emptyList();
			return new SelectSpec(Table.create(table), projectedFields, selectList, Criteria.empty(), Sort.unsorted(),
					-1, -1, false);
		}

		public SelectSpec doWithTable(BiFunction<Table, SelectSpec, SelectSpec> function) {
			return function.apply(getTable(), this);
		}

		/**
		 * Associate {@code projectedFields} with the select and create a new
		 * {@link SelectSpec}.
		 *
		 * @param projectedFields
		 * @return the {@link SelectSpec}.
		 * @since 1.1
		 */
		public SelectSpec withProjection(String... projectedFields) {
			return withProjection(Arrays.stream(projectedFields).map(table::column).collect(Collectors.toList()));
		}

		/**
		 * Associate {@code projectedFields} with the select and create a new
		 * {@link SelectSpec}.
		 *
		 * @param projectedFields
		 * @return the {@link SelectSpec}.
		 * @since 1.1
		 */
		public SelectSpec withProjection(SqlIdentifier... projectedFields) {
			return withProjection(Arrays.stream(projectedFields).map(table::column).collect(Collectors.toList()));
		}

		/**
		 * Associate {@code expressions} with the select list and create a new
		 * {@link SelectSpec}.
		 *
		 * @param expressions
		 * @return the {@link SelectSpec}.
		 * @since 1.1
		 */
		public SelectSpec withProjection(Expression... expressions) {

			List<Expression> selectList = new ArrayList<>(this.selectList);
			selectList.addAll(Arrays.asList(expressions));

			return new SelectSpec(this.table, projectedFields, selectList, this.criteria, this.sort, this.limit,
					this.offset, this.distinct);
		}

		/**
		 * Associate {@code projectedFields} with the select and create a new
		 * {@link SelectSpec}.
		 *
		 * @param projectedFields
		 * @return the {@link SelectSpec}.
		 * @since 1.1
		 */
		public SelectSpec withProjection(Collection<Expression> projectedFields) {

			List<Expression> selectList = new ArrayList<>(this.selectList);
			selectList.addAll(projectedFields);

			return new SelectSpec(this.table, this.projectedFields, selectList, this.criteria, this.sort, this.limit,
					this.offset, this.distinct);
		}

		/**
		 * Associate a {@link Criteria} with the select and return a new
		 * {@link SelectSpec}.
		 *
		 * @param criteria
		 * @return the {@link SelectSpec}.
		 */
		public SelectSpec withCriteria(CriteriaDefinition criteria) {
			return new SelectSpec(this.table, this.projectedFields, this.selectList, criteria, this.sort, this.limit,
					this.offset, this.distinct);
		}

		/**
		 * Associate {@link Sort} with the select and create a new {@link SelectSpec}.
		 *
		 * @param sort
		 * @return the {@link SelectSpec}.
		 */
		public SelectSpec withSort(Sort sort) {

			if (sort.isSorted()) {
				return new SelectSpec(this.table, this.projectedFields, this.selectList, this.criteria, sort,
						this.limit, this.offset, this.distinct);
			}

			return new SelectSpec(this.table, this.projectedFields, this.selectList, this.criteria, this.sort,
					this.limit, this.offset, this.distinct);
		}

		/**
		 * Associate a {@link Pageable} with the select and create a new
		 * {@link SelectSpec}.
		 *
		 * @param page
		 * @return the {@link SelectSpec}.
		 */
		public SelectSpec withPage(Pageable page) {

			if (page.isPaged()) {

				Sort sort = page.getSort();

				return new SelectSpec(this.table, this.projectedFields, this.selectList, this.criteria,
						sort.isSorted() ? sort : this.sort, page.getPageSize(), page.getOffset(), this.distinct);
			}

			return new SelectSpec(this.table, this.projectedFields, this.selectList, this.criteria, this.sort,
					this.limit, this.offset, this.distinct);
		}

		/**
		 * Associate a result offset with the select and create a new
		 * {@link SelectSpec}.
		 *
		 * @param offset
		 * @return the {@link SelectSpec}.
		 */
		public SelectSpec offset(long offset) {
			return new SelectSpec(this.table, this.projectedFields, this.selectList, this.criteria, this.sort,
					this.limit, offset, this.distinct);
		}

		/**
		 * Associate a result limit with the select and create a new {@link SelectSpec}.
		 *
		 * @param limit
		 * @return the {@link SelectSpec}.
		 */
		public SelectSpec limit(int limit) {
			return new SelectSpec(this.table, this.projectedFields, this.selectList, this.criteria, this.sort, limit,
					this.offset, this.distinct);
		}

		/**
		 * Associate a result statement distinct with the select and create a new
		 * {@link SelectSpec}.
		 *
		 * @return the {@link SelectSpec}.
		 */
		public SelectSpec distinct() {
			return new SelectSpec(this.table, this.projectedFields, this.selectList, this.criteria, this.sort, limit,
					this.offset, true);
		}

		public Table getTable() {
			return this.table;
		}

		/**
		 * @return
		 * @deprecated since 1.1, use {@link #getSelectList()} instead.
		 */
		@Deprecated
		public List<String> getProjectedFields() {
			return Collections.unmodifiableList(this.projectedFields);
		}

		public List<Expression> getSelectList() {
			return Collections.unmodifiableList(selectList);
		}

		@Nullable
		public CriteriaDefinition getCriteria() {
			return this.criteria;
		}

		public Sort getSort() {
			return this.sort;
		}

		public long getOffset() {
			return this.offset;
		}

		public int getLimit() {
			return this.limit;
		}

		public boolean isDistinct() {
			return this.distinct;
		}
	}

}
