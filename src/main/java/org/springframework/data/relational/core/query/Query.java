package org.springframework.data.relational.core.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

public class Query {

	private final @Nullable CriteriaDefinition criteria;

	private final List<SqlIdentifier> columns;
	private final Sort sort;
	private final int limit;
	private final long offset;

	/**
	 * 数据库表做了分表时使用
	 */
	private final SqlIdentifier table;

	/**
	 * Static factory method to create a {@link Query} using the provided
	 * {@link CriteriaDefinition}.
	 *
	 * @param criteria must not be {@literal null}.
	 * @return a new {@link Query} for the given {@link Criteria}.
	 */
	public static Query query(CriteriaDefinition criteria) {
		return new Query(criteria);
	}

	/**
	 * Creates a new {@link Query} using the given {@link Criteria}.
	 *
	 * @param criteria must not be {@literal null}.
	 */
	private Query(@Nullable CriteriaDefinition criteria) {
		this(criteria, Collections.emptyList(), Sort.unsorted(), -1, -1, null);
	}

	private Query(@Nullable CriteriaDefinition criteria, List<SqlIdentifier> columns, Sort sort, int limit, long offset,
			SqlIdentifier table) {

		this.criteria = criteria;
		this.columns = columns;
		this.sort = sort;
		this.limit = limit;
		this.offset = offset;
		this.table = table;
	}

	/**
	 * Create a new empty {@link Query}.
	 *
	 * @return
	 */
	public static Query empty() {
		return new Query(null);
	}

	/**
	 * Add columns to the query.
	 *
	 * @param columns
	 * @return a new {@link Query} object containing the former settings with
	 *         {@code columns} applied.
	 */
	public Query columns(String... columns) {

		Assert.notNull(columns, "Columns must not be null");

		return withColumns(Arrays.stream(columns).map(SqlIdentifier::unquoted).collect(Collectors.toList()));
	}

	/**
	 * Add columns to the query.
	 *
	 * @param columns
	 * @return a new {@link Query} object containing the former settings with
	 *         {@code columns} applied.
	 */
	public Query columns(Collection<String> columns) {

		Assert.notNull(columns, "Columns must not be null");

		return withColumns(columns.stream().map(SqlIdentifier::unquoted).collect(Collectors.toList()));
	}

	/**
	 * Add columns to the query.
	 *
	 * @param columns
	 * @return a new {@link Query} object containing the former settings with
	 *         {@code columns} applied.
	 * @since 1.1
	 */
	public Query columns(SqlIdentifier... columns) {

		Assert.notNull(columns, "Columns must not be null");

		return withColumns(Arrays.asList(columns));
	}

	/**
	 * Add columns to the query.
	 *
	 * @param columns
	 * @return a new {@link Query} object containing the former settings with
	 *         {@code columns} applied.
	 */
	private Query withColumns(Collection<SqlIdentifier> columns) {

		Assert.notNull(columns, "Columns must not be null");

		List<SqlIdentifier> newColumns = new ArrayList<>(this.columns);
		newColumns.addAll(columns);
		return new Query(this.criteria, newColumns, this.sort, this.limit, offset, this.table);
	}

	/**
	 * 数据库表做了分表时使用
	 * 
	 * @param table
	 * @return
	 */
	public Query table(String table) {
		return new Query(this.criteria, this.columns, this.sort, this.limit, this.offset,
				SqlIdentifier.unquoted(table));
	}

	/**
	 * 数据库表做了分表时使用
	 * 
	 * @param table
	 * @return
	 */
	public Query table(SqlIdentifier table) {
		return new Query(this.criteria, this.columns, this.sort, this.limit, this.offset, table);
	}

	/**
	 * Set number of rows to skip before returning results.
	 *
	 * @param offset
	 * @return a new {@link Query} object containing the former settings with
	 *         {@code offset} applied.
	 */
	public Query offset(long offset) {
		return new Query(this.criteria, this.columns, this.sort, this.limit, offset, this.table);
	}

	/**
	 * Limit the number of returned documents to {@code limit}.
	 *
	 * @param limit
	 * @return a new {@link Query} object containing the former settings with
	 *         {@code limit} applied.
	 */
	public Query limit(int limit) {
		return new Query(this.criteria, this.columns, this.sort, limit, this.offset, this.table);
	}

	/**
	 * Set the given pagination information on the {@link Query} instance. Will
	 * transparently set {@code offset} and {@code limit} as well as applying the
	 * {@link Sort} instance defined with the {@link Pageable}.
	 *
	 * @param pageable
	 * @return a new {@link Query} object containing the former settings with
	 *         {@link Pageable} applied.
	 */
	public Query with(Pageable pageable) {

		if (pageable.isUnpaged()) {
			return this;
		}

		assertNoCaseSort(pageable.getSort());

		return new Query(this.criteria, this.columns, this.sort.and(sort), pageable.getPageSize(), pageable.getOffset(),
				this.table);
	}

	/**
	 * Add a {@link Sort} to the {@link Query} instance.
	 *
	 * @param sort
	 * @return a new {@link Query} object containing the former settings with
	 *         {@link Sort} applied.
	 */
	public Query sort(Sort sort) {

		Assert.notNull(sort, "Sort must not be null!");

		if (sort.isUnsorted()) {
			return this;
		}

		assertNoCaseSort(sort);

		return new Query(this.criteria, this.columns, this.sort.and(sort), this.limit, this.offset, this.table);
	}

	/**
	 * Return the {@link Criteria} to be applied.
	 *
	 * @return
	 */
	public Optional<CriteriaDefinition> getCriteria() {
		return Optional.ofNullable(this.criteria);
	}

	/**
	 * Return the columns that this query should project.
	 *
	 * @return
	 */
	public List<SqlIdentifier> getColumns() {
		return columns;
	}

	/**
	 * Return {@literal true} if the {@link Query} has a sort parameter.
	 *
	 * @return {@literal true} if sorted.
	 * @see Sort#isSorted()
	 */
	public boolean isSorted() {
		return sort.isSorted();
	}

	public Sort getSort() {
		return sort;
	}

	/**
	 * Return the number of rows to skip.
	 *
	 * @return
	 */
	public long getOffset() {
		return this.offset;
	}

	/**
	 * Return the maximum number of rows to be return.
	 *
	 * @return
	 */
	public int getLimit() {
		return this.limit;
	}

	/**
	 * 数据库表做了分表时使用
	 * 
	 * @return
	 */
	public SqlIdentifier getTable() {
		return this.table;
	}

	private static void assertNoCaseSort(Sort sort) {

		for (Sort.Order order : sort) {
			if (order.isIgnoreCase()) {
				throw new IllegalArgumentException(
						String.format("Given sort contained an Order for %s with ignore case;"
								+ "Ignore case sorting is not supported", order.getProperty()));
			}
		}
	}
}
