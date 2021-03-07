package org.springframework.data.jdbc.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.data.jdbc.repository.query.BoundCondition;
import org.springframework.data.jdbc.repository.query.UpdateMapper;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.mapping.ManyToMany;
import org.springframework.data.relational.core.mapping.ManyToOne;
import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.data.relational.core.mapping.OneToMany;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.query.CriteriaDefinition;
import org.springframework.data.relational.core.query.Query;
import org.springframework.data.relational.core.sql.Column;
import org.springframework.data.relational.core.sql.Delete;
import org.springframework.data.relational.core.sql.Expression;
import org.springframework.data.relational.core.sql.Insert;
import org.springframework.data.relational.core.sql.OrderByField;
import org.springframework.data.relational.core.sql.Select;
import org.springframework.data.relational.core.sql.SelectBuilder;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.core.sql.StatementBuilder;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.data.relational.core.sql.Update;
import org.springframework.data.relational.core.sql.render.RenderContext;
import org.springframework.data.relational.core.sql.render.SqlRenderer;
import org.springframework.data.util.Pair;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * 
 * @author Jerry Pan, NJUST
 *
 */
public class DefaultStatementMapper implements StatementMapper {

	private final Dialect dialect;

	private final UpdateMapper updateMapper;

	private final RenderContext renderContext;

	private final NamingStrategy namingStrategy;

	private final RelationalMappingContext mappingContext;

	public DefaultStatementMapper(Dialect dialect, RenderContext renderContext, UpdateMapper updateMapper,
			RelationalMappingContext mappingContext) {
		this.dialect = dialect;
		this.updateMapper = updateMapper;
		this.renderContext = renderContext;
		this.mappingContext = mappingContext;
		this.namingStrategy = this.mappingContext.getNamingStrategy();
	}

	@SuppressWarnings("unchecked")
	public <T> TypedStatementMapper<T> forType(Class<T> type) {

		Assert.notNull(type, "Type must not be null!");

		return new DefaultTypedStatementMapper<>(
				(RelationalPersistentEntity<T>) this.mappingContext.getRequiredPersistentEntity(type));
	}

	@Override
	public PreparedOperation<?> getMappedObject(SelectSpec selectSpec) {
		return (PreparedOperation<?>) getMappedObject(selectSpec, null);
	}

	@Override
	public PreparedOperation<?> getMappedObject(Query query) {
		return (PreparedOperation<?>) getMappedObject(query, null);
	}

	private PreparedOperation<?> getMappedObject(Query query, @Nullable RelationalPersistentEntity<?> entity) {
		Table table = Table.create(entity.getTableName());
		AtomicInteger atomicInteger = new AtomicInteger();
		MapSqlParameterSource sqlParameterSource = new MapSqlParameterSource();

		SelectBuilder.SelectAndFrom selectAndFrom = StatementBuilder.select(getSelectList(table, entity));

		SelectBuilder.SelectFromAndJoin selectBuilder = selectAndFrom.from(table);

		CriteriaDefinition criteria = query.getCriteria().orElse(null);

		Map<String, Table> tableMap = new HashMap<String, Table>();
		Map<String, Class<?>> clazzMap = new HashMap<String, Class<?>>();
		Pair<Map<String, Table>, Map<String, Class<?>>> pair = Pair.of(tableMap, clazzMap);

		if (criteria != null && !criteria.isEmpty()) {
			if (criteria.isGroup()) {
				CriteriaDefinition previous = criteria.getPrevious();

				if (previous != null) {
					unroll(selectBuilder, previous, table, entity, tableMap, clazzMap);
				}

				unrollGroup(selectBuilder, criteria.getGroup(), table, entity, tableMap, clazzMap);
			} else {
				unroll(selectBuilder, criteria, table, entity, tableMap, clazzMap);
			}

			BoundCondition mappedObject = this.updateMapper.getMappedObject(criteria, table, entity, sqlParameterSource,
					atomicInteger, pair);

			selectBuilder.where(mappedObject.getCondition());
		}

		if (query.isSorted()) {
			List<OrderByField> exchangeSort = this.updateMapper.getMappedSort(selectBuilder, table, query.getSort(),
					entity, pair);
			selectBuilder.orderBy(exchangeSort);
		}

		if (query.getLimit() > 0) {
			selectBuilder.limit(query.getLimit());
		}

		if (query.getOffset() > 0) {
			selectBuilder.offset(query.getOffset());
		}

		Select select = selectBuilder.build();
		return new DefaultPreparedOperation<Select>(select, sqlParameterSource, this.renderContext);
	}

	private PreparedOperation<?> getMappedObject(SelectSpec selectSpec,
			@Nullable RelationalPersistentEntity<?> entity) {

		Table table = selectSpec.getTable();
		AtomicInteger atomicInteger = new AtomicInteger();
		MapSqlParameterSource sqlParameterSource = new MapSqlParameterSource();
		SelectBuilder.SelectAndFrom selectAndFrom = StatementBuilder.select(getSelectList(selectSpec, entity));

		if (selectSpec.isDistinct()) {
			selectAndFrom = selectAndFrom.distinct();
		}

		SelectBuilder.SelectFromAndJoin selectBuilder = selectAndFrom.from(table);

		CriteriaDefinition criteria = selectSpec.getCriteria();
		Map<String, Table> tableMap = new HashMap<String, Table>();
		Map<String, Class<?>> clazzMap = new HashMap<String, Class<?>>();
		Pair<Map<String, Table>, Map<String, Class<?>>> pair = Pair.of(tableMap, clazzMap);

		if (criteria != null && !criteria.isEmpty()) {
			if (criteria.isGroup()) {
				CriteriaDefinition previous = criteria.getPrevious();

				if (previous != null) {
					unroll(selectBuilder, previous, table, entity, tableMap, clazzMap);
				}

				unrollGroup(selectBuilder, criteria.getGroup(), table, entity, tableMap, clazzMap);
			} else {
				unroll(selectBuilder, criteria, table, entity, tableMap, clazzMap);
			}

			BoundCondition mappedObject = this.updateMapper.getMappedObject(criteria, table, entity, sqlParameterSource,
					atomicInteger, pair);

			selectBuilder.where(mappedObject.getCondition());
		}

		if (selectSpec.getSort().isSorted()) {
			List<OrderByField> sort = this.updateMapper.getMappedSort(selectBuilder, table, selectSpec.getSort(),
					entity, pair);
			selectBuilder.orderBy(sort);
		}

		if (selectSpec.getLimit() > 0) {
			selectBuilder.limit(selectSpec.getLimit());
		}

		if (selectSpec.getOffset() > 0) {
			selectBuilder.offset(selectSpec.getOffset());
		}

		Select select = selectBuilder.build();
		return new DefaultPreparedOperation<>(select, sqlParameterSource, this.renderContext);
	}

	private void resolve(SelectBuilder.SelectFromAndJoin selectBuilder, CriteriaDefinition criteria, Table table,
			@Nullable RelationalPersistentEntity<?> entity, Map<String, Table> tableMap,
			Map<String, Class<?>> clazzMap) {
		String column = criteria.getColumn().toString();
		updateMapper.resolveColumn(selectBuilder, column, table, entity, tableMap, clazzMap);
	}

	private Pair<Map<String, Table>, Map<String, Class<?>>> unroll(SelectBuilder.SelectFromAndJoin selectBuilder,
			CriteriaDefinition criteria, Table table, @Nullable RelationalPersistentEntity<?> entity,
			Map<String, Table> tableMap, Map<String, Class<?>> clazzMap) {

		CriteriaDefinition current = criteria;

		// reverse unroll criteria chain
		Map<CriteriaDefinition, CriteriaDefinition> forwardChain = new HashMap<>();

		while (current.hasPrevious()) {
			forwardChain.put(current.getPrevious(), current);
			current = current.getPrevious();
		}

		if (forwardChain.size() == 0) {
			resolve(selectBuilder, criteria, table, entity, tableMap, clazzMap);
			return Pair.of(tableMap, clazzMap);
		}

		resolve(selectBuilder, current, table, entity, tableMap, clazzMap);

		while (forwardChain.containsKey(current)) {
			CriteriaDefinition criterion = forwardChain.get(current);

			if (criterion.isEmpty()) {
				current = criterion;
				continue;
			}

			if (criterion.isGroup()) {
				unrollGroup(selectBuilder, criterion.getGroup(), table, entity, tableMap, clazzMap);
				current = criterion;
				continue;
			}

			resolve(selectBuilder, criterion, table, entity, tableMap, clazzMap);

			current = criterion;
		}

		return Pair.of(tableMap, clazzMap);
	}

	private Pair<Map<String, Table>, Map<String, Class<?>>> unrollGroup(SelectBuilder.SelectFromAndJoin selectBuilder,
			List<? extends CriteriaDefinition> criteria, Table table, @Nullable RelationalPersistentEntity<?> entity,
			Map<String, Table> tableMap, Map<String, Class<?>> clazzMap) {

		for (CriteriaDefinition criterion : criteria) {

			if (criterion.isEmpty()) {
				continue;
			}

			unroll(selectBuilder, criterion, table, entity, tableMap, clazzMap);
		}

		return Pair.of(tableMap, clazzMap);
	}

	protected List<Expression> getSelectList(SelectSpec selectSpec, @Nullable RelationalPersistentEntity<?> entity) {

		if (entity == null) {
			return selectSpec.getSelectList();
		}

		List<Expression> selectList = selectSpec.getSelectList();
		List<Expression> mapped = new ArrayList<>(selectList.size());

		for (Expression expression : selectList) {
			mapped.add(updateMapper.getMappedObject(expression, entity));
		}

		return mapped;
	}

	protected List<Expression> getSelectList(Table table, @Nullable RelationalPersistentEntity<?> entity) {
		List<Expression> columnExpressions = new ArrayList<>();

		Iterator<RelationalPersistentProperty> iterator = entity.iterator();

		while (iterator.hasNext()) {
			RelationalPersistentProperty persistentProperty = iterator.next();

			if (persistentProperty.isEntity() || persistentProperty.isAnnotationPresent(ManyToOne.class)
					|| persistentProperty.isAnnotationPresent(OneToMany.class)
					|| persistentProperty.isAnnotationPresent(ManyToMany.class)) {
				continue;
			}

			String property = persistentProperty.getName();

			columnExpressions.add(Column.create(namingStrategy.getColumnName(property), table));
		}

		return columnExpressions;
	}

	protected String toSql(SqlIdentifier identifier) {

		Assert.notNull(identifier, "SqlIdentifier must not be null");

		return identifier.toSql(this.dialect.getIdentifierProcessing());
	}

	public RenderContext getRenderContext() {
		return renderContext;
	}

	static class DefaultPreparedOperation<T> implements PreparedOperation<T> {

		private final T source;
		private final RenderContext renderContext;
		private final SqlParameterSource sqlParameterSource;

		DefaultPreparedOperation(T source, SqlParameterSource sqlParameterSource, RenderContext renderContext) {
			this.source = source;
			this.renderContext = renderContext;
			this.sqlParameterSource = sqlParameterSource;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.springframework.data.r2dbc.function.PreparedOperation#getSource()
		 */
		@Override
		public T getSource() {
			return this.source;
		}

		@Override
		public SqlParameterSource getSqlParameterSource() {
			return this.sqlParameterSource;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.springframework.data.r2dbc.function.QueryOperation#toQuery()
		 */
		@Override
		public String toQuery() {

			SqlRenderer sqlRenderer = SqlRenderer.create(this.renderContext);

			if (this.source instanceof Select) {
				return sqlRenderer.render((Select) this.source);
			}

			if (this.source instanceof Insert) {
				return sqlRenderer.render((Insert) this.source);
			}

			if (this.source instanceof Update) {
				return sqlRenderer.render((Update) this.source);
			}

			if (this.source instanceof Delete) {
				return sqlRenderer.render((Delete) this.source);
			}

			throw new IllegalStateException("Cannot render " + this.getSource());
		}
	}

	class DefaultTypedStatementMapper<T> implements TypedStatementMapper<T> {

		final RelationalPersistentEntity<T> entity;

		DefaultTypedStatementMapper(RelationalPersistentEntity<T> entity) {
			this.entity = entity;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.springframework.data.r2dbc.function.StatementMapper#forType(java.lang.
		 * Class)
		 */
		@Override
		public <TC> TypedStatementMapper<TC> forType(Class<TC> type) {
			return DefaultStatementMapper.this.forType(type);
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.springframework.data.r2dbc.function.StatementMapper#getMappedObject(org.
		 * springframework.data.r2dbc.function.StatementMapper.SelectSpec)
		 */
		@Override
		public PreparedOperation<?> getMappedObject(SelectSpec selectSpec) {
			return DefaultStatementMapper.this.getMappedObject(selectSpec, this.entity);
		}

		@Override
		public PreparedOperation<?> getMappedObject(Query query) {
			return DefaultStatementMapper.this.getMappedObject(query, this.entity);
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.springframework.data.r2dbc.function.StatementMapper#getRenderContext()
		 */
		@Override
		public RenderContext getRenderContext() {
			return DefaultStatementMapper.this.getRenderContext();
		}
	}

}
