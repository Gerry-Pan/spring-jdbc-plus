package org.springframework.data.jdbc.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.data.jdbc.exception.SelectBuildException;
import org.springframework.data.jdbc.repository.query.BoundCondition;
import org.springframework.data.jdbc.repository.query.DefaultParametrizedQuery;
import org.springframework.data.jdbc.repository.query.ExistsCallback;
import org.springframework.data.jdbc.repository.query.UpdateMapper;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.mapping.ManyToMany;
import org.springframework.data.relational.core.mapping.ManyToOne;
import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.data.relational.core.mapping.OneToMany;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.query.CriteriaDefinition;
import org.springframework.data.relational.core.query.ExistsCriteria;
import org.springframework.data.relational.core.query.Query;
import org.springframework.data.relational.core.sql.Column;
import org.springframework.data.relational.core.sql.Condition;
import org.springframework.data.relational.core.sql.ExistsCondition;
import org.springframework.data.relational.core.sql.Expression;
import org.springframework.data.relational.core.sql.OrderByField;
import org.springframework.data.relational.core.sql.Select;
import org.springframework.data.relational.core.sql.SelectBuilder;
import org.springframework.data.relational.core.sql.SingleLiteral;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.core.sql.StatementBuilder;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.data.relational.core.sql.render.RenderContext;
import org.springframework.data.relational.core.sql.render.SqlRenderer;
import org.springframework.data.util.Pair;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

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
		ExistsCallback existsCallback = this::resolveExistsCriteria;
		updateMapper.setExistsCallback(existsCallback);

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
	public DefaultParametrizedQuery getMappedObject(SelectSpec selectSpec) {
		return getMappedObject(selectSpec, null);
	}

	@Override
	public DefaultParametrizedQuery getMappedObject(Query query) {
		return getMappedObject(query, null);
	}

	private DefaultParametrizedQuery getMappedObject(Query query, @Nullable RelationalPersistentEntity<?> entity) {
		Table table = Table.create(entity.getTableName());
		AtomicInteger atomicInteger = new AtomicInteger();
		MapSqlParameterSource sqlParameterSource = new MapSqlParameterSource();

		return getMappedObject(query, entity, table, sqlParameterSource, atomicInteger);
	}

	private DefaultParametrizedQuery getMappedObject(Query query, @Nullable RelationalPersistentEntity<?> entity,
			Table table, MapSqlParameterSource sqlParameterSource, AtomicInteger atomicInteger) {

		SelectBuilder.SelectAndFrom selectAndFrom = StatementBuilder.select(getSelectList(query, table, entity));

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
		SqlRenderer sqlRenderer = SqlRenderer.create(this.renderContext);

		return new DefaultParametrizedQuery(sqlRenderer.render(select), sqlParameterSource);
	}

	private DefaultParametrizedQuery getMappedObject(SelectSpec selectSpec,
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
		SqlRenderer sqlRenderer = SqlRenderer.create(this.renderContext);

		return new DefaultParametrizedQuery(sqlRenderer.render(select), sqlParameterSource);
	}

	@SuppressWarnings("unused")
	private DefaultParametrizedQuery getMappedObject(SelectSpec selectSpec,
			@Nullable RelationalPersistentEntity<?> entity, MapSqlParameterSource sqlParameterSource,
			AtomicInteger atomicInteger) {

		Table table = selectSpec.getTable();
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
		SqlRenderer sqlRenderer = SqlRenderer.create(this.renderContext);

		return new DefaultParametrizedQuery(sqlRenderer.render(select), sqlParameterSource);
	}

	private void resolve(SelectBuilder.SelectFromAndJoin selectBuilder, CriteriaDefinition criteria, Table table,
			@Nullable RelationalPersistentEntity<?> entity, Map<String, Table> tableMap,
			Map<String, Class<?>> clazzMap) {
		if (criteria instanceof ExistsCriteria) {
			return;
		}

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

	protected List<Expression> getSelectList(Query query, Table table, @Nullable RelationalPersistentEntity<?> entity) {
		List<Expression> columnExpressions = new ArrayList<>();
		List<SqlIdentifier> columns = query.getColumns();

		if (!CollectionUtils.isEmpty(columns)) {
			columns.forEach(column -> {
				columnExpressions.add(Column.create(column, table));
			});
		} else {
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
		}

		return columnExpressions;
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

	private Condition resolveExistsCriteria(RelationalPersistentEntity<?> entity, ExistsCriteria existsCriteria,
			MapSqlParameterSource sqlParameterSource, AtomicInteger atomicInteger) {
		Class<?> subClass = existsCriteria.getFrom();
		List<SqlIdentifier> columns = existsCriteria.getColumns();
		RelationalPersistentEntity<?> subEntity = mappingContext.getRequiredPersistentEntity(subClass);

		Table table = Table.create(entity.getTableName());
		Table subTable = Table.create(subEntity.getTableName()).as("T");

		String localKey = existsCriteria.getLocalKey();
		String inverseKey = existsCriteria.getInverseKey();
		String relation = existsCriteria.getRelation();

		Criteria criteria = (Criteria) existsCriteria.getCriteria();

		if (criteria == null) {
			criteria = Criteria.empty();
		}

		boolean f = Boolean.FALSE;
		if (StringUtils.hasText(relation)) {
			f = Boolean.TRUE;

			java.lang.reflect.Field field = ReflectionUtils.findField(subClass, relation);

			ManyToOne manyToOne = field.getAnnotation(ManyToOne.class);

			if (manyToOne == null) {
				throw new SelectBuildException("Not found ManyToOne in " + relation + " of " + subClass.getName());
			}

			String property = manyToOne.property();
			String col = namingStrategy.getColumnName(property);
			String idProperty = entity.getIdProperty().getName();

			criteria = criteria.and(col).is(new SingleLiteral(table.toString() + "." + idProperty));
		}

		if (!f && StringUtils.hasText(localKey) && StringUtils.hasText(inverseKey)) {
			criteria = criteria.and(localKey).is(new SingleLiteral(table.toString() + "." + inverseKey));
		}

		Query subQuery = Query.query(criteria).columns(columns.toArray(new SqlIdentifier[columns.size()]));

		DefaultParametrizedQuery subParametrizedQuery = getMappedObject(subQuery, subEntity, subTable,
				sqlParameterSource, atomicInteger);

		return new ExistsCondition(subParametrizedQuery.toString());
	}

	public RenderContext getRenderContext() {
		return renderContext;
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
		public DefaultParametrizedQuery getMappedObject(SelectSpec selectSpec) {
			return DefaultStatementMapper.this.getMappedObject(selectSpec, this.entity);
		}

		@Override
		public DefaultParametrizedQuery getMappedObject(Query query) {
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
