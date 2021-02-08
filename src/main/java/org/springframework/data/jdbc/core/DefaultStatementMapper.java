package org.springframework.data.jdbc.core;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Currency;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.data.jdbc.exception.SelectBuildException;
import org.springframework.data.jdbc.repository.query.BoundCondition;
import org.springframework.data.jdbc.repository.query.UpdateMapper;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.mapping.ManyToMany;
import org.springframework.data.relational.core.mapping.ManyToOne;
import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.data.relational.core.mapping.OneToMany;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.query.CriteriaDefinition;
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
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

/**
 * 
 * @author Jerry Pan, NJUST
 *
 */
public class DefaultStatementMapper implements StatementMapper {

	private final Class<?>[] clazzs = new Class<?>[] { Long.class, Integer.class, Double.class, Float.class, Byte.class,
			Short.class, Boolean.class, Character.class, String.class, byte[].class, BigDecimal.class, Byte[].class,
			Date.class, java.sql.Date.class, java.sql.Time.class, java.sql.Timestamp.class, Calendar.class,
			java.sql.Clob.class, java.sql.Blob.class, Serializable.class, Locale.class, TimeZone.class, Currency.class,
			Class.class };

	private final Dialect dialect;

	private final UpdateMapper updateMapper;

	private final RenderContext renderContext;

	private final RelationalMappingContext mappingContext;

	public DefaultStatementMapper(Dialect dialect, RenderContext renderContext, UpdateMapper updateMapper,
			RelationalMappingContext mappingContext) {
		this.dialect = dialect;
		this.updateMapper = updateMapper;
		this.renderContext = renderContext;
		this.mappingContext = mappingContext;
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

		if (criteria != null && !criteria.isEmpty()) {
			Map<String, Table> tableMap = new HashMap<String, Table>();
			Map<String, Class<?>> clazzMap = new HashMap<String, Class<?>>();

			if (criteria.isGroup()) {
				CriteriaDefinition previous = criteria.getPrevious();

				if (previous != null) {
					unroll(selectBuilder, previous, table, entity, tableMap, clazzMap);
				}

				unrollGroup(selectBuilder, criteria.getGroup(), table, entity, tableMap, clazzMap);
			} else {
				unroll(selectBuilder, criteria, table, entity, tableMap, clazzMap);
			}

			Pair<Map<String, Table>, Map<String, Class<?>>> pair = Pair.of(tableMap, clazzMap);

			BoundCondition mappedObject = this.updateMapper.getMappedObject(criteria, table, entity, sqlParameterSource,
					atomicInteger, pair);

			selectBuilder.where(mappedObject.getCondition());
		}

		if (selectSpec.getSort().isSorted()) {
			List<OrderByField> sort = this.updateMapper.getMappedSort(table, selectSpec.getSort(), entity);
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

		if (column.indexOf(".") != -1) {
			String[] names = column.split("\\.");
			NamingStrategy namingStrategy = mappingContext.getNamingStrategy();

			Table left = table;
			RelationalPersistentEntity<?> leftEntity = entity;

			for (int i = 0; i < names.length - 1; i++) {
				String name = names[i];
				Field field = ReflectionUtils.findField(leftEntity.getType(), name);
				Class<?> clazz = field.getType();

				if (isPrimitive(clazz)) {
					throw new SelectBuildException("Doesn't support Primitive class for " + name + " in " + column);
				}

				OneToMany oneToMany = null;
				ManyToOne manyToOne = null;
				ManyToMany manyToMany = null;

				if (isInterface(clazz, Iterable.class.getName())) {
					oneToMany = field.getAnnotation(OneToMany.class);
					manyToMany = field.getAnnotation(ManyToMany.class);

					if (oneToMany == null && manyToMany == null) {
						throw new SelectBuildException(
								"Not found OneToMany or ManyToMany for " + name + " in " + column);
					}

					if (oneToMany != null && manyToMany != null) {
						throw new SelectBuildException(
								"Either OneToMany or ManyToMany can be used for" + name + " in " + column);
					}

					ParameterizedType parameterizedType = (ParameterizedType) field.getGenericType();

					Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();

					if (actualTypeArguments == null || actualTypeArguments.length == 0) {
						throw new SelectBuildException("Has no ParameterizedType for " + name + " in " + column);
					}

					if (actualTypeArguments.length > 1) {
						throw new SelectBuildException("Too many ParameterizedType");
					}

					clazz = (Class<?>) actualTypeArguments[0];
				} else {
					manyToOne = field.getAnnotation(ManyToOne.class);

					if (manyToOne == null) {
						throw new SelectBuildException("Not found ManyToOne for " + name + " in " + column);
					}
				}

				if (oneToMany != null && manyToOne != null) {
					throw new SelectBuildException(
							"Either OneToMany or ManyToOne can be used for" + name + " in " + column);
				}

				if (manyToMany != null && manyToOne != null) {
					throw new SelectBuildException(
							"Either ManyToMany or ManyToOne can be used for" + name + " in " + column);
				}

				Table right = null;
				RelationalPersistentEntity<?> rightEntity = mappingContext.getRequiredPersistentEntity(clazz);

				if (rightEntity == null) {
					throw new SelectBuildException("Has no PersistentEntity for " + name + " in " + column);
				}

				StringBuilder sb = new StringBuilder();

				for (int j = 0; j <= i; j++) {
					sb.append(names[j]);

					if (j < i) {
						sb.append(".");
					}
				}

				String tableName = sb.toString();

				if (tableMap.containsKey(tableName)) {
					right = tableMap.get(tableName);
				} else {
					right = Table.create(rightEntity.getTableName());

					if (manyToOne != null) {
						String property = manyToOne.property();
						String col = namingStrategy.getColumnName(property);
						String idProperty = rightEntity.getIdProperty().getName();

						selectBuilder.leftOuterJoin(right).on(Column.create(SqlIdentifier.quoted(col), left))
								.equals(Column.create(SqlIdentifier.quoted(idProperty), right)).build();
					}

					if (oneToMany != null) {
						String mappedBy = oneToMany.mappedBy();
						String idProperty = leftEntity.getIdProperty().getName();

						Field rightField = ReflectionUtils.findField(clazz, mappedBy);
						ManyToOne mto = rightField.getAnnotation(ManyToOne.class);

						String col = null;

						if (mto == null) {
							String property = mappedBy.concat("Id");

							col = namingStrategy.getColumnName(property);
						} else {
							String property = mto.property();

							col = namingStrategy.getColumnName(property);
						}

						selectBuilder.leftOuterJoin(right).on(Column.create(SqlIdentifier.quoted(idProperty), left))
								.equals(Column.create(SqlIdentifier.quoted(col), right)).build();
					}

					if (manyToMany != null) {
						boolean f = false;
						String mappedBy = manyToMany.mappedBy();
						String reference = manyToMany.table();
						String localColumn = manyToMany.column();
						String inverseColumn = manyToMany.inverseColumn();

						if (StringUtils.hasText(reference)) {
							if (!StringUtils.hasText(localColumn)) {
								throw new SelectBuildException(
										"Not found localColumn in ManyToMany for " + name + " in " + column);
							}

							if (!StringUtils.hasText(inverseColumn)) {
								throw new SelectBuildException(
										"Not found inverseColumn in ManyToMany for " + name + " in " + column);
							}

							Table middle = Table.create(reference);

							String leftIdProperty = leftEntity.getIdProperty().getName();
							String rightIdProperty = rightEntity.getIdProperty().getName();

							String lc = namingStrategy.getColumnName(localColumn);
							String ic = namingStrategy.getColumnName(inverseColumn);

							selectBuilder.leftOuterJoin(middle)
									.on(Column.create(SqlIdentifier.quoted(leftIdProperty), left))
									.equals(Column.create(SqlIdentifier.quoted(lc), middle)).build();

							selectBuilder.leftOuterJoin(right).on(Column.create(SqlIdentifier.quoted(ic), middle))
									.equals(Column.create(SqlIdentifier.quoted(rightIdProperty), right)).build();

							f = true;
						}

						if (!f && StringUtils.hasText(mappedBy)) {
							Field rightField = ReflectionUtils.findField(clazz, mappedBy);
							ManyToMany mtm = rightField.getAnnotation(ManyToMany.class);

							reference = mtm.table();
							localColumn = mtm.column();
							inverseColumn = mtm.inverseColumn();

							Table middle = Table.create(reference);

							String leftIdProperty = leftEntity.getIdProperty().getName();
							String rightIdProperty = rightEntity.getIdProperty().getName();

							String lc = namingStrategy.getColumnName(localColumn);
							String ic = namingStrategy.getColumnName(inverseColumn);

							selectBuilder.leftOuterJoin(middle)
									.on(Column.create(SqlIdentifier.quoted(leftIdProperty), left))
									.equals(Column.create(SqlIdentifier.quoted(ic), middle)).build();

							selectBuilder.leftOuterJoin(right).on(Column.create(SqlIdentifier.quoted(lc), middle))
									.equals(Column.create(SqlIdentifier.quoted(rightIdProperty), right)).build();

							f = true;
						}

						if (!f) {
							throw new SelectBuildException(
									"Must set table or mappedBy in ManyToMany for " + name + " in " + column);
						}
					}

					tableMap.put(tableName, right);
				}

				clazzMap.put(tableName, clazz);

				left = right;
				leftEntity = rightEntity;
			}
		}
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

	protected String toSql(SqlIdentifier identifier) {

		Assert.notNull(identifier, "SqlIdentifier must not be null");

		return identifier.toSql(this.dialect.getIdentifierProcessing());
	}

	private boolean isInterface(Class<?> c, String szInterface) {
		Class<?>[] face = c.getInterfaces();
		for (int i = 0, j = face.length; i < j; i++) {
			if (face[i].getName().equals(szInterface)) {
				return true;
			} else {
				Class<?>[] face1 = face[i].getInterfaces();
				for (int x = 0; x < face1.length; x++) {
					if (face1[x].getName().equals(szInterface)) {
						return true;
					} else if (isInterface(face1[x], szInterface)) {
						return true;
					}
				}
			}
		}
		if (null != c.getSuperclass()) {
			return isInterface(c.getSuperclass(), szInterface);
		}
		return false;
	}

	public boolean isPrimitive(Class<?> clazz) {
		boolean flag = false;

		if (clazz.isPrimitive()) {
			return true;
		}

		for (Class<?> _clazz : this.clazzs) {
			if (clazz.isAssignableFrom(_clazz)) {
				flag = true;
				break;
			}
		}

		return flag;
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
