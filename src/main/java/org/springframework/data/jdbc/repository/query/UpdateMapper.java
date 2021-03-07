package org.springframework.data.jdbc.repository.query;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Currency;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.data.domain.Sort;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.exception.SelectBuildException;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.dialect.Escaper;
import org.springframework.data.relational.core.mapping.ManyToMany;
import org.springframework.data.relational.core.mapping.ManyToOne;
import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.data.relational.core.mapping.OneToMany;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.query.CriteriaDefinition;
import org.springframework.data.relational.core.query.CriteriaDefinition.Combinator;
import org.springframework.data.relational.core.query.CriteriaDefinition.Comparator;
import org.springframework.data.relational.core.query.ValueFunction;
import org.springframework.data.relational.core.sql.Column;
import org.springframework.data.relational.core.sql.Condition;
import org.springframework.data.relational.core.sql.Conditions;
import org.springframework.data.relational.core.sql.Expression;
import org.springframework.data.relational.core.sql.Functions;
import org.springframework.data.relational.core.sql.OrderByField;
import org.springframework.data.relational.core.sql.ReferenceColumn;
import org.springframework.data.relational.core.sql.SQL;
import org.springframework.data.relational.core.sql.SelectBuilder;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.Pair;
import org.springframework.data.util.TypeInformation;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

public class UpdateMapper extends QueryMapper {

	private final Class<?>[] clazzs = new Class<?>[] { Long.class, Integer.class, Double.class, Float.class, Byte.class,
			Short.class, Boolean.class, Character.class, String.class, byte[].class, BigDecimal.class, Byte[].class,
			Date.class, java.sql.Date.class, java.sql.Time.class, java.sql.Timestamp.class, Calendar.class,
			java.sql.Clob.class, java.sql.Blob.class, Serializable.class, Locale.class, TimeZone.class, Currency.class,
			Class.class };

	private final Dialect dialect;

	private final JdbcConverter converter;

	private final RelationalMappingContext mappingContext;

	public UpdateMapper(Dialect dialect, JdbcConverter converter, RelationalMappingContext mappingContext) {
		super(dialect, converter);
		this.dialect = dialect;
		this.converter = converter;
		this.mappingContext = mappingContext;
	}

	public BoundCondition getMappedObject(CriteriaDefinition criteria, Table table,
			@Nullable RelationalPersistentEntity<?> entity, MapSqlParameterSource sqlParameterSource,
			AtomicInteger atomicInteger) {

		Assert.notNull(criteria, "CriteriaDefinition must not be null!");
		Assert.notNull(table, "Table must not be null!");

		if (criteria.isEmpty()) {
			throw new IllegalArgumentException("Cannot map empty Criteria");
		}

		Condition mapped = unroll(criteria, table, entity, sqlParameterSource, atomicInteger);

		return new BoundCondition(sqlParameterSource, mapped);
	}

	public BoundCondition getMappedObject(CriteriaDefinition criteria, Table table,
			@Nullable RelationalPersistentEntity<?> entity, MapSqlParameterSource sqlParameterSource,
			AtomicInteger atomicInteger, Pair<Map<String, Table>, Map<String, Class<?>>> pair) {

		Assert.notNull(criteria, "CriteriaDefinition must not be null!");
		Assert.notNull(table, "Table must not be null!");

		if (criteria.isEmpty()) {
			throw new IllegalArgumentException("Cannot map empty Criteria");
		}

		Condition mapped = unroll(criteria, table, entity, sqlParameterSource, atomicInteger, pair);

		return new BoundCondition(sqlParameterSource, mapped);
	}

	public List<OrderByField> getMappedSort(SelectBuilder.SelectFromAndJoin selectBuilder, Table table, Sort sort,
			@Nullable RelationalPersistentEntity<?> entity, Pair<Map<String, Table>, Map<String, Class<?>>> pair) {

		List<OrderByField> mappedOrder = new ArrayList<>();

		for (Sort.Order order : sort) {
			String column = order.getProperty();

			if (column.indexOf(".") != -1) {
				int last = column.lastIndexOf(".");

				String tableName = column.substring(0, last);
				String fieldName = column.substring(last + 1);

				Map<String, Table> tableMap = pair.getFirst();

				if (!tableMap.containsKey(tableName)) {
					this.resolveColumn(selectBuilder, column, table, entity, pair);
				}

				Table rightTable = tableMap.get(tableName);
				Column c = ReferenceColumn.create(fieldName, rightTable);

				OrderByField orderBy = OrderByField.from(c).withNullHandling(order.getNullHandling());
				mappedOrder.add(order.isAscending() ? orderBy.asc() : orderBy.desc());

				continue;
			}

			Field field = createPropertyField(entity, SqlIdentifier.unquoted(column), this.mappingContext);
			OrderByField orderBy = OrderByField.from(table.column(field.getMappedColumnName()))
					.withNullHandling(order.getNullHandling());
			mappedOrder.add(order.isAscending() ? orderBy.asc() : orderBy.desc());
		}

		return mappedOrder;
	}

	public void resolveColumn(SelectBuilder.SelectFromAndJoin selectBuilder, String column, Table table,
			@Nullable RelationalPersistentEntity<?> entity, Pair<Map<String, Table>, Map<String, Class<?>>> pair) {
		Map<String, Table> tableMap = pair.getFirst();
		Map<String, Class<?>> clazzMap = pair.getSecond();
		resolveColumn(selectBuilder, column, table, entity, tableMap, clazzMap);
	}

	public void resolveColumn(SelectBuilder.SelectFromAndJoin selectBuilder, String column, Table table,
			@Nullable RelationalPersistentEntity<?> entity, Map<String, Table> tableMap,
			Map<String, Class<?>> clazzMap) {
		if (column.indexOf(".") != -1) {
			String[] names = column.split("\\.");
			NamingStrategy namingStrategy = mappingContext.getNamingStrategy();

			Table left = table;
			RelationalPersistentEntity<?> leftEntity = entity;

			for (int i = 0; i < names.length - 1; i++) {
				String name = names[i];
				java.lang.reflect.Field field = ReflectionUtils.findField(leftEntity.getType(), name);
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

						java.lang.reflect.Field rightField = ReflectionUtils.findField(clazz, mappedBy);
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
							java.lang.reflect.Field rightField = ReflectionUtils.findField(clazz, mappedBy);
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

	private Condition resolve(CriteriaDefinition criterion, Table table, @Nullable RelationalPersistentEntity<?> entity,
			MapSqlParameterSource sqlParameterSource, AtomicInteger atomicInteger,
			Pair<Map<String, Table>, Map<String, Class<?>>> pair) {
		Condition condition = null;
		SqlIdentifier columnIdentifier = criterion.getColumn();
		Map<String, Table> tableMap = pair.getFirst();
		Map<String, Class<?>> clazzMap = pair.getSecond();

		if (columnIdentifier != null) {
			String column = criterion.getColumn().toString();
			if (StringUtils.hasText(column) && column.indexOf(".") != -1) {
				int last = column.lastIndexOf(".");

				String tableName = column.substring(0, last);
				String field = column.substring(last + 1);

				Table right = tableMap.get(tableName);
				Class<?> rightClazz = clazzMap.get(tableName);
				RelationalPersistentEntity<?> rightEntity = getMappingContext().getRequiredPersistentEntity(rightClazz);

				Criteria c = null;
				Constructor<Criteria> constructor = null;

				try {
					constructor = Criteria.class.getDeclaredConstructor(Criteria.class, Combinator.class, List.class,
							SqlIdentifier.class, Comparator.class, Object.class, boolean.class);

					constructor.setAccessible(true);

					c = constructor.newInstance(criterion.getPrevious(), criterion.getCombinator(),
							criterion.getGroup(), SqlIdentifier.quoted(field), criterion.getComparator(),
							criterion.getValue(), criterion.isIgnoreCase());
				} catch (Exception | Error e) {
					throw new SelectBuildException("Use reflect to modify Criteria's Constructor failure");
				} finally {
					if (constructor != null) {
						constructor.setAccessible(false);
					}
				}

				condition = getCondition(c, right, rightEntity, sqlParameterSource, atomicInteger, pair);
			} else {
				condition = getCondition(criterion, table, entity, sqlParameterSource, atomicInteger, pair);
			}
		} else {
			condition = getCondition(criterion, table, entity, sqlParameterSource, atomicInteger, pair);
		}

		return condition;
	}

	private Condition unroll(CriteriaDefinition criteria, Table table, @Nullable RelationalPersistentEntity<?> entity,
			MapSqlParameterSource sqlParameterSource, AtomicInteger atomicInteger,
			Pair<Map<String, Table>, Map<String, Class<?>>> pair) {

		CriteriaDefinition current = criteria;

		// reverse unroll criteria chain
		Map<CriteriaDefinition, CriteriaDefinition> forwardChain = new HashMap<>();

		while (current.hasPrevious()) {
			forwardChain.put(current.getPrevious(), current);
			current = current.getPrevious();
		}

		// perform the actual mapping
		Condition mapped = resolve(current, table, entity, sqlParameterSource, atomicInteger, pair);

		while (forwardChain.containsKey(current)) {

			CriteriaDefinition criterion = forwardChain.get(current);

			Condition result = null;
			Condition condition = resolve(criterion, table, entity, sqlParameterSource, atomicInteger, pair);

			if (condition != null) {
				result = combine(criterion, mapped, criterion.getCombinator(), condition);
			}

			if (result != null) {
				mapped = result;
			}

			current = criterion;
		}

		if (mapped == null) {
			throw new IllegalStateException("Cannot map empty Criteria");
		}

		return mapped;
	}

	private Condition unroll(CriteriaDefinition criteria, Table table, @Nullable RelationalPersistentEntity<?> entity,
			MapSqlParameterSource sqlParameterSource, AtomicInteger atomicInteger) {

		CriteriaDefinition current = criteria;

		// reverse unroll criteria chain
		Map<CriteriaDefinition, CriteriaDefinition> forwardChain = new HashMap<>();

		while (current.hasPrevious()) {
			forwardChain.put(current.getPrevious(), current);
			current = current.getPrevious();
		}

		// perform the actual mapping
		Condition mapped = getCondition(current, table, entity, sqlParameterSource, atomicInteger);
		while (forwardChain.containsKey(current)) {

			CriteriaDefinition criterion = forwardChain.get(current);
			Condition result = null;

			Condition condition = getCondition(criterion, table, entity, sqlParameterSource, atomicInteger);
			if (condition != null) {
				result = combine(criterion, mapped, criterion.getCombinator(), condition);
			}

			if (result != null) {
				mapped = result;
			}
			current = criterion;
		}

		if (mapped == null) {
			throw new IllegalStateException("Cannot map empty Criteria");
		}

		return mapped;
	}

	@Nullable
	private Condition unrollGroup(List<? extends CriteriaDefinition> criteria, Table table,
			CriteriaDefinition.Combinator combinator, @Nullable RelationalPersistentEntity<?> entity,
			MapSqlParameterSource sqlParameterSource, AtomicInteger atomicInteger) {

		Condition mapped = null;
		for (CriteriaDefinition criterion : criteria) {

			if (criterion.isEmpty()) {
				continue;
			}

			Condition condition = unroll(criterion, table, entity, sqlParameterSource, atomicInteger);

			mapped = combine(criterion, mapped, combinator, condition);
		}

		return mapped;
	}

	@Nullable
	private Condition unrollGroup(List<? extends CriteriaDefinition> criteria, Table table,
			CriteriaDefinition.Combinator combinator, @Nullable RelationalPersistentEntity<?> entity,
			MapSqlParameterSource sqlParameterSource, AtomicInteger atomicInteger,
			Pair<Map<String, Table>, Map<String, Class<?>>> pair) {

		Condition mapped = null;
		for (CriteriaDefinition criterion : criteria) {

			if (criterion.isEmpty()) {
				continue;
			}

			Condition condition = unroll(criterion, table, entity, sqlParameterSource, atomicInteger, pair);

			mapped = combine(criterion, mapped, combinator, condition);
		}

		return mapped;
	}

	@Nullable
	private Condition getCondition(CriteriaDefinition criteria, Table table,
			@Nullable RelationalPersistentEntity<?> entity, MapSqlParameterSource sqlParameterSource,
			AtomicInteger atomicInteger) {

		if (criteria.isEmpty()) {
			return null;
		}

		if (criteria.isGroup()) {

			Condition condition = unrollGroup(criteria.getGroup(), table, criteria.getCombinator(), entity,
					sqlParameterSource, atomicInteger);

			return condition == null ? null : Conditions.nest(condition);
		}

		return mapCondition(criteria, table, entity, sqlParameterSource, atomicInteger);
	}

	@Nullable
	private Condition getCondition(CriteriaDefinition criteria, Table table,
			@Nullable RelationalPersistentEntity<?> entity, MapSqlParameterSource sqlParameterSource,
			AtomicInteger atomicInteger, Pair<Map<String, Table>, Map<String, Class<?>>> pair) {

		if (criteria.isEmpty()) {
			return null;
		}

		if (criteria.isGroup()) {

			Condition condition = unrollGroup(criteria.getGroup(), table, criteria.getCombinator(), entity,
					sqlParameterSource, atomicInteger, pair);

			return condition == null ? null : Conditions.nest(condition);
		}

		return mapCondition(criteria, table, entity, sqlParameterSource, atomicInteger);
	}

	private Condition combine(CriteriaDefinition criteria, @Nullable Condition currentCondition,
			CriteriaDefinition.Combinator combinator, Condition nextCondition) {

		if (currentCondition == null) {
			currentCondition = nextCondition;
		} else if (combinator == CriteriaDefinition.Combinator.AND) {
			currentCondition = currentCondition.and(nextCondition);
		} else if (combinator == CriteriaDefinition.Combinator.OR) {
			currentCondition = currentCondition.or(nextCondition);
		} else {
			throw new IllegalStateException("Combinator " + criteria.getCombinator() + " not supported");
		}

		return currentCondition;
	}

	@SuppressWarnings("unchecked")
	private Condition mapCondition(CriteriaDefinition criteria, Table table,
			@Nullable RelationalPersistentEntity<?> entity, MapSqlParameterSource sqlParameterSource,
			AtomicInteger atomicInteger) {

		Field propertyField = createPropertyFieldCustom(entity, criteria.getColumn(), getMappingContext());
		Column column = table.column(propertyField.getMappedColumnName());
		TypeInformation<?> actualType = propertyField.getTypeHint().getRequiredActualType();

		Object mappedValue;
		Class<?> typeHint;

		if (criteria.getValue() instanceof ValueFunction) {
			ValueFunction<Object> valueFunction = (ValueFunction<Object>) criteria.getValue();
			Object value = valueFunction.apply(getEscaper(criteria.getComparator()));

			mappedValue = convertValue(value, propertyField.getTypeHint());
			typeHint = actualType.getType();
		} else {

			mappedValue = convertValue(criteria.getValue(), propertyField.getTypeHint());
			typeHint = actualType.getType();
		}

		return createCondition(column, mappedValue, typeHint, criteria.getComparator(), criteria.isIgnoreCase(),
				sqlParameterSource, atomicInteger);
	}

	private Escaper getEscaper(Comparator comparator) {

		if (comparator == Comparator.LIKE || comparator == Comparator.NOT_LIKE) {
			return dialect.getLikeEscaper();
		}

		return Escaper.DEFAULT;
	}

	@SuppressWarnings("unchecked")
	private Condition createCondition(Column column, @Nullable Object mappedValue, Class<?> valueType,
			Comparator comparator, boolean ignoreCase, MapSqlParameterSource sqlParameterSource,
			AtomicInteger atomicInteger) {

		if (comparator.equals(Comparator.IS_NULL)) {
			return column.isNull();
		}

		if (comparator.equals(Comparator.IS_NOT_NULL)) {
			return column.isNotNull();
		}

		if (comparator == Comparator.IS_TRUE) {
			return column.isEqualTo(SQL.literalOf(true));
		}

		if (comparator == Comparator.IS_FALSE) {
			return column.isEqualTo(SQL.literalOf(false));
		}

		Expression columnExpression = column;
		if (ignoreCase && String.class == valueType) {
			columnExpression = Functions.upper(column);
		}

		if (comparator == Comparator.NOT_IN || comparator == Comparator.IN) {

			Condition condition;

			if (mappedValue instanceof Iterable) {

				List<Expression> expressions = new ArrayList<>(
						mappedValue instanceof Collection ? ((Collection<?>) mappedValue).size() : 10);

				for (Object o : (Iterable<?>) mappedValue) {
					expressions.add(bind(o, atomicInteger, sqlParameterSource, valueType));
				}

				condition = Conditions.in(columnExpression, expressions.toArray(new Expression[0]));

			} else {

				Expression expression = bind(mappedValue, atomicInteger, sqlParameterSource, valueType);

				condition = Conditions.in(columnExpression, expression);
			}

			if (comparator == Comparator.NOT_IN) {
				condition = condition.not();
			}

			return condition;
		}

		if (comparator == Comparator.BETWEEN || comparator == Comparator.NOT_BETWEEN) {

			Pair<Object, Object> pair = (Pair<Object, Object>) mappedValue;

			Expression begin = bind(pair.getFirst(), atomicInteger, sqlParameterSource, valueType, ignoreCase);
			Expression end = bind(pair.getSecond(), atomicInteger, sqlParameterSource, valueType, ignoreCase);

			return comparator == Comparator.BETWEEN ? Conditions.between(columnExpression, begin, end)
					: Conditions.notBetween(columnExpression, begin, end);
		}

		switch (comparator) {
		case EQ: {
			Expression expression = bind(mappedValue, atomicInteger, sqlParameterSource, valueType, ignoreCase);
			return Conditions.isEqual(columnExpression, expression);
		}
		case NEQ: {
			Expression expression = bind(mappedValue, atomicInteger, sqlParameterSource, valueType, ignoreCase);
			return Conditions.isEqual(columnExpression, expression).not();
		}
		case LT: {
			Expression expression = bind(mappedValue, atomicInteger, sqlParameterSource, valueType);
			return column.isLess(expression);
		}
		case LTE: {
			Expression expression = bind(mappedValue, atomicInteger, sqlParameterSource, valueType);
			return column.isLessOrEqualTo(expression);
		}
		case GT: {
			Expression expression = bind(mappedValue, atomicInteger, sqlParameterSource, valueType);
			return column.isGreater(expression);
		}
		case GTE: {
			Expression expression = bind(mappedValue, atomicInteger, sqlParameterSource, valueType);
			return column.isGreaterOrEqualTo(expression);
		}
		case LIKE: {
			Expression expression = bind(mappedValue, atomicInteger, sqlParameterSource, valueType, ignoreCase);
			return Conditions.like(columnExpression, expression);
		}
		case NOT_LIKE: {
			Expression expression = bind(mappedValue, atomicInteger, sqlParameterSource, valueType, ignoreCase);
			return Conditions.notLike(columnExpression, expression);
		}
		default:
			throw new UnsupportedOperationException("Comparator " + comparator + " not supported");
		}
	}

	Field createPropertyFieldCustom(@Nullable RelationalPersistentEntity<?> entity, SqlIdentifier key,
			MappingContext<? extends RelationalPersistentEntity<?>, RelationalPersistentProperty> mappingContext) {
		return entity == null ? new Field(key) : new MetadataBackedField(key, entity, mappingContext, converter);
	}

	Class<?> getTypeHintCustom(@Nullable Object mappedValue, Class<?> propertyType) {
		return propertyType;
	}

	private Expression bind(@Nullable Object mappedValue, AtomicInteger atomicInteger,
			MapSqlParameterSource sqlParameterSource, Class<?> valueType) {
		return bind(mappedValue, atomicInteger, sqlParameterSource, valueType, false);
	}

	private Expression bind(@Nullable Object mappedValue, AtomicInteger atomicInteger,
			MapSqlParameterSource sqlParameterSource, Class<?> valueType, boolean ignoreCase) {
		String n = "p" + atomicInteger.getAndIncrement();
		sqlParameterSource.addValue(n, mappedValue);

		return ignoreCase ? Functions.upper(SQL.bindMarker(":" + n)) : SQL.bindMarker(":" + n);
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
		if (clazz.isEnum()) {
			return true;
		}

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

	@Nullable
	@SuppressWarnings("unchecked")
	protected Object convertValue(@Nullable Object value, TypeInformation<?> typeInformation) {

		if (value == null) {
			return null;
		}

		if (value instanceof Pair) {
			Pair<Object, Object> pair = (Pair<Object, Object>) value;

			Object first = convertValue(pair.getFirst(),
					typeInformation.getActualType() != null ? typeInformation.getRequiredActualType()
							: ClassTypeInformation.OBJECT);

			Object second = convertValue(pair.getSecond(),
					typeInformation.getActualType() != null ? typeInformation.getRequiredActualType()
							: ClassTypeInformation.OBJECT);

			return Pair.of(first, second);
		}

		if (value instanceof Iterable) {

			List<Object> mapped = new ArrayList<>();

			for (Object o : (Iterable<?>) value) {
				mapped.add(convertValue(o,
						typeInformation.getActualType() != null ? typeInformation.getRequiredActualType()
								: ClassTypeInformation.OBJECT));
			}

			return mapped;
		}

		if (value.getClass().isArray()
				&& (ClassTypeInformation.OBJECT.equals(typeInformation) || typeInformation.isCollectionLike())) {
			return value;
		}

		return this.converter.writeValue(value, typeInformation);
	}

	protected RelationalMappingContext getMappingContext() {
		return this.mappingContext;
	}

}
