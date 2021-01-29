package cn.com.pan.jdbc.query;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.dialect.Escaper;
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
import org.springframework.data.relational.core.sql.SQL;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.Pair;
import org.springframework.data.util.TypeInformation;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import cn.com.pan.jdbc.exception.SelectBuildException;

public class UpdateMapper extends QueryMapper {

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
			@Nullable RelationalPersistentEntity<?> entity, MapSqlParameterSource sqlParameterSource) {

		Assert.notNull(criteria, "CriteriaDefinition must not be null!");
		Assert.notNull(table, "Table must not be null!");

		if (criteria.isEmpty()) {
			throw new IllegalArgumentException("Cannot map empty Criteria");
		}

		Condition mapped = unroll(criteria, table, entity, sqlParameterSource);

		return new BoundCondition(sqlParameterSource, mapped);
	}

	public BoundCondition getMappedObject(CriteriaDefinition criteria, Table table,
			@Nullable RelationalPersistentEntity<?> entity, MapSqlParameterSource sqlParameterSource,
			Pair<Map<String, Table>, Map<String, Class<?>>> pair) {

		Assert.notNull(criteria, "CriteriaDefinition must not be null!");
		Assert.notNull(table, "Table must not be null!");

		if (criteria.isEmpty()) {
			throw new IllegalArgumentException("Cannot map empty Criteria");
		}

		Condition mapped = unroll(criteria, table, entity, sqlParameterSource, pair);

		return new BoundCondition(sqlParameterSource, mapped);
	}

	private Condition resolve(CriteriaDefinition criterion, Table table, @Nullable RelationalPersistentEntity<?> entity,
			MapSqlParameterSource sqlParameterSource, Pair<Map<String, Table>, Map<String, Class<?>>> pair) {
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

				condition = getCondition(c, right, rightEntity, sqlParameterSource, pair);
			} else {
				condition = getCondition(criterion, table, entity, sqlParameterSource, pair);
			}
		} else {
			condition = getCondition(criterion, table, entity, sqlParameterSource, pair);
		}

		return condition;
	}

	private Condition unroll(CriteriaDefinition criteria, Table table, @Nullable RelationalPersistentEntity<?> entity,
			MapSqlParameterSource sqlParameterSource, Pair<Map<String, Table>, Map<String, Class<?>>> pair) {

		CriteriaDefinition current = criteria;

		// reverse unroll criteria chain
		Map<CriteriaDefinition, CriteriaDefinition> forwardChain = new HashMap<>();

		while (current.hasPrevious()) {
			forwardChain.put(current.getPrevious(), current);
			current = current.getPrevious();
		}

		// perform the actual mapping
		Condition mapped = resolve(current, table, entity, sqlParameterSource, pair);

		while (forwardChain.containsKey(current)) {

			CriteriaDefinition criterion = forwardChain.get(current);

			Condition result = null;
			Condition condition = resolve(criterion, table, entity, sqlParameterSource, pair);

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
			MapSqlParameterSource sqlParameterSource) {

		CriteriaDefinition current = criteria;

		// reverse unroll criteria chain
		Map<CriteriaDefinition, CriteriaDefinition> forwardChain = new HashMap<>();

		while (current.hasPrevious()) {
			forwardChain.put(current.getPrevious(), current);
			current = current.getPrevious();
		}

		// perform the actual mapping
		Condition mapped = getCondition(current, table, entity, sqlParameterSource);
		while (forwardChain.containsKey(current)) {

			CriteriaDefinition criterion = forwardChain.get(current);
			Condition result = null;

			Condition condition = getCondition(criterion, table, entity, sqlParameterSource);
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
			MapSqlParameterSource sqlParameterSource) {

		Condition mapped = null;
		for (CriteriaDefinition criterion : criteria) {

			if (criterion.isEmpty()) {
				continue;
			}

			Condition condition = unroll(criterion, table, entity, sqlParameterSource);

			mapped = combine(criterion, mapped, combinator, condition);
		}

		return mapped;
	}

	@Nullable
	private Condition unrollGroup(List<? extends CriteriaDefinition> criteria, Table table,
			CriteriaDefinition.Combinator combinator, @Nullable RelationalPersistentEntity<?> entity,
			MapSqlParameterSource sqlParameterSource, Pair<Map<String, Table>, Map<String, Class<?>>> pair) {

		Condition mapped = null;
		for (CriteriaDefinition criterion : criteria) {

			if (criterion.isEmpty()) {
				continue;
			}

			Condition condition = unroll(criterion, table, entity, sqlParameterSource, pair);

			mapped = combine(criterion, mapped, combinator, condition);
		}

		return mapped;
	}

	@Nullable
	private Condition getCondition(CriteriaDefinition criteria, Table table,
			@Nullable RelationalPersistentEntity<?> entity, MapSqlParameterSource sqlParameterSource) {

		if (criteria.isEmpty()) {
			return null;
		}

		if (criteria.isGroup()) {

			Condition condition = unrollGroup(criteria.getGroup(), table, criteria.getCombinator(), entity,
					sqlParameterSource);

			return condition == null ? null : Conditions.nest(condition);
		}

		return mapCondition(criteria, table, entity, sqlParameterSource);
	}

	@Nullable
	private Condition getCondition(CriteriaDefinition criteria, Table table,
			@Nullable RelationalPersistentEntity<?> entity, MapSqlParameterSource sqlParameterSource,
			Pair<Map<String, Table>, Map<String, Class<?>>> pair) {

		if (criteria.isEmpty()) {
			return null;
		}

		if (criteria.isGroup()) {

			Condition condition = unrollGroup(criteria.getGroup(), table, criteria.getCombinator(), entity,
					sqlParameterSource, pair);

			return condition == null ? null : Conditions.nest(condition);
		}

		return mapCondition(criteria, table, entity, sqlParameterSource);
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
			@Nullable RelationalPersistentEntity<?> entity, MapSqlParameterSource sqlParameterSource) {

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
				sqlParameterSource);
	}

	private Escaper getEscaper(Comparator comparator) {

		if (comparator == Comparator.LIKE || comparator == Comparator.NOT_LIKE) {
			return dialect.getLikeEscaper();
		}

		return Escaper.DEFAULT;
	}

	@SuppressWarnings("unchecked")
	private Condition createCondition(Column column, @Nullable Object mappedValue, Class<?> valueType,
			Comparator comparator, boolean ignoreCase, MapSqlParameterSource sqlParameterSource) {

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
					expressions.add(bind(o, column, sqlParameterSource, valueType));
				}

				condition = Conditions.in(columnExpression, expressions.toArray(new Expression[0]));

			} else {

				Expression expression = bind(mappedValue, column, sqlParameterSource, valueType);

				condition = Conditions.in(columnExpression, expression);
			}

			if (comparator == Comparator.NOT_IN) {
				condition = condition.not();
			}

			return condition;
		}

		if (comparator == Comparator.BETWEEN || comparator == Comparator.NOT_BETWEEN) {

			Pair<Object, Object> pair = (Pair<Object, Object>) mappedValue;

			Expression begin = bind(pair.getFirst(), column, sqlParameterSource, valueType, ignoreCase);
			Expression end = bind(pair.getSecond(), column, sqlParameterSource, valueType, ignoreCase);

			return comparator == Comparator.BETWEEN ? Conditions.between(columnExpression, begin, end)
					: Conditions.notBetween(columnExpression, begin, end);
		}

		switch (comparator) {
		case EQ: {
			Expression expression = bind(mappedValue, column, sqlParameterSource, valueType, ignoreCase);
			return Conditions.isEqual(columnExpression, expression);
		}
		case NEQ: {
			Expression expression = bind(mappedValue, column, sqlParameterSource, valueType, ignoreCase);
			return Conditions.isEqual(columnExpression, expression).not();
		}
		case LT: {
			Expression expression = bind(mappedValue, column, sqlParameterSource, valueType);
			return column.isLess(expression);
		}
		case LTE: {
			Expression expression = bind(mappedValue, column, sqlParameterSource, valueType);
			return column.isLessOrEqualTo(expression);
		}
		case GT: {
			Expression expression = bind(mappedValue, column, sqlParameterSource, valueType);
			return column.isGreater(expression);
		}
		case GTE: {
			Expression expression = bind(mappedValue, column, sqlParameterSource, valueType);
			return column.isGreaterOrEqualTo(expression);
		}
		case LIKE: {
			Expression expression = bind(mappedValue, column, sqlParameterSource, valueType, ignoreCase);
			return Conditions.like(columnExpression, expression);
		}
		case NOT_LIKE: {
			Expression expression = bind(mappedValue, column, sqlParameterSource, valueType, ignoreCase);
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

	private Expression bind(@Nullable Object mappedValue, Column column, MapSqlParameterSource sqlParameterSource,
			Class<?> valueType) {
		return bind(mappedValue, column, sqlParameterSource, valueType, false);
	}

	private Expression bind(@Nullable Object mappedValue, Column column, MapSqlParameterSource sqlParameterSource,
			Class<?> valueType, boolean ignoreCase) {
		String n = column.getName().getReference();
		sqlParameterSource.addValue(n, mappedValue);

		return ignoreCase ? Functions.upper(SQL.bindMarker(":" + n)) : SQL.bindMarker(":" + n);
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
