package org.springframework.data.jdbc.repository.query;

import static java.util.regex.Pattern.CASE_INSENSITIVE;
import static org.springframework.util.ObjectUtils.nullSafeEquals;
import static org.springframework.util.ObjectUtils.nullSafeHashCode;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.beans.BeanUtils;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.BeanWrapperImpl;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.data.jdbc.core.convert.JdbcColumnTypes;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.convert.JdbcValue;
import org.springframework.data.jdbc.support.JdbcUtil;
import org.springframework.data.relational.core.mapping.ManyToMany;
import org.springframework.data.relational.core.mapping.ManyToOne;
import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.data.relational.core.mapping.OneToMany;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.repository.query.RelationalEntityMetadata;
import org.springframework.data.relational.repository.query.RelationalParameters;
import org.springframework.data.relational.repository.query.RelationalParameters.RelationalParameter;
import org.springframework.data.repository.query.Parameter;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.SpelQueryContext;
import org.springframework.data.repository.query.SpelQueryContext.SpelExtractor;
import org.springframework.data.repository.query.parser.Part.Type;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

public class StringBasedJdbcQuery extends AbstractJdbcQuery {

	private static final String ENTITY_NAME = "entityName";
	private static final String ENTITY_NAME_VARIABLE = "#" + ENTITY_NAME;

	private static final String _ENTITY = "_entity";
	private static final String _ENTITY_VARIABLE = "#" + _ENTITY;

	private static final String SYNTHETIC_PARAMETER_TEMPLATE = "__$synthetic$__%d";

	private static final Pattern POSITION_BINDING_PATTERN = Pattern.compile("[?](\\d+)");

	private static final Pattern EXPRESSION_BINDING_PATTERN = Pattern.compile("[:]#\\{(.*?)}");

	private static final String PARAMETER_NEEDS_TO_BE_NAMED = "For queries with named parameters you need to provide names for method parameters. Use @Param for query method parameters, or when on Java 8+ use the javac flag -parameters.";

	private final JdbcQueryMethod queryMethod;
	private final JdbcQueryExecution<?> executor;
	private final JdbcConverter converter;
	private BeanFactory beanFactory;

	private final NamingStrategy namingStrategy;

	private SpelExpressionParser parser = new SpelExpressionParser();

	private QueryMethodEvaluationContextProvider evaluationContextProvider;

	ParserContext TEMPLATE_EXPRESSION = new ParserContext() {

		@Override
		public boolean isTemplate() {
			return true;
		}

		@Override
		public String getExpressionPrefix() {
			return "${";
		}

		@Override
		public String getExpressionSuffix() {
			return "}";
		}
	};

	/**
	 * Creates a new {@link StringBasedJdbcQuery} for the given
	 * {@link JdbcQueryMethod}, {@link RelationalMappingContext} and
	 * {@link RowMapper}.
	 *
	 * @param queryMethod      must not be {@literal null}.
	 * @param operations       must not be {@literal null}.
	 * @param defaultRowMapper can be {@literal null} (only in case of a modifying
	 *                         query).
	 */
	public StringBasedJdbcQuery(JdbcQueryMethod queryMethod, NamedParameterJdbcOperations operations,
			@Nullable RowMapper<?> defaultRowMapper, JdbcConverter converter) {

		super(queryMethod, operations, defaultRowMapper);

		this.queryMethod = queryMethod;
		this.converter = converter;
		this.evaluationContextProvider = QueryMethodEvaluationContextProvider.DEFAULT;
		this.namingStrategy = ((RelationalMappingContext) this.converter.getMappingContext()).getNamingStrategy();

		RowMapper<Object> rowMapper = determineRowMapper(defaultRowMapper);
		executor = getQueryExecution( //
				queryMethod, //
				determineResultSetExtractor(rowMapper != defaultRowMapper ? rowMapper : null), //
				rowMapper //
		);
	}

	public StringBasedJdbcQuery(JdbcQueryMethod queryMethod, NamedParameterJdbcOperations operations,
			@Nullable RowMapper<?> defaultRowMapper, JdbcConverter converter,
			QueryMethodEvaluationContextProvider evaluationContextProvider) {

		super(queryMethod, operations, defaultRowMapper);

		this.queryMethod = queryMethod;
		this.converter = converter;
		this.evaluationContextProvider = evaluationContextProvider;
		this.namingStrategy = ((RelationalMappingContext) this.converter.getMappingContext()).getNamingStrategy();

		RowMapper<Object> rowMapper = determineRowMapper(defaultRowMapper);
		executor = getQueryExecution( //
				queryMethod, //
				determineResultSetExtractor(rowMapper != defaultRowMapper ? rowMapper : null), //
				rowMapper //
		);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.springframework.data.repository.query.RepositoryQuery#execute(java.lang.
	 * Object[])
	 */
	@Override
	public Object execute(Object[] objects) {
		List<ParameterBinding> bindings = new ArrayList<>();

		String query = determineQuery();

		RelationalParameters parameters = queryMethod.getParameters();
		MapSqlParameterSource sqlParameterSource = this.bindParameters(objects);

		EvaluationContext evaluationContext = evaluationContextProvider.getEvaluationContext(parameters, objects);

		if (query.contains(_ENTITY_VARIABLE)) {
			RelationalEntityMetadata<?> metadata = queryMethod.getEntityInformation();

			RelationalPersistentEntity<?> entity = metadata.getTableEntity();

			StringBuilder sb = new StringBuilder();
			Iterator<RelationalPersistentProperty> iterator = entity.iterator();

			while (iterator.hasNext()) {
				RelationalPersistentProperty persistentProperty = iterator.next();

				if (persistentProperty.isEntity() || persistentProperty.isAnnotationPresent(ManyToOne.class)
						|| persistentProperty.isAnnotationPresent(OneToMany.class)
						|| persistentProperty.isAnnotationPresent(ManyToMany.class)) {
					continue;
				}

				String property = persistentProperty.getName();

				sb.append(" ");
				sb.append(namingStrategy.getColumnName(property));
				sb.append(",");
			}

			String c = sb.toString();

			if (c.endsWith(",")) {
				c = c.substring(0, c.length() - 1);
			}

			evaluationContext.setVariable(_ENTITY, c.trim());
		}

		if (query.contains(ENTITY_NAME_VARIABLE)) {
			RelationalEntityMetadata<?> metadata = queryMethod.getEntityInformation();

			evaluationContext.setVariable(ENTITY_NAME, metadata.getTableName().toString());
		}

		query = parseExpressionIntoBindings(query, bindings);
		query = parsePositionIntoBindings(query, bindings);

		for (ParameterBinding binding : bindings) {
			String name = binding.getName();
			Integer position = binding.getPosition();

			if (StringUtils.hasText(name) && !sqlParameterSource.hasValue(name)) {
				Expression expression = parser.parseExpression(binding.getExpression(),
						ParserContext.TEMPLATE_EXPRESSION);

				Object value = expression.getValue(evaluationContext);

				sqlParameterSource.addValue(name, value);
			}

			if (position != null) {
				RelationalParameter p = parameters.getBindableParameter(position);
				String parameterName = p.getName()
						.orElseThrow(() -> new IllegalStateException(PARAMETER_NEEDS_TO_BE_NAMED));

				query = query.replace("?" + position.toString(), ":" + parameterName);

				Object value = evaluationContext.lookupVariable(parameterName);

				if (value == null) {
					value = objects[position];
				}

				sqlParameterSource.addValue(parameterName, value);
			}
		}

		Expression e = parser.parseExpression(query, ParserContext.TEMPLATE_EXPRESSION);
		query = e.getValue(evaluationContext, String.class);

		return executor.execute(query, sqlParameterSource);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.springframework.data.repository.query.RepositoryQuery#getQueryMethod()
	 */
	@Override
	public JdbcQueryMethod getQueryMethod() {
		return queryMethod;
	}

	MapSqlParameterSource bindParameters(Object[] objects) {

		MapSqlParameterSource parameters = new MapSqlParameterSource();

		queryMethod.getParameters().getBindableParameters()
				.forEach(p -> convertAndAddParameter(parameters, p, objects[p.getIndex()]));

		return parameters;
	}

	@SuppressWarnings("unchecked")
	private void convertAndAddParameter(MapSqlParameterSource parameters, Parameter p, Object value) {

		String parameterName = p.getName().orElseThrow(() -> new IllegalStateException(PARAMETER_NEEDS_TO_BE_NAMED));

		Class<?> parameterType = queryMethod.getParameters().getParameter(p.getIndex()).getType();
		Class<?> conversionTargetType = JdbcColumnTypes.INSTANCE.resolvePrimitiveType(parameterType);

		if (value instanceof Map) {
			parameters.addValues((Map<String, ?>) value);
			return;
		}

		JdbcValue jdbcValue = converter.writeJdbcValue(value, conversionTargetType,
				JdbcUtil.sqlTypeFor(conversionTargetType));

		JDBCType jdbcType = jdbcValue.getJdbcType();
		if (jdbcType == null) {
			if (value != null) {
				BeanWrapper bw = new BeanWrapperImpl(value);
				PropertyDescriptor[] propertyDescriptors = bw.getPropertyDescriptors();

				for (PropertyDescriptor propertyDescriptor : propertyDescriptors) {
					String name = propertyDescriptor.getName();
					parameters.addValue(name, bw.getPropertyValue(name));
				}
			}

			parameters.addValue(parameterName, jdbcValue.getValue());
		} else {
			parameters.addValue(parameterName, jdbcValue.getValue(), jdbcType.getVendorTypeNumber());
		}
	}

	private String determineQuery() {

		String query = queryMethod.getDeclaredQuery();

		if (!StringUtils.hasText(query)) {
			throw new IllegalStateException(String.format("No query specified on %s", queryMethod.getName()));
		}

		return query;
	}

	@Nullable
	@SuppressWarnings({ "rawtypes", "unchecked" })
	ResultSetExtractor<Object> determineResultSetExtractor(@Nullable RowMapper<Object> rowMapper) {

		String resultSetExtractorRef = queryMethod.getResultSetExtractorRef();

		if (StringUtils.hasText(resultSetExtractorRef)) {

			Assert.notNull(beanFactory, "When a ResultSetExtractorRef is specified the BeanFactory must not be null");

			return (ResultSetExtractor<Object>) beanFactory.getBean(resultSetExtractorRef);
		}

		Class<? extends ResultSetExtractor> resultSetExtractorClass = queryMethod.getResultSetExtractorClass();

		if (isUnconfigured(resultSetExtractorClass, ResultSetExtractor.class)) {
			return null;
		}

		Constructor<? extends ResultSetExtractor> constructor = ClassUtils
				.getConstructorIfAvailable(resultSetExtractorClass, RowMapper.class);

		if (constructor != null) {
			return BeanUtils.instantiateClass(constructor, rowMapper);
		}

		return BeanUtils.instantiateClass(resultSetExtractorClass);
	}

	@SuppressWarnings("unchecked")
	RowMapper<Object> determineRowMapper(@Nullable RowMapper<?> defaultMapper) {

		String rowMapperRef = queryMethod.getRowMapperRef();

		if (StringUtils.hasText(rowMapperRef)) {

			Assert.notNull(beanFactory, "When a RowMapperRef is specified the BeanFactory must not be null");

			return (RowMapper<Object>) beanFactory.getBean(rowMapperRef);
		}

		Class<?> rowMapperClass = queryMethod.getRowMapperClass();

		if (isUnconfigured(rowMapperClass, RowMapper.class)) {
			return (RowMapper<Object>) defaultMapper;
		}

		return (RowMapper<Object>) BeanUtils.instantiateClass(rowMapperClass);
	}

	private static boolean isUnconfigured(@Nullable Class<?> configuredClass, Class<?> defaultClass) {
		return configuredClass == null || configuredClass == defaultClass;
	}

	private String parseExpressionIntoBindings(String query, List<ParameterBinding> bindings) {
		Matcher matcher = EXPRESSION_BINDING_PATTERN.matcher(query);

		int i = 0;
		while (matcher.find()) {
			String s = matcher.group(0).replace(":", "");
			String n = String.format(SYNTHETIC_PARAMETER_TEMPLATE, i);

			query = query.replace(s, n);

			bindings.add(new ParameterBinding(n, null, s));

			i++;
		}

		return query;
	}

	private String parsePositionIntoBindings(String query, List<ParameterBinding> bindings) {
		Matcher matcher = POSITION_BINDING_PATTERN.matcher(query);

		while (matcher.find()) {
			String s = matcher.group(1);

			bindings.add(new ParameterBinding(null, Integer.parseInt(s), null));
		}

		return query;
	}

	public void setBeanFactory(BeanFactory beanFactory) {
		this.beanFactory = beanFactory;
	}

	enum ParameterBindingParser {

		INSTANCE;

		private static final String EXPRESSION_PARAMETER_PREFIX = "__$synthetic$__";
		public static final String POSITIONAL_OR_INDEXED_PARAMETER = "\\?(\\d*+(?![#\\w]))";
		// .....................................................................^ not
		// followed by a hash or a letter.
		// .................................................................^ zero or
		// more digits.
		// .............................................................^ start with a
		// question mark.
		private static final Pattern PARAMETER_BINDING_BY_INDEX = Pattern.compile(POSITIONAL_OR_INDEXED_PARAMETER);
		private static final Pattern PARAMETER_BINDING_PATTERN;
		private static final String MESSAGE = "Already found parameter binding with same index / parameter name but differing binding type! "
				+ "Already have: %s, found %s! If you bind a parameter multiple times make sure they use the same binding.";
		private static final int INDEXED_PARAMETER_GROUP = 4;
		private static final int NAMED_PARAMETER_GROUP = 6;
		private static final int COMPARISION_TYPE_GROUP = 1;

		public static final String IDENTIFIER = "[._$[\\P{Z}&&\\P{Cc}&&\\P{Cf}&&\\P{Punct}]]+";
		public static final String COLON_NO_DOUBLE_COLON = "(?<![:\\\\]):";
		public static final String IDENTIFIER_GROUP = String.format("(%s)", IDENTIFIER);

		static {

			List<String> keywords = new ArrayList<>();

			for (ParameterBindingType type : ParameterBindingType.values()) {
				if (type.getKeyword() != null) {
					keywords.add(type.getKeyword());
				}
			}

			StringBuilder builder = new StringBuilder();
			builder.append("(");
			builder.append(StringUtils.collectionToDelimitedString(keywords, "|")); // keywords
			builder.append(")?");
			builder.append("(?: )?"); // some whitespace
			builder.append("\\(?"); // optional braces around parameters
			builder.append("(");
			builder.append("%?(" + POSITIONAL_OR_INDEXED_PARAMETER + ")%?"); // position parameter and parameter index
			builder.append("|"); // or

			// named parameter and the parameter name
			builder.append("%?(" + COLON_NO_DOUBLE_COLON + IDENTIFIER_GROUP + ")%?");

			builder.append(")");
			builder.append("\\)?"); // optional braces around parameters

			PARAMETER_BINDING_PATTERN = Pattern.compile(builder.toString(), CASE_INSENSITIVE);
		}

		/**
		 * Parses {@link ParameterBinding} instances from the given query and adds them
		 * to the registered bindings. Returns the cleaned up query.
		 */
		String parseParameterBindingsOfQueryIntoBindingsAndReturnCleanedQuery(String query,
				List<ParameterBinding> bindings, Metadata queryMeta) {

			int greatestParameterIndex = tryFindGreatestParameterIndexIn(query);
			boolean parametersShouldBeAccessedByIndex = greatestParameterIndex != -1;

			/*
			 * Prefer indexed access over named parameters if only SpEL Expression
			 * parameters are present.
			 */
			if (!parametersShouldBeAccessedByIndex && query.contains("?#{")) {
				parametersShouldBeAccessedByIndex = true;
				greatestParameterIndex = 0;
			}

			SpelExtractor spelExtractor = createSpelExtractor(query, parametersShouldBeAccessedByIndex,
					greatestParameterIndex);

			String resultingQuery = spelExtractor.getQueryString();
			Matcher matcher = PARAMETER_BINDING_PATTERN.matcher(resultingQuery);

			int expressionParameterIndex = parametersShouldBeAccessedByIndex ? greatestParameterIndex : 0;

			boolean usesJdbcStyleParameters = false;
			while (matcher.find()) {

				if (spelExtractor.isQuoted(matcher.start())) {
					continue;
				}

				String parameterIndexString = matcher.group(INDEXED_PARAMETER_GROUP);
				String parameterName = parameterIndexString != null ? null : matcher.group(NAMED_PARAMETER_GROUP);
				Integer parameterIndex = getParameterIndex(parameterIndexString);

				String typeSource = matcher.group(COMPARISION_TYPE_GROUP);
				String expression = spelExtractor
						.getParameter(parameterName == null ? parameterIndexString : parameterName);
				String replacement = null;

				Assert.isTrue(parameterIndexString != null || parameterName != null,
						() -> String.format("We need either a name or an index! Offending query string: %s", query));

				expressionParameterIndex++;
				if ("".equals(parameterIndexString)) {

					queryMeta.usesJdbcStyleParameters = true;
					parameterIndex = expressionParameterIndex;
				} else {
					usesJdbcStyleParameters = true;
				}

				if (usesJdbcStyleParameters && queryMeta.usesJdbcStyleParameters) {
					throw new IllegalArgumentException(
							"Mixing of ? parameters and other forms like ?1 is not supported!");
				}

				switch (ParameterBindingType.of(typeSource)) {

				case LIKE:

					Type likeType = LikeParameterBinding.getLikeTypeFrom(matcher.group(2));
					replacement = matcher.group(3);

					if (parameterIndex != null) {
						checkAndRegister(new LikeParameterBinding(parameterIndex, likeType, expression), bindings);
					} else {
						checkAndRegister(new LikeParameterBinding(parameterName, likeType, expression), bindings);

						replacement = expression != null ? ":" + parameterName : matcher.group(5);
					}

					break;

				case IN:

					if (parameterIndex != null) {
						checkAndRegister(new InParameterBinding(parameterIndex, expression), bindings);
					} else {
						checkAndRegister(new InParameterBinding(parameterName, expression), bindings);
					}

					break;

				case AS_IS: // fall-through we don't need a special parameter binding for the given
							// parameter.
				default:

					bindings.add(parameterIndex != null ? new ParameterBinding(null, parameterIndex, expression)
							: new ParameterBinding(parameterName, null, expression));
				}

				if (replacement != null) {
					resultingQuery = replaceFirst(resultingQuery, matcher.group(2), replacement);
				}

			}

			return resultingQuery;
		}

		private static SpelExtractor createSpelExtractor(String queryWithSpel,
				boolean parametersShouldBeAccessedByIndex, int greatestParameterIndex) {

			/*
			 * If parameters need to be bound by index, we bind the synthetic expression
			 * parameters starting from position of the greatest discovered index parameter
			 * in order to not mix-up with the actual parameter indices.
			 */
			int expressionParameterIndex = parametersShouldBeAccessedByIndex ? greatestParameterIndex : 0;

			BiFunction<Integer, String, String> indexToParameterName = parametersShouldBeAccessedByIndex
					? (index, expression) -> String.valueOf(index + expressionParameterIndex + 1)
					: (index, expression) -> EXPRESSION_PARAMETER_PREFIX + (index + 1);

			String fixedPrefix = parametersShouldBeAccessedByIndex ? "?" : ":";

			BiFunction<String, String, String> parameterNameToReplacement = (prefix, name) -> fixedPrefix + name;

			return SpelQueryContext.of(indexToParameterName, parameterNameToReplacement).parse(queryWithSpel);
		}

		private static String replaceFirst(String text, String substring, String replacement) {

			int index = text.indexOf(substring);
			if (index < 0) {
				return text;
			}

			return text.substring(0, index) + replacement + text.substring(index + substring.length());
		}

		@Nullable
		private static Integer getParameterIndex(@Nullable String parameterIndexString) {

			if (parameterIndexString == null || parameterIndexString.isEmpty()) {
				return null;
			}
			return Integer.valueOf(parameterIndexString);
		}

		private static int tryFindGreatestParameterIndexIn(String query) {

			Matcher parameterIndexMatcher = PARAMETER_BINDING_BY_INDEX.matcher(query);

			int greatestParameterIndex = -1;
			while (parameterIndexMatcher.find()) {

				String parameterIndexString = parameterIndexMatcher.group(1);
				Integer parameterIndex = getParameterIndex(parameterIndexString);
				if (parameterIndex != null) {
					greatestParameterIndex = Math.max(greatestParameterIndex, parameterIndex);
				}
			}

			return greatestParameterIndex;
		}

		private static void checkAndRegister(ParameterBinding binding, List<ParameterBinding> bindings) {

			bindings.stream() //
					.filter(it -> it.hasName(binding.getName()) || it.hasPosition(binding.getPosition())) //
					.forEach(it -> Assert.isTrue(it.equals(binding), String.format(MESSAGE, it, binding)));

			if (!bindings.contains(binding)) {
				bindings.add(binding);
			}
		}

		/**
		 * An enum for the different types of bindings.
		 *
		 * @author Thomas Darimont
		 * @author Oliver Gierke
		 */
		private enum ParameterBindingType {

			// Trailing whitespace is intentional to reflect that the keywords must be used
			// with at least one whitespace
			// character, while = does not.
			LIKE("like "), IN("in "), AS_IS(null);

			private final @Nullable String keyword;

			ParameterBindingType(@Nullable String keyword) {
				this.keyword = keyword;
			}

			/**
			 * Returns the keyword that will trigger the binding type or {@literal null} if
			 * the type is not triggered by a keyword.
			 *
			 * @return the keyword
			 */
			@Nullable
			public String getKeyword() {
				return keyword;
			}

			/**
			 * Return the appropriate {@link ParameterBindingType} for the given
			 * {@link String}. Returns {@literal #AS_IS} in case no other
			 * {@link ParameterBindingType} could be found.
			 */
			static ParameterBindingType of(String typeSource) {

				if (!StringUtils.hasText(typeSource)) {
					return AS_IS;
				}

				for (ParameterBindingType type : values()) {
					if (type.name().equalsIgnoreCase(typeSource.trim())) {
						return type;
					}
				}

				throw new IllegalArgumentException(String.format("Unsupported parameter binding type %s!", typeSource));
			}
		}
	}

	/**
	 * A generic parameter binding with name or position information.
	 *
	 * @author Thomas Darimont
	 */
	static class ParameterBinding {

		private final @Nullable String name;
		private final @Nullable String expression;
		private final @Nullable Integer position;

		/**
		 * Creates a new {@link ParameterBinding} for the parameter with the given
		 * position.
		 *
		 * @param position must not be {@literal null}.
		 */
		ParameterBinding(Integer position) {
			this(null, position, null);
		}

		/**
		 * Creates a new {@link ParameterBinding} for the parameter with the given name,
		 * position and expression information. Either {@literal name} or
		 * {@literal position} must be not {@literal null}.
		 *
		 * @param name       of the parameter may be {@literal null}.
		 * @param position   of the parameter may be {@literal null}.
		 * @param expression the expression to apply to any value for this parameter.
		 */
		ParameterBinding(@Nullable String name, @Nullable Integer position, @Nullable String expression) {

			if (name == null) {
				Assert.notNull(position, "Position must not be null!");
			}

			if (position == null) {
				Assert.notNull(name, "Name must not be null!");
			}

			this.name = name;
			this.position = position;
			this.expression = expression;
		}

		/**
		 * Returns whether the binding has the given name. Will always be
		 * {@literal false} in case the {@link ParameterBinding} has been set up from a
		 * position.
		 */
		boolean hasName(@Nullable String name) {
			return this.position == null && this.name != null && this.name.equals(name);
		}

		/**
		 * Returns whether the binding has the given position. Will always be
		 * {@literal false} in case the {@link ParameterBinding} has been set up from a
		 * name.
		 */
		boolean hasPosition(@Nullable Integer position) {
			return position != null && this.name == null && position.equals(this.position);
		}

		/**
		 * @return the name
		 */
		@Nullable
		public String getName() {
			return name;
		}

		/**
		 * @return the name
		 * @throws IllegalStateException if the name is not available.
		 * @since 2.0
		 */
		String getRequiredName() throws IllegalStateException {

			String name = getName();

			if (name != null) {
				return name;
			}

			throw new IllegalStateException(String.format("Required name for %s not available!", this));
		}

		/**
		 * @return the position
		 */
		@Nullable
		Integer getPosition() {
			return position;
		}

		/**
		 * @return the position
		 * @throws IllegalStateException if the position is not available.
		 * @since 2.0
		 */
		int getRequiredPosition() throws IllegalStateException {

			Integer position = getPosition();

			if (position != null) {
				return position;
			}

			throw new IllegalStateException(String.format("Required position for %s not available!", this));
		}

		/**
		 * @return {@literal true} if this parameter binding is a synthetic SpEL
		 *         expression.
		 */
		public boolean isExpression() {
			return this.expression != null;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#hashCode()
		 */
		@Override
		public int hashCode() {

			int result = 17;

			result += nullSafeHashCode(this.name);
			result += nullSafeHashCode(this.position);
			result += nullSafeHashCode(this.expression);

			return result;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		@Override
		public boolean equals(Object obj) {

			if (!(obj instanceof ParameterBinding)) {
				return false;
			}

			ParameterBinding that = (ParameterBinding) obj;

			return nullSafeEquals(this.name, that.name) && nullSafeEquals(this.position, that.position)
					&& nullSafeEquals(this.expression, that.expression);
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return String.format("ParameterBinding [name: %s, position: %d, expression: %s]", getName(), getPosition(),
					getExpression());
		}

		/**
		 * @param valueToBind value to prepare
		 */
		@Nullable
		public Object prepare(@Nullable Object valueToBind) {
			return valueToBind;
		}

		@Nullable
		public String getExpression() {
			return expression;
		}
	}

	/**
	 * Represents a {@link ParameterBinding} in a JPQL query augmented with
	 * instructions of how to apply a parameter as an {@code IN} parameter.
	 *
	 * @author Thomas Darimont
	 */
	static class InParameterBinding extends ParameterBinding {

		/**
		 * Creates a new {@link InParameterBinding} for the parameter with the given
		 * name.
		 */
		InParameterBinding(String name, @Nullable String expression) {
			super(name, null, expression);
		}

		/**
		 * Creates a new {@link InParameterBinding} for the parameter with the given
		 * position.
		 */
		InParameterBinding(int position, @Nullable String expression) {
			super(null, position, expression);
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.springframework.data.jpa.repository.query.StringQuery.ParameterBinding#
		 * prepare(java.lang.Object)
		 */
		@Override
		public Object prepare(@Nullable Object value) {

			if (!ObjectUtils.isArray(value)) {
				return value;
			}

			int length = Array.getLength(value);
			Collection<Object> result = new ArrayList<>(length);

			for (int i = 0; i < length; i++) {
				result.add(Array.get(value, i));
			}

			return result;
		}
	}

	/**
	 * Represents a parameter binding in a JPQL query augmented with instructions of
	 * how to apply a parameter as LIKE parameter. This allows expressions like
	 * {@code 鈥ike %?1} in the JPQL query, which is not allowed by plain JPA.
	 *
	 * @author Oliver Gierke
	 * @author Thomas Darimont
	 */
	static class LikeParameterBinding extends ParameterBinding {

		private static final List<Type> SUPPORTED_TYPES = Arrays.asList(Type.CONTAINING, Type.STARTING_WITH,
				Type.ENDING_WITH, Type.LIKE);

		private final Type type;

		/**
		 * Creates a new {@link LikeParameterBinding} for the parameter with the given
		 * name and {@link Type}.
		 *
		 * @param name must not be {@literal null} or empty.
		 * @param type must not be {@literal null}.
		 */
		LikeParameterBinding(String name, Type type) {
			this(name, type, null);
		}

		/**
		 * Creates a new {@link LikeParameterBinding} for the parameter with the given
		 * name and {@link Type} and parameter binding input.
		 *
		 * @param name       must not be {@literal null} or empty.
		 * @param type       must not be {@literal null}.
		 * @param expression may be {@literal null}.
		 */
		LikeParameterBinding(String name, Type type, @Nullable String expression) {

			super(name, null, expression);

			Assert.hasText(name, "Name must not be null or empty!");
			Assert.notNull(type, "Type must not be null!");

			Assert.isTrue(SUPPORTED_TYPES.contains(type), String.format("Type must be one of %s!",
					StringUtils.collectionToCommaDelimitedString(SUPPORTED_TYPES)));

			this.type = type;
		}

		/**
		 * Creates a new {@link LikeParameterBinding} for the parameter with the given
		 * position and {@link Type}.
		 *
		 * @param position position of the parameter in the query.
		 * @param type     must not be {@literal null}.
		 */
		LikeParameterBinding(int position, Type type) {
			this(position, type, null);
		}

		/**
		 * Creates a new {@link LikeParameterBinding} for the parameter with the given
		 * position and {@link Type}.
		 *
		 * @param position   position of the parameter in the query.
		 * @param type       must not be {@literal null}.
		 * @param expression may be {@literal null}.
		 */
		LikeParameterBinding(int position, Type type, @Nullable String expression) {

			super(null, position, expression);

			Assert.isTrue(position > 0, "Position must be greater than zero!");
			Assert.notNull(type, "Type must not be null!");

			Assert.isTrue(SUPPORTED_TYPES.contains(type), String.format("Type must be one of %s!",
					StringUtils.collectionToCommaDelimitedString(SUPPORTED_TYPES)));

			this.type = type;
		}

		/**
		 * Returns the {@link Type} of the binding.
		 *
		 * @return the type
		 */
		public Type getType() {
			return type;
		}

		/**
		 * Prepares the given raw keyword according to the like type.
		 */
		@Nullable
		@Override
		public Object prepare(@Nullable Object value) {

			if (value == null) {
				return null;
			}

			switch (type) {
			case STARTING_WITH:
				return String.format("%s%%", value.toString());
			case ENDING_WITH:
				return String.format("%%%s", value.toString());
			case CONTAINING:
				return String.format("%%%s%%", value.toString());
			case LIKE:
			default:
				return value;
			}
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		@Override
		public boolean equals(Object obj) {

			if (!(obj instanceof LikeParameterBinding)) {
				return false;
			}

			LikeParameterBinding that = (LikeParameterBinding) obj;

			return super.equals(obj) && this.type.equals(that.type);
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#hashCode()
		 */
		@Override
		public int hashCode() {

			int result = super.hashCode();

			result += nullSafeHashCode(this.type);

			return result;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return String.format("LikeBinding [name: %s, position: %d, type: %s]", getName(), getPosition(), type);
		}

		/**
		 * Extracts the like {@link Type} from the given JPA like expression.
		 *
		 * @param expression must not be {@literal null} or empty.
		 */
		private static Type getLikeTypeFrom(String expression) {

			Assert.hasText(expression, "Expression must not be null or empty!");

			if (expression.matches("%.*%")) {
				return Type.CONTAINING;
			}

			if (expression.startsWith("%")) {
				return Type.ENDING_WITH;
			}

			if (expression.endsWith("%")) {
				return Type.STARTING_WITH;
			}

			return Type.LIKE;
		}
	}

	static class Metadata {
		private boolean usesJdbcStyleParameters = false;
	}

}
