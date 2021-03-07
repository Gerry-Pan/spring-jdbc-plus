package org.springframework.data.jdbc.repository.query;

import java.sql.ResultSet;
import java.util.Collections;
import java.util.List;

import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Sort;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.repository.query.RelationalEntityMetadata;
import org.springframework.data.relational.repository.query.RelationalParameterAccessor;
import org.springframework.data.relational.repository.query.RelationalParametersParameterAccessor;
import org.springframework.data.repository.query.Parameters;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.RowMapperResultSetExtractor;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

public class PartTreeJdbcQuery extends AbstractJdbcQuery {

	private final RelationalMappingContext context;
	private final Parameters<?, ?> parameters;
	private final Dialect dialect;
	private final JdbcConverter converter;
	private final PartTree tree;
	private final JdbcQueryExecution<?> execution;
	private final JdbcQueryExecution<Long> totalExecution;
	private final NamedParameterJdbcOperations operations;

	/**
	 * Creates a new {@link PartTreeJdbcQuery}.
	 *
	 * @param context     must not be {@literal null}.
	 * @param queryMethod must not be {@literal null}.
	 * @param dialect     must not be {@literal null}.
	 * @param converter   must not be {@literal null}.
	 * @param operations  must not be {@literal null}.
	 * @param rowMapper   must not be {@literal null}.
	 */
	public PartTreeJdbcQuery(RelationalMappingContext context, JdbcQueryMethod queryMethod, Dialect dialect,
			JdbcConverter converter, NamedParameterJdbcOperations operations, RowMapper<Object> rowMapper) {

		super(queryMethod, operations, rowMapper);

		Assert.notNull(context, "RelationalMappingContext must not be null");
		Assert.notNull(queryMethod, "JdbcQueryMethod must not be null");
		Assert.notNull(dialect, "Dialect must not be null");
		Assert.notNull(converter, "JdbcConverter must not be null");

		this.context = context;
		this.parameters = queryMethod.getParameters();
		this.dialect = dialect;
		this.converter = converter;
		this.operations = operations;

		this.tree = new PartTree(queryMethod.getName(), queryMethod.getEntityInformation().getJavaType());
		JdbcPlusQueryCreator.validate(this.tree, this.parameters, this.converter.getMappingContext());

		ResultSetExtractor<Boolean> extractor = tree.isExistsProjection() ? (ResultSet::next) : null;

		if (queryMethod.isPageQuery() || queryMethod.isSliceQuery()) {
			this.execution = getCollectionQueryExecution(queryMethod, extractor, rowMapper);
			this.totalExecution = getTotalQueryExecution();
		} else {
			this.totalExecution = null;
			this.execution = getQueryExecution(queryMethod, extractor, rowMapper);
		}
	}

	private Sort getDynamicSort(RelationalParameterAccessor accessor) {
		return parameters.potentiallySortsDynamically() ? accessor.getSort() : Sort.unsorted();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.springframework.data.repository.query.RepositoryQuery#execute(java.lang.
	 * Object[])
	 */
	@Override
	public Object execute(Object[] values) {
		RelationalParametersParameterAccessor accessor = new RelationalParametersParameterAccessor(getQueryMethod(),
				values);

		JdbcQueryMethod queryMethod = getQueryMethod();

		if (queryMethod.isPageQuery() || queryMethod.isSliceQuery()) {
			ParametrizedQuery totalQuery = createTotalQuery(accessor);

			Long total = this.totalExecution.execute(totalQuery.getQuery(), totalQuery.getParameterSource());

			if (total <= 0) {
				return new PageImpl<>(Collections.emptyList(), accessor.getPageable(), total);
			}

			ParametrizedQuery query = createQuery(accessor);
			Object result = this.execution.execute(query.getQuery(), query.getParameterSource());

			return new PageImpl<>((List<?>) result, accessor.getPageable(), total);
		} else {
			ParametrizedQuery query = createQuery(accessor);
			return this.execution.execute(query.getQuery(), query.getParameterSource());
		}
	}

	protected ParametrizedQuery createQuery(RelationalParametersParameterAccessor accessor) {

		RelationalEntityMetadata<?> entityMetadata = getQueryMethod().getEntityInformation();
		JdbcPlusQueryCreator queryCreator = new JdbcPlusQueryCreator(context, tree, converter, dialect, entityMetadata,
				accessor);

		return queryCreator.createQuery(getDynamicSort(accessor));
	}

	protected ParametrizedQuery createTotalQuery(RelationalParametersParameterAccessor accessor) {
		RelationalEntityMetadata<?> entityMetadata = getQueryMethod().getEntityInformation();
		JdbcTotalQueryCreator totalQueryCreator = new JdbcTotalQueryCreator(context, tree, converter, dialect,
				entityMetadata, accessor);

		return totalQueryCreator.createQuery();
	}

	protected JdbcQueryExecution<Long> getTotalQueryExecution() {
		return totalObjectQuery();
	}

	protected JdbcQueryExecution<?> getCollectionQueryExecution(JdbcQueryMethod queryMethod,
			@Nullable ResultSetExtractor<?> extractor, RowMapper<?> rowMapper) {
		return extractor != null ? getQueryExecution(extractor) : collectionQuery(rowMapper);
	}

	private <T> JdbcQueryExecution<List<T>> collectionQuery(RowMapper<T> rowMapper) {
		return getQueryExecution(new RowMapperResultSetExtractor<>(rowMapper));
	}

	private <T> JdbcQueryExecution<T> getQueryExecution(ResultSetExtractor<T> resultSetExtractor) {
		return (query, parameters) -> operations.query(query, parameters, resultSetExtractor);
	}

	private JdbcQueryExecution<Long> totalObjectQuery() {

		return (query, parameters) -> {
			try {
				return operations.queryForObject(query, parameters, Long.class);
			} catch (EmptyResultDataAccessException e) {
				return null;
			}
		};
	}

}
