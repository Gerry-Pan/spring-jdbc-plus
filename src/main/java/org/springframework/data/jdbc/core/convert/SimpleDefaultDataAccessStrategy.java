package org.springframework.data.jdbc.core.convert;

import java.util.Map;

import org.springframework.data.jdbc.core.DefaultStatementMapper;
import org.springframework.data.jdbc.core.PreparedOperation;
import org.springframework.data.jdbc.core.StatementMapper;
import org.springframework.data.jdbc.repository.query.UpdateMapper;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.dialect.RenderContextFactory;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.sql.render.RenderContext;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

public class SimpleDefaultDataAccessStrategy extends DefaultDataAccessStrategy implements DataAccessStrategySupport {

	private final JdbcConverter converter;

	private final UpdateMapper updateMapper;

	private final StatementMapper statementMapper;

	private final RelationalMappingContext context;

	private final NamedParameterJdbcOperations operations;

	public SimpleDefaultDataAccessStrategy(Dialect dialect, SqlGeneratorSource sqlGeneratorSource,
			RelationalMappingContext context, JdbcConverter converter, NamedParameterJdbcOperations operations) {
		super(sqlGeneratorSource, context, converter, operations);

		RenderContextFactory factory = new RenderContextFactory(dialect);
		RenderContext renderContext = factory.createRenderContext();

		this.context = context;
		this.converter = converter;
		this.operations = operations;
		this.updateMapper = new UpdateMapper(dialect, converter, context);
		this.statementMapper = new DefaultStatementMapper(dialect, renderContext, this.updateMapper, context);
	}

	@Override
	public long count(PreparedOperation<?> operation, Class<?> domainType) {
		String sql = operation.get();
		return operations.queryForObject(sql, operation.getSqlParameterSource(), Long.class);
	}

	@SuppressWarnings("unchecked")
	public <T> Iterable<T> findAll(PreparedOperation<?> operation, Class<T> domainType) {
		String sql = operation.get();
		return operations.query(sql, operation.getSqlParameterSource(), (RowMapper<T>) getEntityRowMapper(domainType));
	}

	public int update(String sql, Map<String, ?> paramMap) {
		return operations.update(sql, paramMap);
	}

	public int update(String sql, SqlParameterSource paramSource) {
		return operations.update(sql, paramSource);
	}

	public int[] batchUpdate(String sql, Map<String, ?>[] batchValues) {
		return operations.batchUpdate(sql, batchValues);
	}

	public int[] batchUpdate(String sql, SqlParameterSource[] batchArgs) {
		return operations.batchUpdate(sql, batchArgs);
	}

	private EntityRowMapper<?> getEntityRowMapper(Class<?> domainType) {
		return new EntityRowMapper<>(getRequiredPersistentEntity(domainType), converter);
	}

	@SuppressWarnings("unchecked")
	private <S> RelationalPersistentEntity<S> getRequiredPersistentEntity(Class<S> domainType) {
		return (RelationalPersistentEntity<S>) context.getRequiredPersistentEntity(domainType);
	}

	public StatementMapper getStatementMapper() {
		return this.statementMapper;
	}

}
