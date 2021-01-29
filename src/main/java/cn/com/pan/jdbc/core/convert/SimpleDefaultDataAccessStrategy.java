package cn.com.pan.jdbc.core.convert;

import org.springframework.data.jdbc.core.convert.DefaultDataAccessStrategy;
import org.springframework.data.jdbc.core.convert.EntityRowMapper;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.convert.SqlGeneratorSource;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.dialect.RenderContextFactory;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.sql.render.RenderContext;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;

import cn.com.pan.jdbc.core.DefaultStatementMapper;
import cn.com.pan.jdbc.core.PreparedOperation;
import cn.com.pan.jdbc.core.StatementMapper;
import cn.com.pan.jdbc.query.UpdateMapper;

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

	@SuppressWarnings("unchecked")
	public <T> Iterable<T> findAll(PreparedOperation<?> operation, Class<T> domainType) {
		String sql = operation.get();
		return operations.query(sql, operation.getSqlParameterSource(), (RowMapper<T>) getEntityRowMapper(domainType));
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
