package org.springframework.data.jdbc.repository.query;

import java.util.Optional;

import org.springframework.data.domain.Sort;
import org.springframework.data.jdbc.core.DefaultStatementMapper;
import org.springframework.data.jdbc.core.StatementMapper;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.dialect.RenderContextFactory;
import org.springframework.data.relational.core.mapping.PersistentPropertyPathExtension;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.query.CriteriaDefinition;
import org.springframework.data.relational.core.sql.Functions;
import org.springframework.data.relational.core.sql.render.RenderContext;
import org.springframework.data.relational.repository.query.RelationalEntityMetadata;
import org.springframework.data.relational.repository.query.RelationalParameterAccessor;
import org.springframework.data.relational.repository.query.RelationalQueryCreator;
import org.springframework.data.repository.query.Parameters;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

public class JdbcTotalQueryCreator extends JdbcQueryCreator {

	private final UpdateMapper queryMapper;
	private final StatementMapper statementMapper;
	private final RelationalEntityMetadata<?> entityMetadata;
	private final RenderContextFactory renderContextFactory;

	/**
	 * Creates new instance of this class with the given {@link PartTree},
	 * {@link JdbcConverter}, {@link Dialect}, {@link RelationalEntityMetadata} and
	 * {@link RelationalParameterAccessor}.
	 *
	 * @param context
	 * @param tree           part tree, must not be {@literal null}.
	 * @param converter      must not be {@literal null}.
	 * @param dialect        must not be {@literal null}.
	 * @param entityMetadata relational entity metadata, must not be
	 *                       {@literal null}.
	 * @param accessor       parameter metadata provider, must not be
	 *                       {@literal null}.
	 */
	public JdbcTotalQueryCreator(RelationalMappingContext context, PartTree tree, JdbcConverter converter,
			Dialect dialect, RelationalEntityMetadata<?> entityMetadata, RelationalParameterAccessor accessor) {
		super(context, tree, converter, dialect, entityMetadata, accessor);

		Assert.notNull(converter, "JdbcConverter must not be null");
		Assert.notNull(dialect, "Dialect must not be null");
		Assert.notNull(entityMetadata, "Relational entity metadata must not be null");

		this.entityMetadata = entityMetadata;
		this.queryMapper = new UpdateMapper(dialect, converter, context);
		this.renderContextFactory = new RenderContextFactory(dialect);

		RenderContext renderContext = renderContextFactory.createRenderContext();

		this.statementMapper = new DefaultStatementMapper(dialect, renderContext, queryMapper, context);
	}

	/**
	 * Validate parameters for the derived query. Specifically checking that the
	 * query method defines scalar parameters and collection parameters where
	 * required and that invalid parameter declarations are rejected.
	 *
	 * @param tree
	 * @param parameters
	 */
	static void validate(PartTree tree, Parameters<?, ?> parameters,
			MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> context) {

		RelationalQueryCreator.validate(tree, parameters);

		for (PartTree.OrPart parts : tree) {
			for (Part part : parts) {

				PersistentPropertyPath<? extends RelationalPersistentProperty> propertyPath = context
						.getPersistentPropertyPath(part.getProperty());
				PersistentPropertyPathExtension path = new PersistentPropertyPathExtension(context, propertyPath);

				for (PersistentPropertyPathExtension pathToValidate = path; path.getLength() > 0; path = path
						.getParentPath()) {
					validateProperty(pathToValidate);
				}
			}
		}
	}

	private static void validateProperty(PersistentPropertyPathExtension path) {

		if (!path.getParentPath().isEmbedded() && path.getLength() > 1) {
			throw new IllegalArgumentException(String.format("Cannot query by nested property: %s",
					path.getRequiredPersistentPropertyPath().toDotPath()));
		}

		if (path.isMultiValued() || path.isMap()) {
			throw new IllegalArgumentException(String.format("Cannot query by multi-valued property: %s",
					path.getRequiredPersistentPropertyPath().getLeafProperty().getName()));
		}

		if (!path.isEmbedded() && path.isEntity()) {
			throw new IllegalArgumentException(String.format("Cannot query by nested entity: %s",
					path.getRequiredPersistentPropertyPath().toDotPath()));
		}

		if (path.getRequiredPersistentPropertyPath().getLeafProperty().isReference()) {
			throw new IllegalArgumentException(String.format("Cannot query by reference: %s",
					path.getRequiredPersistentPropertyPath().toDotPath()));
		}
	}

	/**
	 * Creates {@link ParametrizedQuery} applying the given {@link Criteria} and
	 * {@link Sort} definition.
	 *
	 * @param criteria {@link Criteria} to be applied to query
	 * @param sort     sort option to be applied to query, must not be
	 *                 {@literal null}.
	 * @return instance of {@link ParametrizedQuery}
	 */
	@Override
	protected ParametrizedQuery complete(@Nullable Criteria criteria, Sort sort) {
		RelationalPersistentEntity<?> entity = entityMetadata.getTableEntity();

		StatementMapper statementMapper = this.statementMapper.forType(entity.getType());

		StatementMapper.SelectSpec selectSpec = statementMapper //
				.createSelect(entity.getTableName().toString()) //
				.doWithTable((table, spec) -> {
					return spec.withProjection(
							Functions.count(table.column(entity.getRequiredIdProperty().getColumnName())));
				});

		Optional<CriteriaDefinition> criteriaOptional = Optional.of(criteria);
		if (criteriaOptional.isPresent()) {
			selectSpec = criteriaOptional.map(selectSpec::withCriteria).orElse(selectSpec);
		}

		return statementMapper.getMappedObject(selectSpec);
	}

}
