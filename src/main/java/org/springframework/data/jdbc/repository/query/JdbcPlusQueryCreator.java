package org.springframework.data.jdbc.repository.query;

import java.lang.reflect.Field;

import org.springframework.data.domain.Sort;
import org.springframework.data.jdbc.core.DefaultStatementMapper;
import org.springframework.data.jdbc.core.PreparedOperation;
import org.springframework.data.jdbc.core.StatementMapper;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.dialect.RenderContextFactory;
import org.springframework.data.relational.core.mapping.ManyToMany;
import org.springframework.data.relational.core.mapping.ManyToOne;
import org.springframework.data.relational.core.mapping.OneToMany;
import org.springframework.data.relational.core.mapping.PersistentPropertyPathExtension;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.query.Query;
import org.springframework.data.relational.core.sql.render.RenderContext;
import org.springframework.data.relational.repository.query.RelationalEntityMetadata;
import org.springframework.data.relational.repository.query.RelationalParameterAccessor;
import org.springframework.data.relational.repository.query.RelationalQueryCreator;
import org.springframework.data.repository.query.Parameters;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.lang.Nullable;
import org.springframework.util.ReflectionUtils;

public class JdbcPlusQueryCreator extends RelationalQueryCreator<ParametrizedQuery> {

	private final UpdateMapper queryMapper;
	private final StatementMapper statementMapper;
	private final RenderContextFactory renderContextFactory;
	private final RelationalEntityMetadata<?> entityMetadata;
	private final RelationalParameterAccessor accessor;

	JdbcPlusQueryCreator(RelationalMappingContext context, PartTree tree, JdbcConverter converter, Dialect dialect,
			RelationalEntityMetadata<?> entityMetadata, RelationalParameterAccessor accessor) {
		super(tree, accessor);

		this.accessor = accessor;
		this.entityMetadata = entityMetadata;
		this.queryMapper = new UpdateMapper(dialect, converter, context);
		this.renderContextFactory = new RenderContextFactory(dialect);

		RenderContext renderContext = renderContextFactory.createRenderContext();

		this.statementMapper = new DefaultStatementMapper(dialect, renderContext, queryMapper, context);
	}

	protected ParametrizedQuery complete(@Nullable Criteria criteria, Sort sort) {
		RelationalPersistentEntity<?> entity = entityMetadata.getTableEntity();
		Query query = Query.query(criteria).with(accessor.getPageable()).sort(sort);
		StatementMapper statementMapper = this.statementMapper.forType(entity.getType());
		PreparedOperation<?> operation = statementMapper.getMappedObject(query);

		String sql = operation.get();

		return new ParametrizedQuery(sql, operation.getSqlParameterSource());
	}

	static void validate(PartTree tree, Parameters<?, ?> parameters,
			MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> context) {

		RelationalQueryCreator.validate(tree, parameters);

		for (PartTree.OrPart parts : tree) {
			for (Part part : parts) {
				PropertyPath pp = part.getProperty();
				Field field = ReflectionUtils.findField(pp.getOwningType().getType(), pp.getSegment());

				if (field == null) {
					throw new IllegalArgumentException(
							String.format("Cannot query by nested property: %s", pp.toDotPath()));
				}

				if (field.isAnnotationPresent(ManyToOne.class) || field.isAnnotationPresent(OneToMany.class)
						|| field.isAnnotationPresent(ManyToMany.class)) {
					continue;
				}

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
		RelationalPersistentProperty persistentProperty = path.getRequiredPersistentPropertyPath().getBaseProperty();

		if (persistentProperty == null || persistentProperty.isEntity()
				|| persistentProperty.isAnnotationPresent(ManyToOne.class)
				|| persistentProperty.isAnnotationPresent(OneToMany.class)
				|| persistentProperty.isAnnotationPresent(ManyToMany.class)) {
			return;
		}

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

}
