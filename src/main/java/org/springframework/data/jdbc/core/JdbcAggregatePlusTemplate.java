package org.springframework.data.jdbc.core;

import java.beans.FeatureDescriptor;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.springframework.beans.BeanWrapper;
import org.springframework.beans.BeanWrapperImpl;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.context.ApplicationContext;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jdbc.core.convert.DataAccessStrategySupport;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.mapping.ManyToMany;
import org.springframework.data.projection.ProjectionInformation;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.query.CriteriaDefinition;
import org.springframework.data.relational.core.query.Query;
import org.springframework.data.relational.core.sql.Expression;
import org.springframework.data.relational.core.sql.Functions;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.data.util.ParsingUtils;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

/**
 * 
 * @author Jerry Pan, NJUST
 *
 */
public class JdbcAggregatePlusTemplate extends JdbcAggregateTemplate
		implements JdbcAggregateOperations, BeanFactoryAware {

	private final static String manyToManyDeleteSqlFormat = "delete from %s where %s = :%s";

	private final static String manyToManyInsertSqlFormat = "insert into %s(%s, %s) values(:%s, :%s)";

	private final DataAccessStrategySupport accessStrategy;

	private final RelationalMappingContext context;

	private final SpelAwareProxyProjectionFactory projectionFactory;

	public JdbcAggregatePlusTemplate(ApplicationContext publisher, RelationalMappingContext context,
			JdbcConverter converter, DataAccessStrategySupport dataAccessStrategy) {
		super(publisher, context, converter, dataAccessStrategy);

		this.context = context;
		this.accessStrategy = dataAccessStrategy;
		this.projectionFactory = new SpelAwareProxyProjectionFactory();
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.projectionFactory.setBeanFactory(beanFactory);
	}

	@Override
	public <T> T save(T instance) {
		T entity = super.save(instance);
		Field[] fields = instance.getClass().getDeclaredFields();
		RelationalPersistentEntity<?> persistentEntity = context.getRequiredPersistentEntity(entity.getClass());

		RelationalPersistentProperty idProperty = persistentEntity.getIdProperty();
		BeanWrapper bw = new BeanWrapperImpl(entity);
		Object id = bw.getPropertyValue(idProperty.getName());

		for (Field field : fields) {
			ManyToMany mtm = AnnotatedElementUtils.findMergedAnnotation(field, ManyToMany.class);
			if (mtm != null && StringUtils.hasText(mtm.column()) && StringUtils.hasText(mtm.inverseColumn())
					&& StringUtils.hasText(mtm.table())) {
				String reference = mtm.table();
				String localColumn = mtm.column();
				String inverseColumn = mtm.inverseColumn();

				String lc = ParsingUtils.reconcatenateCamelCase(localColumn, "_");
				String ic = ParsingUtils.reconcatenateCamelCase(inverseColumn, "_");

				String deleteSql = String.format(manyToManyDeleteSqlFormat, reference, lc, lc);

				MapSqlParameterSource ps = new MapSqlParameterSource();
				ps.addValue(lc, id);

				accessStrategy.update(deleteSql, ps);

				String insertSql = String.format(manyToManyInsertSqlFormat, reference, lc, ic, lc, ic);

				Collection<?> collection = (Collection<?>) bw.getPropertyValue(field.getName());

				if (!CollectionUtils.isEmpty(collection)) {
					RelationalPersistentEntity<?> pe = null;
					RelationalPersistentProperty ip = null;
					List<SqlParameterSource> psList = new ArrayList<SqlParameterSource>();
					for (Object item : collection) {
						if (pe == null) {
							pe = context.getRequiredPersistentEntity(item.getClass());
							ip = pe.getIdProperty();
						}

						BeanWrapper ibw = new BeanWrapperImpl(item);
						Object inverseId = ibw.getPropertyValue(ip.getName());

						MapSqlParameterSource ips = new MapSqlParameterSource();
						ips.addValue(lc, id);
						ips.addValue(ic, inverseId);

						psList.add(ips);
					}

					accessStrategy.batchUpdate(insertSql, psList.toArray(new SqlParameterSource[psList.size()]));
				}

			}
		}

		return entity;
	}

	public Long count(Query query, Class<?> entityClass) {
		return doCount(query, entityClass, getTableName(entityClass));
	}

	public <T> List<T> findList(Class<T> entityClass) {
		Iterable<T> items = findAll(entityClass);
		return StreamSupport.stream(items.spliterator(), false).collect(Collectors.toList());
	}

	public <T> List<T> findList(Query query, Class<T> entityClass) {
		Iterable<T> items = findAll(query, entityClass);
		return StreamSupport.stream(items.spliterator(), false).collect(Collectors.toList());
	}

	public <T> Iterable<T> findAll(Query query, Class<T> entityClass) {
		return doFind(query, entityClass, getTableName(entityClass), entityClass);
	}

	public <T> T findOne(Query query, Class<T> entityClass) {
		Iterator<T> iterator = doFind(query.limit(1), entityClass, getTableName(entityClass), entityClass).iterator();
		return iterator.hasNext() ? iterator.next() : null;
	}

	public <T> Page<T> findPage(Query query, Class<T> entityClass) {
		SqlIdentifier tableName = getTableName(entityClass);
		long totalCount = doCount(query, entityClass, tableName);

		int page = Long.valueOf(query.getOffset() / query.getLimit()).intValue();

		Pageable pageable = PageRequest.of(page, query.getLimit());

		if (totalCount <= 0) {
			return new PageImpl<T>(Collections.emptyList(), pageable, totalCount);
		}

		Iterable<T> items = doFind(query, entityClass, tableName, entityClass);

		return new PageImpl<T>(StreamSupport.stream(items.spliterator(), false).collect(Collectors.toList()), pageable,
				totalCount);
	}

	<T> Iterable<T> doFind(Query query, Class<?> entityClass, SqlIdentifier tableName, Class<T> returnType) {
		if (CollectionUtils.isEmpty(query.getColumns())) {
			RelationalPersistentEntity<?> relationalPersistentEntity = getRequiredEntity(entityClass);

			List<String> columns = new ArrayList<String>();
			Iterator<RelationalPersistentProperty> iterator = relationalPersistentEntity.iterator();

			while (iterator.hasNext()) {
				RelationalPersistentProperty persistentProperty = iterator.next();
				String property = persistentProperty.getName();

				columns.add(property);
			}

			query = query.columns(columns);
		}

		Query q = query;

		StatementMapper statementMapper = accessStrategy.getStatementMapper().forType(entityClass);

		StatementMapper.SelectSpec selectSpec = statementMapper //
				.createSelect(tableName) //
				.doWithTable((table, spec) -> spec.withProjection(getSelectProjection(table, q, returnType)));

		if (query.getLimit() > 0) {
			selectSpec = selectSpec.limit(query.getLimit());
		}

		if (query.getOffset() > 0) {
			selectSpec = selectSpec.offset(query.getOffset());
		}

		if (query.isSorted()) {
			selectSpec = selectSpec.withSort(query.getSort());
		}

		Optional<CriteriaDefinition> criteria = query.getCriteria();
		if (criteria.isPresent()) {
			selectSpec = criteria.map(selectSpec::withCriteria).orElse(selectSpec);
		}

		PreparedOperation<?> operation = statementMapper.getMappedObject(selectSpec);

		return accessStrategy.findAll(operation, returnType);
	}

	<T> Long doCount(Query query, Class<?> entityClass, SqlIdentifier tableName) {
		RelationalPersistentEntity<?> entity = getRequiredEntity(entityClass);
		StatementMapper statementMapper = accessStrategy.getStatementMapper().forType(entityClass);

		StatementMapper.SelectSpec selectSpec = statementMapper //
				.createSelect(tableName) //
				.doWithTable((table, spec) -> {
					return spec.withProjection(
							Functions.count(table.column(entity.getRequiredIdProperty().getColumnName())));
				});

		Optional<CriteriaDefinition> criteria = query.getCriteria();
		if (criteria.isPresent()) {
			selectSpec = criteria.map(selectSpec::withCriteria).orElse(selectSpec);
		}

		PreparedOperation<?> operation = statementMapper.getMappedObject(selectSpec);

		return accessStrategy.count(operation, entityClass);
	}

	SqlIdentifier getTableName(Class<?> entityClass) {
		return getRequiredEntity(entityClass).getTableName();
	}

	private RelationalPersistentEntity<?> getRequiredEntity(Class<?> entityClass) {
		return this.context.getRequiredPersistentEntity(entityClass);
	}

	private <T> List<Expression> getSelectProjection(Table table, Query query, Class<T> returnType) {

		if (query.getColumns().isEmpty()) {

			if (returnType.isInterface()) {

				ProjectionInformation projectionInformation = projectionFactory.getProjectionInformation(returnType);

				if (projectionInformation.isClosed()) {
					return projectionInformation.getInputProperties().stream().map(FeatureDescriptor::getName)
							.map(table::column).collect(Collectors.toList());
				}
			}

			return Collections.singletonList(table.asterisk());
		}

		return query.getColumns().stream().map(table::column).collect(Collectors.toList());
	}

}