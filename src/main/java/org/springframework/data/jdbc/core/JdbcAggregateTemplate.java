package org.springframework.data.jdbc.core;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.mapping.IdentifierAccessor;
import org.springframework.data.mapping.callback.EntityCallbacks;
import org.springframework.data.relational.core.conversion.AggregateChange;
import org.springframework.data.relational.core.conversion.MutableAggregateChange;
import org.springframework.data.relational.core.conversion.RelationalEntityDeleteWriter;
import org.springframework.data.relational.core.conversion.RelationalEntityInsertWriter;
import org.springframework.data.relational.core.conversion.RelationalEntityUpdateWriter;
import org.springframework.data.relational.core.mapping.ManyToMany;
import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.event.AfterDeleteCallback;
import org.springframework.data.relational.core.mapping.event.AfterDeleteEvent;
import org.springframework.data.relational.core.mapping.event.AfterLoadCallback;
import org.springframework.data.relational.core.mapping.event.AfterLoadEvent;
import org.springframework.data.relational.core.mapping.event.AfterSaveCallback;
import org.springframework.data.relational.core.mapping.event.AfterSaveEvent;
import org.springframework.data.relational.core.mapping.event.BeforeConvertCallback;
import org.springframework.data.relational.core.mapping.event.BeforeDeleteCallback;
import org.springframework.data.relational.core.mapping.event.BeforeDeleteEvent;
import org.springframework.data.relational.core.mapping.event.BeforeSaveCallback;
import org.springframework.data.relational.core.mapping.event.BeforeSaveEvent;
import org.springframework.data.relational.core.mapping.event.Identifier;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

public class JdbcAggregateTemplate implements JdbcAggregateOperations {

	protected final static String manyToManyDeleteAllSqlFormat = "DELETE FROM %s";

	protected final static String manyToManyDeleteSqlFormat = "DELETE FROM %s WHERE %s = :%s";

	protected final static String manyToManyInsertSqlFormat = "INSERT INTO %s(%s, %s) VALUES(:%s, :%s)";

	private final ApplicationEventPublisher publisher;
	private final RelationalMappingContext context;

	private final RelationalEntityDeleteWriter jdbcEntityDeleteWriter;
	private final RelationalEntityInsertWriter jdbcEntityInsertWriter;
	private final RelationalEntityUpdateWriter jdbcEntityUpdateWriter;

	private final DataAccessStrategy accessStrategy;
	private final AggregateChangeExecutor executor;

	private NamedParameterJdbcOperations operations;

	private EntityCallbacks entityCallbacks = EntityCallbacks.create();

	/**
	 * Creates a new {@link JdbcAggregateTemplate} given {@link ApplicationContext},
	 * {@link RelationalMappingContext} and {@link DataAccessStrategy}.
	 *
	 * @param publisher          must not be {@literal null}.
	 * @param context            must not be {@literal null}.
	 * @param dataAccessStrategy must not be {@literal null}.
	 * @since 1.1
	 */
	public JdbcAggregateTemplate(ApplicationContext publisher, RelationalMappingContext context,
			JdbcConverter converter, DataAccessStrategy dataAccessStrategy) {

		Assert.notNull(publisher, "ApplicationContext must not be null!");
		Assert.notNull(context, "RelationalMappingContext must not be null!");
		Assert.notNull(converter, "RelationalConverter must not be null!");
		Assert.notNull(dataAccessStrategy, "DataAccessStrategy must not be null!");

		this.publisher = publisher;
		this.context = context;
		this.accessStrategy = dataAccessStrategy;

		this.jdbcEntityInsertWriter = new RelationalEntityInsertWriter(context);
		this.jdbcEntityUpdateWriter = new RelationalEntityUpdateWriter(context);
		this.jdbcEntityDeleteWriter = new RelationalEntityDeleteWriter(context);

		this.executor = new AggregateChangeExecutor(converter, accessStrategy);

		setEntityCallbacks(EntityCallbacks.create(publisher));
	}

	/**
	 * Creates a new {@link JdbcAggregateTemplate} given
	 * {@link ApplicationEventPublisher}, {@link RelationalMappingContext} and
	 * {@link DataAccessStrategy}.
	 *
	 * @param publisher          must not be {@literal null}.
	 * @param context            must not be {@literal null}.
	 * @param dataAccessStrategy must not be {@literal null}.
	 */
	public JdbcAggregateTemplate(ApplicationEventPublisher publisher, RelationalMappingContext context,
			JdbcConverter converter, DataAccessStrategy dataAccessStrategy) {

		Assert.notNull(publisher, "ApplicationEventPublisher must not be null!");
		Assert.notNull(context, "RelationalMappingContext must not be null!");
		Assert.notNull(converter, "RelationalConverter must not be null!");
		Assert.notNull(dataAccessStrategy, "DataAccessStrategy must not be null!");

		this.publisher = publisher;
		this.context = context;
		this.accessStrategy = dataAccessStrategy;

		this.jdbcEntityInsertWriter = new RelationalEntityInsertWriter(context);
		this.jdbcEntityUpdateWriter = new RelationalEntityUpdateWriter(context);
		this.jdbcEntityDeleteWriter = new RelationalEntityDeleteWriter(context);
		this.executor = new AggregateChangeExecutor(converter, accessStrategy);
	}

	/**
	 * @param entityCallbacks
	 * @since 1.1
	 */
	public void setEntityCallbacks(EntityCallbacks entityCallbacks) {

		Assert.notNull(entityCallbacks, "Callbacks must not be null.");

		this.entityCallbacks = entityCallbacks;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.springframework.data.jdbc.core.JdbcAggregateOperations#save(java.lang.
	 * Object)
	 */
	@Override
	public <T> T save(T instance) {

		Assert.notNull(instance, "Aggregate instance must not be null!");

		RelationalPersistentEntity<?> persistentEntity = context.getRequiredPersistentEntity(instance.getClass());

		Function<T, MutableAggregateChange<T>> changeCreator = persistentEntity.isNew(instance)
				? this::createInsertChange
				: this::createUpdateChange;

		return store(instance, changeCreator, persistentEntity);
	}

	/**
	 * Dedicated insert function to do just the insert of an instance of an
	 * aggregate, including all the members of the aggregate.
	 *
	 * @param instance the aggregate root of the aggregate to be inserted. Must not
	 *                 be {@code null}.
	 * @return the saved instance.
	 */
	@Override
	public <T> T insert(T instance) {

		Assert.notNull(instance, "Aggregate instance must not be null!");

		RelationalPersistentEntity<?> persistentEntity = context.getRequiredPersistentEntity(instance.getClass());

		return store(instance, this::createInsertChange, persistentEntity);
	}

	/**
	 * Dedicated update function to do just an update of an instance of an
	 * aggregate, including all the members of the aggregate.
	 *
	 * @param instance the aggregate root of the aggregate to be inserted. Must not
	 *                 be {@code null}.
	 * @return the saved instance.
	 */
	@Override
	public <T> T update(T instance) {

		Assert.notNull(instance, "Aggregate instance must not be null!");

		RelationalPersistentEntity<?> persistentEntity = context.getRequiredPersistentEntity(instance.getClass());

		return store(instance, this::createUpdateChange, persistentEntity);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.springframework.data.jdbc.core.JdbcAggregateOperations#count(java.lang.
	 * Class)
	 */
	@Override
	public long count(Class<?> domainType) {

		Assert.notNull(domainType, "Domain type must not be null");

		return accessStrategy.count(domainType);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.springframework.data.jdbc.core.JdbcAggregateOperations#findById(java.lang
	 * .Object, java.lang.Class)
	 */
	@Override
	public <T> T findById(Object id, Class<T> domainType) {

		Assert.notNull(id, "Id must not be null!");
		Assert.notNull(domainType, "Domain type must not be null!");

		T entity = accessStrategy.findById(id, domainType);
		if (entity != null) {
			return triggerAfterLoad(entity);
		}
		return entity;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.springframework.data.jdbc.core.JdbcAggregateOperations#existsById(java.
	 * lang.Object, java.lang.Class)
	 */
	@Override
	public <T> boolean existsById(Object id, Class<T> domainType) {

		Assert.notNull(id, "Id must not be null!");
		Assert.notNull(domainType, "Domain type must not be null!");

		return accessStrategy.existsById(id, domainType);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.springframework.data.jdbc.core.JdbcAggregateOperations#findAll(java.lang.
	 * Class, org.springframework.data.domain.Sort)
	 */
	@Override
	public <T> Iterable<T> findAll(Class<T> domainType, Sort sort) {

		Assert.notNull(domainType, "Domain type must not be null!");

		Iterable<T> all = accessStrategy.findAll(domainType, sort);
		return triggerAfterLoad(all);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.springframework.data.jdbc.core.JdbcAggregateOperations#findAll(java.lang.
	 * Class, org.springframework.data.domain.Pageable)
	 */
	@Override
	public <T> Page<T> findAll(Class<T> domainType, Pageable pageable) {

		Assert.notNull(domainType, "Domain type must not be null!");

		Iterable<T> items = triggerAfterLoad(accessStrategy.findAll(domainType, pageable));
		long totalCount = accessStrategy.count(domainType);

		return new PageImpl<>(StreamSupport.stream(items.spliterator(), false).collect(Collectors.toList()), pageable,
				totalCount);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.springframework.data.jdbc.core.JdbcAggregateOperations#findAll(java.lang.
	 * Class)
	 */
	@Override
	public <T> Iterable<T> findAll(Class<T> domainType) {

		Assert.notNull(domainType, "Domain type must not be null!");

		Iterable<T> all = accessStrategy.findAll(domainType);
		return triggerAfterLoad(all);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.springframework.data.jdbc.core.JdbcAggregateOperations#findAllById(java.
	 * lang.Iterable, java.lang.Class)
	 */
	@Override
	public <T> Iterable<T> findAllById(Iterable<?> ids, Class<T> domainType) {

		Assert.notNull(ids, "Ids must not be null!");
		Assert.notNull(domainType, "Domain type must not be null!");

		Iterable<T> allById = accessStrategy.findAllById(ids, domainType);
		return triggerAfterLoad(allById);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.springframework.data.jdbc.core.JdbcAggregateOperations#delete(java.lang.
	 * Object, java.lang.Class)
	 */
	@Override
	public <S> void delete(S aggregateRoot, Class<S> domainType) {

		Assert.notNull(aggregateRoot, "Aggregate root must not be null!");
		Assert.notNull(domainType, "Domain type must not be null!");

		IdentifierAccessor identifierAccessor = context.getRequiredPersistentEntity(domainType)
				.getIdentifierAccessor(aggregateRoot);

		deleteTree(identifierAccessor.getRequiredIdentifier(), aggregateRoot, domainType);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.springframework.data.jdbc.core.JdbcAggregateOperations#deleteById(java.
	 * lang.Object, java.lang.Class)
	 */
	@Override
	public <S> void deleteById(Object id, Class<S> domainType) {

		Assert.notNull(id, "Id must not be null!");
		Assert.notNull(domainType, "Domain type must not be null!");

		deleteTree(id, null, domainType);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.springframework.data.jdbc.core.JdbcAggregateOperations#deleteAll(java.
	 * lang.Class)
	 */
	@Override
	public void deleteAll(Class<?> domainType) {

		Assert.notNull(domainType, "Domain type must not be null!");

		deleteAllManyToMany(domainType);

		MutableAggregateChange<?> change = createDeletingChange(domainType);
		executor.execute(change);
	}

	private <T> T store(T aggregateRoot, Function<T, MutableAggregateChange<T>> changeCreator,
			RelationalPersistentEntity<?> persistentEntity) {

		Assert.notNull(aggregateRoot, "Aggregate instance must not be null!");

		aggregateRoot = triggerBeforeConvert(aggregateRoot);

		MutableAggregateChange<T> change = changeCreator.apply(aggregateRoot);

		aggregateRoot = triggerBeforeSave(aggregateRoot, change);

		change.setEntity(aggregateRoot);

		T entityAfterExecution = executor.execute(change);

		Object identifier = persistentEntity.getIdentifierAccessor(entityAfterExecution).getIdentifier();

		Assert.notNull(identifier, "After saving the identifier must not be null!");

		return triggerAfterSave(entityAfterExecution, change);
	}

	private <T> void deleteTree(Object id, @Nullable T entity, Class<T> domainType) {

		MutableAggregateChange<T> change = createDeletingChange(id, entity, domainType);

		entity = triggerBeforeDelete(entity, id, change);
		change.setEntity(entity);

		deleteManyToMany(id, domainType);

		executor.execute(change);

		triggerAfterDelete(entity, id, change);
	}

	private <T> void deleteManyToMany(Object id, Class<T> domainType) {
		Field[] fields = domainType.getDeclaredFields();
		NamingStrategy namingStrategy = context.getNamingStrategy();

		for (Field field : fields) {
			ManyToMany mtm = AnnotatedElementUtils.findMergedAnnotation(field, ManyToMany.class);

			if (mtm != null) {
				String reference = mtm.table();
				String localColumn = mtm.column();
				String inverseColumn = mtm.inverseColumn();

				if (StringUtils.hasText(localColumn) && StringUtils.hasText(inverseColumn)
						&& StringUtils.hasText(reference)) {

					String lc = namingStrategy.getColumnName(localColumn);

					String deleteSql = String.format(manyToManyDeleteSqlFormat, reference, lc, lc);

					MapSqlParameterSource ps = new MapSqlParameterSource();
					ps.addValue(lc, id);

					operations.update(deleteSql, ps);
				}
			}
		}
	}

	private <T> void deleteAllManyToMany(Class<T> domainType) {
		Field[] fields = domainType.getDeclaredFields();
		NamingStrategy namingStrategy = context.getNamingStrategy();

		for (Field field : fields) {
			ManyToMany mtm = AnnotatedElementUtils.findMergedAnnotation(field, ManyToMany.class);

			if (mtm != null) {
				String reference = mtm.table();
				String localColumn = mtm.column();
				String inverseColumn = mtm.inverseColumn();

				if (StringUtils.hasText(localColumn) && StringUtils.hasText(inverseColumn)
						&& StringUtils.hasText(reference)) {

					String lc = namingStrategy.getColumnName(localColumn);

					String deleteSql = String.format(manyToManyDeleteAllSqlFormat, reference, lc, lc);

					MapSqlParameterSource ps = new MapSqlParameterSource();

					operations.update(deleteSql, ps);
				}
			}
		}
	}

	private <T> MutableAggregateChange<T> createInsertChange(T instance) {

		MutableAggregateChange<T> aggregateChange = MutableAggregateChange.forSave(instance);
		jdbcEntityInsertWriter.write(instance, aggregateChange);
		return aggregateChange;
	}

	private <T> MutableAggregateChange<T> createUpdateChange(T instance) {

		MutableAggregateChange<T> aggregateChange = MutableAggregateChange.forSave(instance);
		jdbcEntityUpdateWriter.write(instance, aggregateChange);
		return aggregateChange;
	}

	private <T> MutableAggregateChange<T> createDeletingChange(Object id, @Nullable T entity, Class<T> domainType) {

		MutableAggregateChange<T> aggregateChange = MutableAggregateChange.forDelete(domainType, entity);
		jdbcEntityDeleteWriter.write(id, aggregateChange);
		return aggregateChange;
	}

	private MutableAggregateChange<?> createDeletingChange(Class<?> domainType) {

		MutableAggregateChange<?> aggregateChange = MutableAggregateChange.forDelete(domainType, null);
		jdbcEntityDeleteWriter.write(null, aggregateChange);
		return aggregateChange;
	}

	private <T> Iterable<T> triggerAfterLoad(Iterable<T> all) {

		List<T> result = new ArrayList<>();

		for (T e : all) {
			result.add(triggerAfterLoad(e));
		}

		return result;
	}

	private <T> T triggerAfterLoad(T entity) {

		publisher.publishEvent(new AfterLoadEvent<>(entity));

		return entityCallbacks.callback(AfterLoadCallback.class, entity);
	}

	private <T> T triggerBeforeConvert(T aggregateRoot) {
		return entityCallbacks.callback(BeforeConvertCallback.class, aggregateRoot);
	}

	private <T> T triggerBeforeSave(T aggregateRoot, AggregateChange<T> change) {

		publisher.publishEvent(new BeforeSaveEvent<>(aggregateRoot, change));

		return entityCallbacks.callback(BeforeSaveCallback.class, aggregateRoot, change);
	}

	private <T> T triggerAfterSave(T aggregateRoot, AggregateChange<T> change) {

		publisher.publishEvent(new AfterSaveEvent<>(aggregateRoot, change));

		return entityCallbacks.callback(AfterSaveCallback.class, aggregateRoot);
	}

	private <T> void triggerAfterDelete(@Nullable T aggregateRoot, Object id, MutableAggregateChange<T> change) {

		publisher.publishEvent(new AfterDeleteEvent<>(Identifier.of(id), aggregateRoot, change));

		if (aggregateRoot != null) {
			entityCallbacks.callback(AfterDeleteCallback.class, aggregateRoot);
		}
	}

	@Nullable
	private <T> T triggerBeforeDelete(@Nullable T aggregateRoot, Object id, MutableAggregateChange<T> change) {

		publisher.publishEvent(new BeforeDeleteEvent<>(Identifier.of(id), aggregateRoot, change));

		if (aggregateRoot != null) {
			return entityCallbacks.callback(BeforeDeleteCallback.class, aggregateRoot, change);
		}

		return null;
	}

	public NamedParameterJdbcOperations getOperations() {
		return operations;
	}

	public JdbcAggregateTemplate setOperations(NamedParameterJdbcOperations operations) {
		this.operations = operations;
		return this;
	}

}
