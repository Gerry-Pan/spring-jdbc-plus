package org.springframework.data.jdbc.repository.query;

import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.query.ExistsCriteria;
import org.springframework.data.relational.core.sql.Condition;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

@FunctionalInterface
public interface ExistsCallback {

	/**
	 * 
	 * @param masterEntity
	 * @param existsCriteria
	 * @param sqlParameterSource
	 * @param atomicInteger
	 * @return
	 */
	public Condition resolve(RelationalPersistentEntity<?> masterEntity, ExistsCriteria existsCriteria, MapSqlParameterSource sqlParameterSource,
			AtomicInteger atomicInteger);

}
