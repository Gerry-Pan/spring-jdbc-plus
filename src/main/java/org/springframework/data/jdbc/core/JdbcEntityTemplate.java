package org.springframework.data.jdbc.core;

import org.springframework.context.ApplicationContext;
import org.springframework.data.jdbc.core.convert.DataAccessStrategySupport;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;

/**
 * Named like org.springframework.data.r2dbc.core.R2dbcEntityTemplate
 * 
 * @author Jerry Pan, NJUST
 *
 */
public class JdbcEntityTemplate extends JdbcAggregatePlusTemplate {

	public JdbcEntityTemplate(ApplicationContext publisher, RelationalMappingContext context, JdbcConverter converter,
			DataAccessStrategySupport dataAccessStrategy) {
		super(publisher, context, converter, dataAccessStrategy);
	}

}
