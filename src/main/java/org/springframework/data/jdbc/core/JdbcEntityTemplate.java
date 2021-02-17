package org.springframework.data.jdbc.core;

import org.springframework.context.ApplicationContext;
import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;

/**
 * Named like org.springframework.data.r2dbc.core.R2dbcEntityTemplate
 * 
 * @author Jerry Pan, NJUST
 *
 */
public class JdbcEntityTemplate extends JdbcAggregatePlusTemplate {

	public JdbcEntityTemplate(ApplicationContext publisher, RelationalMappingContext context, JdbcConverter converter,
			DataAccessStrategy dataAccessStrategy, Dialect dialect) {
		super(publisher, context, converter, dataAccessStrategy, dialect);
	}

	public JdbcEntityTemplate setOperations(NamedParameterJdbcOperations operations) {
		super.setOperations(operations);
		return this;
	}

}
