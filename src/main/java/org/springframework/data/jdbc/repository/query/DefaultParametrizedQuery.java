package org.springframework.data.jdbc.repository.query;

import org.springframework.jdbc.core.namedparam.SqlParameterSource;

/**
 * change ParametrizedQuery to public
 * 
 * @author Jerry
 *
 */
public class DefaultParametrizedQuery extends ParametrizedQuery {

	public DefaultParametrizedQuery(String query, SqlParameterSource parameterSource) {
		super(query, parameterSource);
	}

	@Override
	public String getQuery() {
		return super.getQuery();
	}

	@Override
	public SqlParameterSource getParameterSource() {
		return super.getParameterSource();
	}

}
