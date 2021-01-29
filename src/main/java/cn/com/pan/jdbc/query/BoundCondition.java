package cn.com.pan.jdbc.query;

import org.springframework.data.relational.core.sql.Condition;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.util.Assert;

public class BoundCondition {

	private final Condition condition;

	private final SqlParameterSource sqlParameterSource;

	public BoundCondition(SqlParameterSource sqlParameterSource, Condition condition) {
		Assert.notNull(sqlParameterSource, "SqlParameterSource must not be null!");
		Assert.notNull(condition, "Condition must not be null!");

		this.condition = condition;
		this.sqlParameterSource = sqlParameterSource;
	}

	public Condition getCondition() {
		return condition;
	}

	public SqlParameterSource getSqlParameterSource() {
		return sqlParameterSource;
	}

}
