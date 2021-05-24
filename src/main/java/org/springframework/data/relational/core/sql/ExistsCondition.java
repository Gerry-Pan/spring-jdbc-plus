package org.springframework.data.relational.core.sql;

public class ExistsCondition extends AbstractSegment implements Condition {

	private String subQuery;

	public ExistsCondition(String subQuery) {
		this.subQuery = subQuery;
	}

	@Override
	public String toString() {
		return "EXISTS (" + subQuery + ")";
	}

}
