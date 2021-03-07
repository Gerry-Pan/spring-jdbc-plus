package org.springframework.data.relational.core.sql;

import org.springframework.util.Assert;

public class ReferenceColumn extends Column {

	ReferenceColumn(String name, Table table) {
		super(name, table);
	}

	ReferenceColumn(SqlIdentifier name, Table table) {
		super(name, table);
	}

	public static ReferenceColumn create(String name, Table table) {

		Assert.hasText(name, "Name must not be null or empty");
		Assert.notNull(table, "Table must not be null");

		return new ReferenceColumn(SqlIdentifier.unquoted(name), table);
	}

	public static ReferenceColumn create(SqlIdentifier name, Table table) {

		Assert.notNull(name, "Name must not be null");
		Assert.notNull(table, "Table must not be null");

		return new ReferenceColumn(name, table);
	}

	@Override
	public SqlIdentifier getReferenceName() {
		return SqlIdentifier.unquoted(super.toString());
	}

}
