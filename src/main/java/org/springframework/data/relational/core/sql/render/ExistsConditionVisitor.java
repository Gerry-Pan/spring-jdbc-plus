package org.springframework.data.relational.core.sql.render;

import org.springframework.data.relational.core.sql.Condition;
import org.springframework.data.relational.core.sql.ExistsCondition;

public class ExistsConditionVisitor extends ConditionVisitor {

	private ExistsCondition condition;
	private final RenderTarget target;

	public ExistsConditionVisitor(RenderContext context, RenderTarget target) {
		super(context);
		this.target = target;
	}

	@Override
	Delegation enterMatched(Condition segment) {
		if (segment instanceof ExistsCondition) {
			condition = (ExistsCondition) segment;
			target.onRendered(condition.toString());
		}
		return Delegation.retain();
	}

}
