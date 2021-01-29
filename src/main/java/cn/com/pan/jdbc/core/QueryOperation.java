package cn.com.pan.jdbc.core;

import java.util.function.Supplier;

@FunctionalInterface
public interface QueryOperation extends Supplier<String> {

	String toQuery();

	@Override
	default String get() {
		return toQuery();
	}

}
