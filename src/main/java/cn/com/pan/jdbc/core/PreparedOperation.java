package cn.com.pan.jdbc.core;

import org.springframework.jdbc.core.namedparam.SqlParameterSource;

public interface PreparedOperation<T> extends QueryOperation {

	T getSource();

	SqlParameterSource getSqlParameterSource();
	
}
