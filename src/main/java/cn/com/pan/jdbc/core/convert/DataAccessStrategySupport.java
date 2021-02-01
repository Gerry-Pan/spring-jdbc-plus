package cn.com.pan.jdbc.core.convert;

import java.util.Map;

import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import cn.com.pan.jdbc.core.PreparedOperation;
import cn.com.pan.jdbc.core.StatementMapper;

public interface DataAccessStrategySupport extends DataAccessStrategy {

	public long count(PreparedOperation<?> operation, Class<?> domainType);

	public <T> Iterable<T> findAll(PreparedOperation<?> operation, Class<T> domainType);

	public int update(String sql, Map<String, ?> paramMap);

	public int update(String sql, SqlParameterSource paramSource);

	public int[] batchUpdate(String sql, Map<String, ?>[] batchValues);

	public int[] batchUpdate(String sql, SqlParameterSource[] batchArgs);

	public StatementMapper getStatementMapper();

}
