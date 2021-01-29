package cn.com.pan.jdbc.core.convert;

import org.springframework.data.jdbc.core.convert.DataAccessStrategy;

import cn.com.pan.jdbc.core.PreparedOperation;
import cn.com.pan.jdbc.core.StatementMapper;

public interface DataAccessStrategySupport extends DataAccessStrategy {

	public <T> Iterable<T> findAll(PreparedOperation<?> operation, Class<T> domainType);

	public StatementMapper getStatementMapper();

}
