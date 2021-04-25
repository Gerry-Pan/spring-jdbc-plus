package org.springframework.data.relational.core.mapping;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.data.annotation.Transient;

/**
 * 只为Criteria查询使用，不做数据抓取
 * 
 * @author Jerry
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD })
@Documented
@Inherited
@Transient
public @interface ManyToOne {

	/**
	 * Java Entity中的屬性
	 * 
	 * @return
	 */
	String property();

}
