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
@Inherited
@Transient
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD, ElementType.METHOD })
public @interface ManyToMany {

	/**
	 * 当前实体类的数据表在中间表中的关联字段
	 * 
	 * @return
	 */
	String column() default "";

	/**
	 * 属性实体类的数据表在中间表中的关联字段
	 * 
	 * @return
	 */
	String inverseColumn() default "";

	/**
	 * 中间表
	 * 
	 * @return
	 */
	String table() default "";

	String mappedBy() default "";

}
