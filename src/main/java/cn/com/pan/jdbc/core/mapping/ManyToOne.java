package cn.com.pan.jdbc.core.mapping;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.data.annotation.Transient;

@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD, ElementType.METHOD })
@Documented
@Inherited
@Transient
public @interface ManyToOne {

	/**
	 * Database Table中的欄位
	 * 
	 * @return
	 */
	String column();

	/**
	 * Java Entity中的屬性
	 * 
	 * @return
	 */
	String property();

}
