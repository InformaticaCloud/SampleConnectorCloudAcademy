package com.informatica.connector.wrapper.util;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;




	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target({java.lang.annotation.ElementType.FIELD})
	public @interface ConnectionProperty
	{
	  public String label();
	  public ConnectionPropertyType type() default ConnectionPropertyType.JAVATYPE;
	  public boolean required() default false;
	  public String[] listValues() default {}; 	  
	}