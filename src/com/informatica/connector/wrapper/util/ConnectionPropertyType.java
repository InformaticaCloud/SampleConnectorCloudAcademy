package com.informatica.connector.wrapper.util;

public enum ConnectionPropertyType {
		/**
		 *  JAVATYPE: Use the java Type of the field
		 *  This should work for long,boolean,String 
		 *  for any other type use LIST, PASSWORD, ALPHABET
		 */
		JAVATYPE,
		/**
		 *  LIST: Use if the ConnectionAttribute is of type java.util.List
		 */
		LIST,
		/**
		 *  ALPHA: Use if the ConnectionAttribute can only contain alphabets [a-z,A-Z]
		 */
		ALPHA,
		/**
		 *  ALPHANUMERIC: Use if the ConnectionAttribute can only contain alphabets and numbers
		 */
		ALPHANUMERIC,
		/**
		 *  PASSWORD: Use if the ConnectionAttribute is a password field
		 */
		PASSWORD		
	}