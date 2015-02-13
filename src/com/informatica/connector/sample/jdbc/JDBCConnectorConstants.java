package com.informatica.connector.sample.jdbc;

import java.math.BigDecimal;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.informatica.cloud.api.adapter.runtime.exception.DataConversionException;
import com.informatica.cloud.api.adapter.typesystem.JavaDataType;
import com.informatica.connector.wrapper.util.ConnectorUtils;


public class JDBCConnectorConstants {

	//TODO - 1.1 INFA: generate UUID using the GenerateUUIDTest class and paste it here
	public static final String PLUGIN_UUID = "4e6a729f-e13b-449a-ad17-38069b635f52";

//TODO - 1.2 INFA: Define alphanumeric replacement string for special characters 
//this is simple map between special characters and the corresponding replacement strings 
	private static final String[][] specialCharacterReplacementMap = {{" ","__bl__"}, {"?","__qu__"}, {":","__co__"}, {".","__do__"},{"-","__hi__"}};	
	
	public static String sanitizeName(String var)
	{		
		for(int i=0;i<JDBCConnectorConstants.specialCharacterReplacementMap.length;i++)
		{
			var = var.replaceAll(Pattern.quote(JDBCConnectorConstants.specialCharacterReplacementMap[i][0]), Matcher.quoteReplacement(JDBCConnectorConstants.specialCharacterReplacementMap[i][1]));
		}
		return var;
	}

	public static String unsanitizeName(String var)
	{		
		for(int i=0;i<JDBCConnectorConstants.specialCharacterReplacementMap.length;i++)
		{
			var = var.replaceAll(Pattern.quote(JDBCConnectorConstants.specialCharacterReplacementMap[i][1]), Matcher.quoteReplacement(JDBCConnectorConstants.specialCharacterReplacementMap[i][0]));
		}
		return var;
	}
	
	
	// TODO - 3.2.1 INFA: Handle proprietary types
	/**
	 * the Following java types are supported in Informatica cloud Boolean,
	 * Byte[], Short, Integer, Long, Float, Double, String, java.sql.Timestamp,
	 * java.math.BigDecimal, java.math.BigInteger
	 */
	public static Class<?> getJavaClassFromSqlType(int sqlType, String dbTypeName) throws ClassNotFoundException 
	{		
			JavaDataType jdt = null;
			switch (sqlType) {
			case java.sql.Types.ARRAY:
				jdt = JavaDataType.JAVA_STRING;
				break;
			case java.sql.Types.BIGINT:
				jdt = JavaDataType.JAVA_LONG;
				break;
			case java.sql.Types.BINARY:
				jdt = JavaDataType.JAVA_PRIMITIVE_BYTEARRAY;
				break;
			case java.sql.Types.BIT:
				jdt = JavaDataType.JAVA_BOOLEAN;
				break;
			case java.sql.Types.BLOB:
				jdt = JavaDataType.JAVA_PRIMITIVE_BYTEARRAY;
				break;
			case java.sql.Types.BOOLEAN:
				jdt = JavaDataType.JAVA_BOOLEAN;
				break;
			case java.sql.Types.CHAR:
				jdt = JavaDataType.JAVA_STRING;
				break;
			case java.sql.Types.CLOB:
				jdt = JavaDataType.JAVA_STRING;
				break;
			case java.sql.Types.DATALINK:
				jdt = JavaDataType.JAVA_STRING;
				break;
			case java.sql.Types.DATE:
				jdt = JavaDataType.JAVA_TIMESTAMP;
				break;
			case java.sql.Types.DECIMAL:
				jdt = JavaDataType.JAVA_BIGDECIMAL;
				break;
			case java.sql.Types.DISTINCT:
				jdt = JavaDataType.JAVA_STRING;
				break;
			case java.sql.Types.DOUBLE:
				jdt = JavaDataType.JAVA_DOUBLE;
				break;
			case java.sql.Types.FLOAT:
				jdt = JavaDataType.JAVA_DOUBLE;
				break;
			case java.sql.Types.INTEGER:
				jdt = JavaDataType.JAVA_INTEGER;
				break;
			case java.sql.Types.JAVA_OBJECT:
				jdt = JavaDataType.JAVA_PRIMITIVE_BYTEARRAY;
				break;
			case java.sql.Types.LONGNVARCHAR:
				jdt = JavaDataType.JAVA_STRING;
				break;
			case java.sql.Types.LONGVARBINARY:
				jdt = JavaDataType.JAVA_PRIMITIVE_BYTEARRAY;
				break;
			case java.sql.Types.LONGVARCHAR:
				jdt = JavaDataType.JAVA_STRING;
				break;
			case java.sql.Types.NCHAR:
				jdt = JavaDataType.JAVA_STRING;
				break;
			case java.sql.Types.NCLOB:
				jdt = JavaDataType.JAVA_STRING;
				break;
			case java.sql.Types.NUMERIC:
				jdt = JavaDataType.JAVA_BIGDECIMAL;
				break;
			case java.sql.Types.NVARCHAR:
				jdt = JavaDataType.JAVA_STRING;
				break;
			case java.sql.Types.REAL:
				jdt = JavaDataType.JAVA_FLOAT;
				break;
			case java.sql.Types.REF:
				jdt = JavaDataType.JAVA_STRING;
				break;
			case java.sql.Types.ROWID:
				jdt = JavaDataType.JAVA_STRING;
				break;
			case java.sql.Types.SMALLINT:
				jdt = JavaDataType.JAVA_INTEGER;
				break;
			case java.sql.Types.SQLXML:
				jdt = JavaDataType.JAVA_STRING;
				break;
			case java.sql.Types.STRUCT:
				jdt = JavaDataType.JAVA_STRING;
				break;
			case java.sql.Types.TIME:
				jdt = JavaDataType.JAVA_TIMESTAMP;
				break;
			case java.sql.Types.TIMESTAMP:
				jdt = JavaDataType.JAVA_TIMESTAMP;
				break;
			case java.sql.Types.TINYINT:
				jdt = JavaDataType.JAVA_INTEGER;
				break;
			case java.sql.Types.VARBINARY:
				jdt = JavaDataType.JAVA_PRIMITIVE_BYTEARRAY;
				break;
			case java.sql.Types.VARCHAR:
				jdt = JavaDataType.JAVA_STRING;
				break;
			case java.sql.Types.OTHER:
				//TODO handle other types as well
				if (dbTypeName.equalsIgnoreCase("NCHAR") || dbTypeName.equalsIgnoreCase("NVARCHAR")
						|| dbTypeName.equalsIgnoreCase("NVARCHAR2") || dbTypeName.equalsIgnoreCase("NCLOB")) {
					jdt = JavaDataType.JAVA_STRING;
				}else
					jdt = JavaDataType.JAVA_PRIMITIVE_BYTEARRAY;
				break;
			default:
				jdt = JavaDataType.JAVA_PRIMITIVE_BYTEARRAY;			
				break;
			}
			return ConnectorUtils.classForJavaDataType(jdt);
		}	
		
	// TODO - 3.2.2 INFA: Handle proprietary Object values
	/**
	 * the Following java types are supported in Informatica cloud Boolean,
	 * Byte[], Short, Integer, Long, Float, Double, String, java.sql.Timestamp,
	 * java.math.BigDecimal, java.math.BigInteger
	 * @throws DataConversionException 
	 */
	public static Object toInfaJavaDataType(Object value, JavaDataType jdt) throws DataConversionException {
		
		if(value==null)
			return null;
		
		/*
		// Example Handling Oracle proprietary types
		if(value.getClass().getName().startsWith("oracle"))
		{
			try
			{
				Method toJdbcMethod = value.getClass().getMethod("toJdbc");
				value = toJdbcMethod.invoke(value);
			}catch(Throwable t)
			{
				t.printStackTrace();
				throw new DataConversionException(t.toString())
			}
		}
		 */		

		//This will take care of basic type conversion String to Number, boolean etc
		return ConnectorUtils.toInfaJavaDataType(value, jdt);
	}
	
	/**
	 * @param infaClazz
	 * @param precision
	 * @param maxPrecision
	 * @param scale
	 * @return
	 */
	public static int getAdjustedPrecision(Class<?> infaClazz,int precision, int maxPrecision, int scale) 
	{		
		int ret = precision;		
		if(!infaClazz.getCanonicalName().equals(String.class.getCanonicalName()) 
				&& !infaClazz.getCanonicalName().equals(byte[].class.getCanonicalName())
				&&	!infaClazz.getCanonicalName().equals(BigDecimal.class.getCanonicalName()))
		{
			//To use defaults for all other types set to -1
			ret = -1;
			return ret;
		}	
			
		if(BigDecimal.class.isAssignableFrom(infaClazz))
		{
			if(ret == 0 && scale <= 0)
				ret = 38;
			
			if(ret < 0 || ret > 38)
			{
				ret = 38;				
			}
		}else		
		if(ret <= 0 || ret > maxPrecision)
		{
			ret = maxPrecision;				
		}
		
		return ret;
	}

	/**
	 * @param infaClazz
	 * @param precision
	 * @param scale
	 * @return
	 */
	public static int getAdjustedScale(Class<?> infaClazz, int precision, int scale) 
	{		
		int ret = scale;		
		if(!infaClazz.getCanonicalName().equals(BigDecimal.class.getCanonicalName()))
		//		&& !infaClazz.getCanonicalName().equals(Double.class.getCanonicalName())
		//		&& !infaClazz.getCanonicalName().equals(Float.class.getCanonicalName()))
		{
			//To use defaults for all other types set to -1
			ret = -1;
		}		

		if(ret>precision)
			ret = precision;
		
		if(ret<0)
			ret = 0;
		
		return ret;
	}
	

}
