package com.informatica.connector.wrapper.util;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.Reader;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Date;
import java.sql.RowId;
import java.util.Calendar;
import java.util.Enumeration;
import java.util.Properties;
import java.util.UUID;

import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.beanutils.MethodUtils;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.ClassUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.informatica.cloud.api.adapter.connection.InsufficientConnectInfoException;
import com.informatica.cloud.api.adapter.runtime.exception.DataConversionException;
import com.informatica.cloud.api.adapter.typesystem.JavaDataType;

/**
 * @author pugupta
 *
 */
public class ConnectorUtils {
	
	public static final int DEFAULT_BUFFER_SIZE = 1024 * 4;
	public static final int EOF = -1;
	
	int PC_MAX_LENGTH = 104857600;
	int PC_MAX_SCALE = 65535;
	
	public static final int MAX_PRECISION_TEXT 	= 104857600;
	public static final int MAX_PRECISION_DATE 	= 29;
	public static final int MAX_SCALE_DATE 		= 9;
	public static final int MAX_PRECISION_INTEGER 	= 10;
	public static final int MAX_PRECISION_BIGINT 	= 19;
	public static final int MAX_PRECISION_SMALLINT = 5;
	public static final int MAX_PRECISION_DOUBLE 	= 15;
	public static final int MAX_PRECISION_REAL 	= 7;

	public static final char LF = '\n';

	public static final char CR = '\r';

	public static final char QUOTE = '"';

	public static final char COMMA = ',';
	

	/**
	 * This method will return the Enum JavaDataType for the input class
	 * 
	 * @param clazz
	 *            The input class for which the JavaDataType should be created
	 * @return The javaDataType that corresponds to the input class
	 * @throws IllegalArgumentException
	 */
	public static JavaDataType getJavaDataType(Class<?> clazz) throws IllegalArgumentException
	{
		if(clazz == null)
			throw  new IllegalArgumentException("Input class is null");
		
		clazz= getAdjustedDataType(clazz);
		
		try
		{
			if (clazz.getCanonicalName().startsWith("java.lang.Byte"))
		      return JavaDataType.JAVA_PRIMITIVE_BYTEARRAY;
		    
		    if(clazz.getCanonicalName().equals(byte[].class.getCanonicalName()))
		    	return JavaDataType.JAVA_PRIMITIVE_BYTEARRAY;

		    //if(clazz.getCanonicalName().equals(Byte[].class.getCanonicalName()))
		    //	return JavaDataType.JAVA_PRIMITIVE_BYTEARRAY;
		    
		    for (JavaDataType localJavaDataType : JavaDataType.values())
		    {
		      if (localJavaDataType.getFullClassName().equals(clazz.getCanonicalName()))
		      {
		        return localJavaDataType;
		      }
		      else 
		      {
		    	  if(classForJavaDataType(localJavaDataType).isAssignableFrom(clazz))
		    	  return localJavaDataType;
		      }
		    }
		}catch(Throwable t)
		{
			//logger.error("Failed to create JavaDataType for class {"+clazz.getName()+"}",t);
			throw  new IllegalArgumentException(clazz == null ? "fieldType is null": clazz.getName());
		}
		//logger.error("Failed to create JavaDataType for class {"+clazz.getName()+"}");
		throw  new IllegalArgumentException(clazz == null ? "fieldType is null": clazz.getName());
	}				

	/**
	 * Only the Following java types are supported in Informatica cloud:
	 * Boolean, byte[], Short, Integer, Long, Float, Double, String,
	 * java.sql.Timestamp, java.math.BigDecimal, java.math.BigInteger
	 * 
	 * The method will try and convert some basic types into a type from
	 * the above list. For example if the Class is Calendar, the class returned
	 * is java.sql.Timestamp
	 * 
	 * @param clazz
	 * @return
	 */
	public static Class<?> getAdjustedDataType(Class<?> clazz) {

		if(Date.class.isAssignableFrom(clazz))
			return java.sql.Timestamp.class;

		if(Calendar.class.isAssignableFrom(clazz))
			return java.sql.Timestamp.class;

		if (clazz.getName().equals("java.util.Date"))
			return java.sql.Timestamp.class;

		if (clazz.getName().equals("javax.xml.datatype.XMLGregorianCalendar"))
			return java.sql.Timestamp.class;

		// special case for handling XMLGregorianCalendar data type
		if (clazz.getName().equals("java.lang.Character"))
			return String.class;
		
		// special case for handling Time data type
		if (clazz.getName().equals("java.sql.Time"))
			return java.sql.Timestamp.class;

		if (clazz.getName().equals("java.sql.Date"))
			return java.sql.Timestamp.class;
		
		if(clazz.getName().equalsIgnoreCase("java.sql.Blob"))
			return byte[].class;

		if(clazz.getName().equalsIgnoreCase("java.sql.Clob"))
			return String.class;

		// non-wrapped primitive data type	
		return ClassUtils.primitiveToWrapper(clazz);
	}

	/**
	 * @deprecated, replaced by
	 *              {@link #toInfaJavaDataType(Object value,JavaDataType jdt)}
	 * @param value
	 *            The value to convert to Infa basic types
	 * @param classCanonicalName
	 *            The class of the Infa basic type to convert the value to
	 * @return Value that is one of type in JavaDataType
	 * @throws DataConversionException
	 */
	public static Object getAdjustedValue(Object value, String classCanonicalName) throws DataConversionException 
	{		
		if(value == null)
			return null;
		
		if(classCanonicalName==null)
			throw new DataConversionException("classCanonicalName cannot be null");
				
		try {
			return toInfaJavaDataType(value, getJavaDataType(classForJavaDataTypeFullClassName(classCanonicalName)));
		} catch (ClassNotFoundException e) {
			throw new DataConversionException(e.toString());
		}				
	}
	
			
		  public static String getClassLoaderClassPath(URLClassLoader loader) 
		  {			  
				StringBuffer strBuf = new StringBuffer();
				if(loader!=null)
				{
			        URL[] urls = loader.getURLs();
			        boolean first = true;
			        for(URL url: urls){
			        	if(!first)
			        		strBuf.append(";");
			        	strBuf.append(url.getFile());
			        	first = false;
			        }
				}
				return strBuf.toString();
		  }
		  
		 
		  public synchronized static boolean configureLog4j() 
		    {
		    	try
		    	{
			    	if (!isLog4jConfigured()) 
			        {
			            BasicConfigurator.configure();
			            Logger log = LogManager.getRootLogger();
			            if(log!=null)
			            	log.setLevel(Level.INFO);
				        else 
				        {
				            Enumeration<?> loggers = LogManager.getCurrentLoggers() ;
				            while (loggers.hasMoreElements()) {
				                Logger c = (Logger) loggers.nextElement();
				            	c.setLevel(Level.INFO);
				            }
				        }
			        }
				}catch(Throwable t){
					t.printStackTrace();
				}    
		    	return true;
		    }

		    public synchronized static void shutdownLog4j() {
		        if (isLog4jConfigured()) {
		            LogManager.shutdown();
		        }
		    }
		    
	    private static boolean isLog4jConfigured() {
	    	try
	    	{
		        Enumeration<?> appenders =  LogManager.getRootLogger().getAllAppenders();
		        if (appenders.hasMoreElements()) {
		            return true;
		        }
		        else {
		            Enumeration<?> loggers = LogManager.getCurrentLoggers() ;
		            while (loggers.hasMoreElements()) {
		                Logger c = (Logger) loggers.nextElement();
		                if (c.getAllAppenders().hasMoreElements())
		                    return true;
		            }
		        }
	    	}catch(Throwable t){
	    		t.printStackTrace();
	    	}    
	        return false;
	    }				  
		  
	    /**
	     * Check if string is only numeric and digits
	     * @param stringValue
	     * @return true if string is only letters or digit
	     */
	    public static boolean isLetterOrDigit(String stringValue) {
		 for (char c : stringValue.toCharArray()) {
		      if (!Character.isLetterOrDigit(c))
		        return false;
		    }
		  return true;
	    }
	    
	    
	    /**
	     * Converts int to byte[]
	     * @param i
	     * @return
	     */
	    public static byte[] toBytes(int i)
	    {
	      byte[] result = new byte[4];

	      result[0] = (byte) (i >> 24);
	      result[1] = (byte) (i >> 16);
	      result[2] = (byte) (i >> 8);
	      result[3] = (byte) (i /*>> 0*/);

	      return result;
	    }

	/**
	 * Load properties from a property file
	 * 
	 * @param propFile
	 * @return
	 * @throws FileNotFoundException
	 */
		public static Properties loadProperties(File propFile) throws FileNotFoundException 
		{
			Properties props = new Properties();
	    	try{
	    		props.load(new FileInputStream(propFile));	    		
	    	} catch (IOException ex) {
	    		throw new FileNotFoundException("Unable to load propertiess file: "+propFile);
	    	}
			return props;
		}

	    
	/**
	 * Load properties from a file in the classpath
	 * 
	 * @param propFileName
	 * @return
	 * @throws FileNotFoundException
	 */
		public static Properties loadProperties(String propFileName) throws FileNotFoundException 
		{
			Properties props = new Properties();
			ClassLoader loader = ConnectorUtils.class.getClassLoader();
	        if (loader == null) {
		    	 loader = Thread.currentThread().getContextClassLoader();
	        }
	    	try{
	    		InputStream propStream = loader.getResourceAsStream(propFileName);
	    		if(propStream!=null)
	    			props.load(propStream);
	    		else
		    		throw new FileNotFoundException("Unable to load propertiess file: " +propFileName) ;	    			
	    	} catch (IOException ex) {
	    		throw new FileNotFoundException("Unable to load propertiess file: "+propFileName) ;
	    	}
			return props;
		}
		
	/**
	 * Saves properties to a file
	 * 
	 * @param props
	 * @param fileName
	 * @throws IOException
	 */
		public static void saveProp(Properties props,String fileName)
				throws IOException {
			FileWriter fw = new FileWriter(fileName);
			props.store(fw, null);
			fw.flush();
			fw.close();
		}

		/*
		public synchronized static boolean configureJdk14Logger(Level newLevel)
		{	
			try
			{
				//get the top Logger:
			    Logger topLogger = java.util.logging.Logger.getLogger("");
	
			    // Handler for console (reuse it if it already exists)
			    Handler consoleHandler = null;
			    //see if there is already a console handler
			    for (Handler handler : topLogger.getHandlers()) {
			        if (handler instanceof ConsoleHandler) {
			            //found the console handler
			            consoleHandler = handler;
			            handler.setLevel(newLevel);
			            Formatter newFormatter = null;
						handler.setFormatter(newFormatter);
			        }
			    }
	
	
			    if (consoleHandler == null) {
			        //there was no console handler found, create a new one
			        consoleHandler = new ConsoleHandler();
			        topLogger.addHandler(consoleHandler);
			    }
			    //set the console handler to SEVERE:
			    consoleHandler.setLevel(newLevel);
				return true;		
			}catch(Throwable t)
			{
				t.printStackTrace();
				return false;
			}
		}
		 */

		/**
		 * Converts input value to desired  java types
		 * @param value The input to be converted
		 * @param jdt The JavaDataType to convert the value to
		 * @return the converted value
		 * @throws DataConversionException
		 */
		public  static Object toInfaJavaDataType(Object value,JavaDataType jdt) throws DataConversionException 
		{
			if(value==null)
				return value;
			
			if(jdt.getFullClassName().startsWith("java.lang.Byte"))
				jdt = JavaDataType.JAVA_PRIMITIVE_BYTEARRAY;
			
			if((value instanceof byte[]) && (jdt == JavaDataType.JAVA_PRIMITIVE_BYTEARRAY))
				return value;
									
			if(value.getClass().getCanonicalName().equals(jdt.getFullClassName()))
				return value;
			
			if(value instanceof InputStream)
			{
				try {
					value = toBytes((InputStream)value);
				} catch (IOException e) {
					e.printStackTrace();
					throw new DataConversionException(e.toString());
				}
			}else if(value instanceof Reader)
			{
				value = toString((Reader) value);
			}
			if(value instanceof UUID)
			{
				value = ((UUID)value).toString();
			}else 
			if(value instanceof RowId)
			{
				value = ((RowId)value).toString();		
			}

			if(value==null)
				return value;
			

			switch (jdt) {			
			case JAVA_PRIMITIVE_BYTEARRAY:
				if(value instanceof String)
				{
					return value.toString().getBytes();					
				}else if(MethodUtils.getMatchingAccessibleMethod(value.getClass(), "toBytes", new Class[0]) != null)
				{
					try {
						return MethodUtils.invokeMethod(value, "toBytes", new Class[0]);
					} catch (Throwable t) {
						t.printStackTrace();
						throw new DataConversionException(t.toString());
					}					
				}else if(MethodUtils.getMatchingAccessibleMethod(value.getClass(), "getBytes", new Class[0]) != null)
				{
					try {
						return MethodUtils.invokeMethod(value, "getBytes", new Class[0]);
					} catch (Throwable t) {
						t.printStackTrace();
						throw new DataConversionException(t.toString());
					}					
				}else if(value instanceof Serializable)
				{
					try {
						return serialize(value);
					} catch (IOException e) {
						e.printStackTrace();
						throw new DataConversionException(e.toString());
					}
				}else
				{
					return value.toString().getBytes();
				}
			case JAVA_STRING:
				if(value instanceof String)
				{
					return value.toString();
				}else if(value instanceof byte[])
				{
					return Base64.encodeBase64String((byte[])value);
				}else if(MethodUtils.getMatchingAccessibleMethod(value.getClass(), "stringValue", new Class[0]) != null)
				{
					try {
						return MethodUtils.invokeExactMethod(value, "stringValue", null,null);
					} catch (Throwable t) {
						t.printStackTrace();
						throw new DataConversionException(t.toString());
					}
				}else
				{
						String s = value.toString();
						if(s.endsWith(Integer.toHexString(value.hashCode())))
						{
							//TODO Find other methods to get string from object
							if(value instanceof Serializable)
							{
								try {
									return Base64.encodeBase64String(serialize(value));
								} catch (IOException e) {
									e.printStackTrace();
									throw new DataConversionException(e.toString());
								}
							}
						}
					return s;
				}
			case JAVA_TIMESTAMP:
				if (value instanceof String) {
					try {
						return java.sql.Timestamp.valueOf(value.toString());
					} catch (Throwable t) {
						// We are assuming the date is in UTC if date string does
						// not contain a timezone in this format
						// 2012-05-15T20:31:03.000Z
						try {
							DatatypeFactory df = DatatypeFactory.newInstance();
							XMLGregorianCalendar gc = df
									.newXMLGregorianCalendar(value.toString());
							if (gc.getTimezone() == javax.xml.datatype.DatatypeConstants.FIELD_UNDEFINED)
								gc.setTimezone(0); // UTC
							return new java.sql.Timestamp(gc.toGregorianCalendar(
									null, null, null).getTimeInMillis());
						} catch (Throwable e) {
							t.printStackTrace();
							throw new DataConversionException("Uparsable timestamp:"
									+ value.toString()+": "+e.toString());
						}
					}
				}
				return ConvertUtils.convert(value, java.sql.Timestamp.class);
			case JAVA_BOOLEAN:
				 String boolvalue = value.toString();
				if(boolvalue.equalsIgnoreCase(Boolean.TRUE.toString()) || boolvalue.equalsIgnoreCase("YES") || boolvalue.equalsIgnoreCase("1") || boolvalue.equalsIgnoreCase("Y"))
					return Boolean.TRUE;
				else
					return ConvertUtils.convert(value, Boolean.class);
			case JAVA_BIGDECIMAL:
				if(value instanceof String)
					return new BigDecimal(value.toString());
				else
					return ConvertUtils.convert(value, BigDecimal.class);
			default:
				if(value instanceof String)
					value = new BigDecimal(value.toString());
				try {
					return ConvertUtils.convert(value, classForJavaDataType(jdt));
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
					throw new DataConversionException(e.toString());
				}
			}	
		}


		/**
		 * Converts Reader to String
		 * @param the input reader
		 * @return the string read from the reader
		 */
		public static String toString(Reader input) {
			if(input == null)
				return null;
			try {
				StringBuffer sbuf = new StringBuffer();
				char[] cbuf = new char[DEFAULT_BUFFER_SIZE];
				int count = -1;
				try {
					int n;
					while ((n = input.read(cbuf)) != -1)
					{
						sbuf.append(cbuf, 0, n);
						count = ((count == -1) ? n : (count + n));
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
				if (count == -1)
					return null;
				else
					return sbuf.toString();
			} finally {
				if (input != null)
					try {
						input.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
			}
		}
		
		/**
		 * Converts an Input Stream to byte[]
		 * @param the input stream
		 * @return the byte[] read from the input stream
		 * @throws IOException
		 */
		public static byte[] toBytes(InputStream input) throws IOException {
			if(input == null)
				return null;
			try 
			{
			ByteArrayOutputStream output = new ByteArrayOutputStream();
			byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
			int n = 0;
			int count = -1;
			while (EOF != (n = input.read(buffer))) {
				output.write(buffer, 0, n);
				count = ((count == -1) ? n : (count + n));
			}
			output.flush();
			if(count == -1)
				return null;
			else
				return output.toByteArray();
			} finally {
				if (input != null)
					try {
						input.close();
					} catch (IOException e) {e.printStackTrace();}
			}
		} 
		
	    public static byte[] serialize(Object obj) throws IOException {
	        ByteArrayOutputStream b = new ByteArrayOutputStream();
	        ObjectOutputStream o = new ObjectOutputStream(b);
	        o.writeObject(obj);
	        return b.toByteArray();
	    }
	    
	    
		/**
		 * @param original String
		 * @param pattern String to Search
		 * @param replace String to replace
		 * @return
		 */
		public static String replaceString(String original, String pattern, String replace) 
		{
			if(original != null && !original.isEmpty() && pattern != null && !pattern.isEmpty() && replace !=null)
			{
				final int len = pattern.length();
				int found = original.indexOf(pattern);
	
				if (found > -1) {
					StringBuffer sb = new StringBuffer();
					int start = 0;
	
					while (found != -1) {
						sb.append(original.substring(start, found));
						sb.append(replace);
						start = found + len;
						found = original.indexOf(pattern, start);
					}
	
					sb.append(original.substring(start));
	
					return sb.toString();
				} else {
					return original;
				}
			}else
				return original;
		}

		/**
		 * This method will return a sanitized version of the string that can be safely written to a csv file
		 * It will strip all (commas, CR, CRLF and quotes) from the string
		 * @param content the string that needs to be cleaned
		 * @return the clean string
		 */
		public static String getCSVFriendlyString(String content)
		{
			if(content!=null && !content.isEmpty())
			{
			content = ConnectorUtils.replaceString(content, "" + COMMA, "");
			content = ConnectorUtils.replaceString(content, "" + CR, "");
			content = ConnectorUtils.replaceString(content, "" + LF, "");
			content = ConnectorUtils.replaceString(content, "" + QUOTE, "");
			}
			return content;
		}
		
	/**
	 * @param jdt
	 *            the JavaDataType for which the java class is required
	 * @return the class representing the JavaDataType
	 * @throws ClassNotFoundException
	 */
	public static Class<?> classForJavaDataType(JavaDataType jdt) throws ClassNotFoundException 
	{
		return classForJavaDataTypeFullClassName(jdt.getFullClassName());
	}
		
	/**
	 * @param cannonicalName
	 *            The JavaDataType full class name (canonical class name)
	 * @return the class representing the JavaDataType
	 * @throws ClassNotFoundException
	 */
	public static Class<?> classForJavaDataTypeFullClassName(String cannonicalName) throws ClassNotFoundException 
	{
		if (cannonicalName.equals(byte[].class.getCanonicalName()))
			cannonicalName = byte[].class.getName();
		return Class.forName(cannonicalName);
	}
	
	
	/**
	 * This method adds the files in the input directory to the input
	 * classloader. Example usage : ConnectorUtils.addDirectoryToClasspath(new
	 * file("c:\\myjars"), (URLClassLoader) this.getClass().getClassLoader());
	 * 
	 * @param dir
	 *            The directory from which to add jars to the classpath
	 * @param classLoader
	 *            The classloader to which the jars need to be added
	 * @throws InsufficientConnectInfoException if the inputs are null or invalid
	 */
	public static void addDirectoryToClasspath(File dir, URLClassLoader classLoader) throws InsufficientConnectInfoException 
	{					
		try 
		{
			if(dir == null || !dir.exists() || !dir.isDirectory())
				throw new InsufficientConnectInfoException("Directory {"+dir+"} not found");					

			if(classLoader == null)
				throw new InsufficientConnectInfoException("Input classLoader is null");					

				Class<URLClassLoader> clazz = URLClassLoader.class;
				Method method = clazz.getDeclaredMethod("addURL", new Class[] { URL.class });
				method.setAccessible(true);
	
				File[] fileList = dir.listFiles();
				// List<URL> urlList = new ArrayList<URL>();
				method.invoke(classLoader, new Object[] { dir.toURI().toURL() });
	
				// urlList.add(dir.toURI().toURL());
				for (File f : fileList) {
					if (!f.isFile())
						continue;
					// urlList.add(f.toURI().toURL());
					method.invoke(classLoader,
							new Object[] { f.toURI().toURL() });
				}
					// URL[] urlArray = urlList.toArray(new URL[0]);				
			} catch (SecurityException e) {
				e.printStackTrace();
			} catch (NoSuchMethodException e) {
				e.printStackTrace();
			} catch (IllegalArgumentException e) {
				e.printStackTrace();
			} catch (MalformedURLException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				e.printStackTrace();
			} catch (Throwable e) {
				e.printStackTrace();
			}
	}
	
		
}
