package com.informatica.connector.wrapper.util;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.informatica.cloud.api.adapter.connection.ConnectionAttribute;
import com.informatica.cloud.api.adapter.connection.ConnectionAttributeType;
import com.informatica.cloud.api.adapter.connection.StandardAttributes;

public class ConnectionAttributeUtil {

	//private static final Logger logger = Logger.getLogger(ConnectionAttributeUtil.class);
	private static final Log logger = LogFactory.getLog(ConnectionAttributeUtil.class);
	
	public static List<ConnectionAttribute> getConnectionAttributes(ISimpleConnector connectionImpl) 
	{
		logger.debug("ConnectionAttributeUtil.getConnectionAttributes(): "+connectionImpl.getClass().getCanonicalName());
		List<ConnectionAttribute> connectionAttributeList = new ArrayList<ConnectionAttribute>();
		
		Field[] flds = connectionImpl.getClass().getFields();
		for(Field field:flds)
		{ 		
			String name = field.getName();
			Class<?> type = field.getType();

			logger.debug("name: "+name);
			logger.debug("type: "+type);

			if(name.equals("username"))
			{				
				connectionAttributeList.add(StandardAttributes.username);
			}else if(name.equals("password"))
			{
				connectionAttributeList.add(StandardAttributes.password);
			}else if(name.equals("connectionUrl"))
			{
				ConnectionAttribute connectionAttribute = StandardAttributes.connectionUrl;
				String defaultValue = null;
				connectionAttribute.setMaxLength(500);
				try {		
					defaultValue =	BeanUtils.getSimpleProperty(connectionImpl, name);
					logger.debug("defaultValue: "+defaultValue);
				} catch (IllegalArgumentException e1) {
					e1.printStackTrace();
				} catch (SecurityException e1) {
					e1.printStackTrace();
				} catch (IllegalAccessException e1) {
					e1.printStackTrace();
				} catch (InvocationTargetException e1) {
					e1.printStackTrace();
				} catch (NoSuchMethodException e1) {
					e1.printStackTrace();
				} 				
				connectionAttribute.setDefaultValue(defaultValue);
				connectionAttributeList.add(connectionAttribute);
			}
		}
		
		int cnt = 1;
		for(Field field:flds)
		{ 
			String name = field.getName();
			Class<?> type = field.getType();

			logger.debug("name: "+name);
			logger.debug("type: "+type);

			int connectionAttributeType = 0;
			String label = null;
			String description = null;
			boolean mandatory = false;
			String groupName = null;
			List<String> listValues = null;
			String defaultValue = null;							
			
			ConnectionProperty cp = field.getAnnotation(ConnectionProperty.class);
			if(cp != null)
			{
				try {		
					defaultValue =	BeanUtils.getSimpleProperty(connectionImpl, name);
					logger.debug("defaultValue: "+defaultValue);
				} catch (IllegalArgumentException e1) {
					e1.printStackTrace();
				} catch (SecurityException e1) {
					e1.printStackTrace();
				} catch (IllegalAccessException e1) {
					e1.printStackTrace();
				} catch (InvocationTargetException e1) {
					e1.printStackTrace();
				} catch (NoSuchMethodException e1) {
					e1.printStackTrace();
				} 				
				
				ConnectionPropertyType pType = cp.type();
				if(pType == ConnectionPropertyType.JAVATYPE)
				{
					if(type.getName().equalsIgnoreCase(boolean.class.getName()))
					{
						connectionAttributeType = ConnectionAttributeType.BOOLEAN;
						if(defaultValue != null && defaultValue.equalsIgnoreCase("true"))
							defaultValue = "1";
						else
							defaultValue = "0";
					}else if(type.getName().equalsIgnoreCase(int.class.getName()))
					{
						connectionAttributeType = ConnectionAttributeType.NUMERIC_TYPE;
					}else if(type.getName().equalsIgnoreCase(long.class.getName()))
					{
						connectionAttributeType = ConnectionAttributeType.NUMERIC_TYPE;
					}else if(type.getName().equalsIgnoreCase(double.class.getName()))
					{
						connectionAttributeType = ConnectionAttributeType.NUMERIC_TYPE;
					}else if(type.getName().equalsIgnoreCase(String.class.getName()))
					{
						connectionAttributeType = ConnectionAttributeType.ALPHABET_TYPE | ConnectionAttributeType.NUMERIC_TYPE | ConnectionAttributeType.SYMBOLS_TYPE;
					}else
					{
						throw new IllegalArgumentException("Illegal Type" + type);
					}

				}else if(pType == ConnectionPropertyType.LIST)
				{
					connectionAttributeType = ConnectionAttributeType.LIST_TYPE;
					if(cp.listValues() == null || cp.listValues().length<1)
						throw new IllegalArgumentException("@ConnectionProperty Annotation must have ListValues for example: listValues={\"Item1\",\"Item2\"}");
					listValues = Arrays.asList(cp.listValues());
					logger.debug("listValues: "+listValues);					
				}else if(pType == ConnectionPropertyType.PASSWORD)
				{
					connectionAttributeType = ConnectionAttributeType.PASSWORD;					
				}else
				{
					throw new IllegalArgumentException("Illegal Type:" + pType);
				}
								
				mandatory = cp.required();
				label = cp.label();				
			
				if(label == null || label.isEmpty() || containsSpecialCharacters(label))
				{
					throw new IllegalArgumentException("@ConnectionProperty label for field {"+name+"}  cannot contain special characters {"+label+")");
				}
				
				ConnectionAttribute connectionAttribute = new ConnectionAttribute();
				connectionAttribute.setId(cnt);
				connectionAttribute.setName(label);
				connectionAttribute.setType(connectionAttributeType);
				connectionAttribute.setDescription(description);
				connectionAttribute.setMandatory(mandatory);
				connectionAttribute.setGroupName(groupName); //Optional
				connectionAttribute.setListValues(listValues); //Required if Type is ConnectionPropertyType.LIST_TYPE
				connectionAttribute.setDefaultValue(defaultValue); //Optional
				connectionAttributeList.add(connectionAttribute);
			
				cnt++;
			}
		}
		logger.debug(connectionAttributeList);
		if(connectionAttributeList.isEmpty())
		{
			logger.fatal("No Connection attributes are declared in class {"+connectionImpl+"}");
			throw new IllegalArgumentException("No Connection attributes are declared in class {"+connectionImpl+"}");
		}
		return connectionAttributeList;
	}

	
	public static void setConnectionAttributes(Object connectorImpl, Map<String, String> connectionAttributes) 
	{
		logger.debug("ConnectionAttributeUtil.setConnectionAttributes(): "+connectorImpl.getClass().getCanonicalName());

		Field[] flds = connectorImpl.getClass().getFields();

		try 
		{
			String username = connectionAttributes.get(StandardAttributes.username.getName());
			if(username != null && !username.isEmpty())
				BeanUtils.setProperty(connectorImpl, "username", username);
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
		
		try 
		{
			String password = connectionAttributes.get(StandardAttributes.password.getName());
			if(password != null && !password.isEmpty())
				BeanUtils.setProperty(connectorImpl, "password", password);
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}

		try 
		{
			String connectionUrl = connectionAttributes.get(StandardAttributes.connectionUrl.getName());
			if(connectionUrl != null && !connectionUrl.isEmpty())
				BeanUtils.setProperty(connectorImpl, "connectionUrl", connectionUrl);			
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
		
		
		for(Field field:flds)
		{ 
			String name = field.getName();
			Class<?> type = field.getType();

			logger.debug("name: "+name);
			logger.debug("type: "+type);

			
			ConnectionProperty cp = field.getAnnotation(ConnectionProperty.class);
			if(cp != null)
			{				
				try {					
					String label = cp.label();
					if(label != null && !label.isEmpty())
					{
						String value = connectionAttributes.get(label);
						if(value!=null)
						{							
							if(type.getCanonicalName().equals(boolean.class.getCanonicalName()))
							{
								if(value.equalsIgnoreCase(Boolean.TRUE.toString()) || value.equalsIgnoreCase("YES") || value.equalsIgnoreCase("1"))
									value = "true";
								else
									value = "false";										
							}
							//logger.debug("value: "+value);
							BeanUtils.setProperty(connectorImpl, name, value);							
						}else
						{
								if(cp.required())
									logger.error("Connection value for required Connection Attribute {"+name+"} is missing");
						}						
					}
				} catch (IllegalArgumentException e1) {
					e1.printStackTrace();
				} catch (SecurityException e1) {
					e1.printStackTrace();
				} catch (IllegalAccessException e1) {
					e1.printStackTrace();
				} catch (InvocationTargetException e1) {
					e1.printStackTrace();
				} 								
			}			
		}
	}
	

  public static boolean containsSpecialCharacters(String str)
  {
    Pattern pat = Pattern.compile("[^a-z0-9 ]", 2);
    Matcher m = pat.matcher(str);
    return m.find();
  }	

}
