package com.informatica.connector.wrapper.typesystem;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.informatica.cloud.api.adapter.typesystem.DataType;
import com.informatica.cloud.api.adapter.typesystem.ITypeSystem;
import com.informatica.cloud.api.adapter.typesystem.JavaDataType;

public class InfaTypeSystem implements ITypeSystem{
	
	HashMap<DataType, List<JavaDataType>> dataTypeMap;
	List<DataType> nativeDataTypes;

	@Override
	public Map<DataType, List<JavaDataType>> getDatatypeMapping() {
		if(dataTypeMap == null){
			dataTypeMap = new HashMap<DataType, List<JavaDataType>>();
			for (DataType dt : getNativeDataTypes()) {
				try {
					dataTypeMap.put(dt, getJavaDataTypesForDataType(dt.getName()));
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		return dataTypeMap;			
	}
	
	@Override
	public List<DataType> getNativeDataTypes() {
		if (nativeDataTypes == null) {
			nativeDataTypes = new ArrayList<DataType>();
			for (AttributeTypeCode type : AttributeTypeCode.values()) {
				int id = type.getDataTypeId();
				String name = type.getDataTypeName();
				DataType dt = new DataType(name, id);
				dt.setDefaultPrecision(type.getDefaultPrecision());
				dt.setDefaultScale(type.getDefaultScale());
				dt.setHasScale(type.hasScale());
				nativeDataTypes.add(dt);
			}
		}
		return nativeDataTypes;
	}
	

	private List<JavaDataType> getJavaDataTypesForDataType(String dataTypeName)
			throws IllegalArgumentException {
		
		ArrayList<JavaDataType> listOfJavaDataTypes = new ArrayList<JavaDataType>();
		
		switch (AttributeTypeCode.fromValue(dataTypeName)) {

		case INTEGER:
			listOfJavaDataTypes.add(JavaDataType.JAVA_INTEGER);
			//listOfJavaDataTypes.add(JavaDataType.JAVA_BIGINTEGER);
			break;

		case BOOLEAN:
			listOfJavaDataTypes.add(JavaDataType.JAVA_BOOLEAN);
			break;

		case DATETIME:
			listOfJavaDataTypes.add(JavaDataType.JAVA_TIMESTAMP);
			break;

		case DECIMAL:
			listOfJavaDataTypes.add(JavaDataType.JAVA_BIGDECIMAL);
			//listOfJavaDataTypes.add(JavaDataType.JAVA_DOUBLE);
			//listOfJavaDataTypes.add(JavaDataType.JAVA_FLOAT);
			break;

		case STRING:
			listOfJavaDataTypes.add(JavaDataType.JAVA_STRING);
			break;

		case DOUBLE:
			listOfJavaDataTypes.add(JavaDataType.JAVA_DOUBLE);
			break;

		case BINARY:
			listOfJavaDataTypes.add(JavaDataType.JAVA_PRIMITIVE_BYTEARRAY);
			break;

		case SHORT:
			listOfJavaDataTypes.add(JavaDataType.JAVA_SHORT);
			break;

		case BIGINT:
			listOfJavaDataTypes.add(JavaDataType.JAVA_BIGINTEGER);
			break;

		case LONG:
			listOfJavaDataTypes.add(JavaDataType.JAVA_LONG);
			break;

		case FLOAT:
			listOfJavaDataTypes.add(JavaDataType.JAVA_FLOAT);
			break;

		case BYTE:
			listOfJavaDataTypes.add(JavaDataType.JAVA_BYTES);
			break;

		default:
			throw new IllegalArgumentException(dataTypeName);
		}
		return listOfJavaDataTypes;
	}
	
	
	public static DataType getDataType(JavaDataType javaDataType) {
		AttributeTypeCode tc = null;
		switch (javaDataType) {
		case JAVA_STRING:
			tc = AttributeTypeCode.STRING;
			break;
		case JAVA_INTEGER:
			tc = AttributeTypeCode.INTEGER;
			break;
		case JAVA_SHORT:
			tc = AttributeTypeCode.SHORT;
			break;
		case JAVA_LONG:
			tc = AttributeTypeCode.LONG;
			break;
		case JAVA_DOUBLE:
			tc = AttributeTypeCode.DOUBLE;
			break;
		case JAVA_FLOAT:
			tc = AttributeTypeCode.FLOAT;
			break;
		case JAVA_BIGINTEGER:
			tc = AttributeTypeCode.BIGINT;
			break;
		case JAVA_BIGDECIMAL:
			tc = AttributeTypeCode.DECIMAL;
			break;
		case JAVA_TIMESTAMP:
			tc = AttributeTypeCode.DATETIME;
			break;
		case JAVA_PRIMITIVE_BYTEARRAY:
			tc = AttributeTypeCode.BINARY;
			break;
		case JAVA_BOOLEAN:
			tc = AttributeTypeCode.BOOLEAN;
			break;
		case JAVA_BYTES:
			tc = AttributeTypeCode.BYTE;
		default:
			throw new IllegalArgumentException(javaDataType.toString());

		}
		DataType dt = new DataType(tc.getDataTypeName(), tc.getDataTypeId());
		dt.setDefaultPrecision(tc.getDefaultPrecision());
		dt.setDefaultScale(tc.getDefaultScale());
		dt.setHasScale(tc.hasScale());		
		return dt;
	}
	
	public static int getDefaultPrecisionForDatatype(final JavaDataType javaDataType) {
		return getDataType(javaDataType).getDefaultPrecision();
	}
	
	public static int getDefaultScaleForDatatype(final JavaDataType javaDataType)
	{
		return getDataType(javaDataType).getDefaultScale();
	}
	
	
}
