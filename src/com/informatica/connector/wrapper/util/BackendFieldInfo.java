package com.informatica.connector.wrapper.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.informatica.cloud.api.adapter.metadata.Field;
import com.informatica.cloud.api.adapter.metadata.FieldAttribute;
import com.informatica.cloud.api.adapter.metadata.RecordInfo;
import com.informatica.cloud.api.adapter.typesystem.JavaDataType;
import com.informatica.connector.wrapper.typesystem.InfaTypeSystem;

public class BackendFieldInfo {

	public static final int MAX_PC_PRECISION = 104857600;
	public static final int MAX_PC_SCALE = 65535;

	private RecordInfo recordInfo;
	private String fieldName;
	private String fieldUniqueName;
	private String fieldLabel;
	private String fieldDescription;
	private Class<?> fieldType;
	private JavaDataType javaDataType;
	private int fieldPrecision = -1;
	private int fieldScale = -1;

	private boolean isMandatory = false;
	private boolean isKey = false;
	private boolean isFilterable = false;
	private String defaultValue = null;
	
	private HashMap<String,String> customAttributes = new HashMap<String, String>();

	/**
	 * @param recordInfo
	 *            The Parent Record for this field
	 * @param fieldName
	 *            The technical name string of the this field (cannot contain
	 *            space or special characters), {@link #setFieldLabel(String)}
	 *            for Setting Business Name
	 * @param fieldType
	 *            The java class of the field type (must be one of: Boolean,
	 *            Byte[], Short, Integer, Long, Float, Double, String,
	 *            java.sql.Timestamp, java.math.BigDecimal,
	 *            java.math.BigInteger)
	 * @param fieldPrecision
	 *            The field precision/length should be >0, if type is Byte[],
	 *            BigDecimal or String, Set to -1 to use default precision for
	 *            other types
	 * @param fieldScale
	 *            The scale of the field, required if type is
	 *            java.math.BigDecimal,set to -1 for other types
	 */
	public BackendFieldInfo(RecordInfo recordInfo, String fieldName, Class<?> fieldType,
			int fieldPrecision, int fieldScale) {

		if(recordInfo==null)
			throw new IllegalArgumentException("recordInfo cannot be null");
		else
			this.recordInfo = recordInfo;
		
		if(fieldName==null||fieldName.isEmpty())
			throw new IllegalArgumentException("fieldName cannot be null");
		else if(containsSpecialCharacters(fieldName))
			throw new IllegalArgumentException("fieldName cannot contain space or special characters");
		else
			this.fieldName = fieldName;
		
		if(fieldType==null)
			throw new IllegalArgumentException("fieldType cannot be null");
		else
			this.fieldType = fieldType;

		this.javaDataType = ConnectorUtils.getJavaDataType(fieldType);		

		if(this.javaDataType!=JavaDataType.JAVA_BIGDECIMAL && this.javaDataType!=JavaDataType.JAVA_STRING &&  this.javaDataType!=JavaDataType.JAVA_PRIMITIVE_BYTEARRAY && 	!this.javaDataType.getFullClassName().startsWith("java.lang.Byte"))
			fieldPrecision = InfaTypeSystem.getDefaultPrecisionForDatatype(getJavaDataType());
		
		if(fieldPrecision<=0)
				throw new IllegalArgumentException("field {"+fieldName+"} of type {"+fieldType+"} cannot have a precision = {"+fieldPrecision+"}");
		
		if(fieldPrecision>BackendFieldInfo.MAX_PC_PRECISION)
			throw new IllegalArgumentException("field {"+fieldName+"} of type {"+fieldType+"} cannot have a precision >  {"+BackendFieldInfo.MAX_PC_PRECISION+"}");

		this.fieldPrecision = fieldPrecision;

		if(this.javaDataType!=JavaDataType.JAVA_BIGDECIMAL)
			fieldScale = InfaTypeSystem.getDefaultScaleForDatatype(getJavaDataType());

		if(fieldScale<0)
			throw new IllegalArgumentException("field {"+fieldName+"} of type {"+fieldType+"} cannot have a scale = {"+fieldScale+"}");		

		if(fieldScale>BackendFieldInfo.MAX_PC_SCALE)
			throw new IllegalArgumentException("field {"+fieldName+"} of type {"+fieldType+"} cannot have a scale >  {"+BackendFieldInfo.MAX_PC_SCALE+"}");

		
		this.fieldScale = fieldScale;	
		
    	//If scale greater than precision, then force the scale to be 1 less than precision
        if (this.getFieldScale() > this.getFieldPrecision())
        	this.fieldScale = (this.getFieldPrecision() > 1) ? (this.getFieldPrecision() - 1) : 0;

		//Defaults
		setFieldUniqueName(getFieldName() + "_u");
		setFieldLabel(getFieldName());
		setFieldDescription(getFieldName());		
	}

	  	  	  
	/**
	 * @return fieldName: The field technical name
	 */
	public String getFieldName() {
		return fieldName;
	}

	/**
	 * @return fieldLabel: The field Business Name
	 */
	public String getFieldLabel() {
		if(fieldLabel!=null)
			return fieldLabel;
		else
			return fieldName;
	}

	/**
	 * Sets the business name of this Field
	 * @param fieldLabel The Business Name for the field
	 */
	public void setFieldLabel(String fieldLabel) {
		if(fieldLabel != null)
			this.fieldLabel = fieldLabel;
	}

	public String getFieldDescription() {
		if(fieldDescription!=null)
			return fieldDescription;
		else
			return fieldName;
	}
	
	public void setFieldDescription(String fieldDescription) {
		if(fieldDescription != null)
			this.fieldDescription = fieldDescription;
	}

	public Class<?> getFieldType() {
		return fieldType;
	}

	  public JavaDataType getJavaDataType() {
		  return javaDataType;			  
	  }	
	

	  
	public int getFieldPrecision() {
		return fieldPrecision;
	}
	
	public int getFieldScale() {
		return fieldScale;
	}

	public boolean isMandatory() {
		return isMandatory;
	}
	
	public void setMandatory(boolean isMandatory) {
		this.isMandatory = isMandatory;
	}

	public boolean isKey() {
		return isKey;
	}
	
	public void setKey(boolean isKey) {
		this.isKey = isKey;
	}

	public boolean isFilterable() {
		return isFilterable;
	}
	
	/**
	 * set the field as filterable default is false
	 * @param isFilterable set true if field can be used to filter data
	 */
	public void setFilterable(boolean isFilterable) {
		this.isFilterable = isFilterable;
	}
	  	
	public RecordInfo getBackendObjectInfo() {
		return recordInfo;
	}

	public String getFieldUniqueName() {
		return fieldUniqueName;
	}


	public void setFieldUniqueName(String fieldUniqueName) {
		this.fieldUniqueName = fieldUniqueName;
	}


	public HashMap<String,String> getCustomAttributes() {
		return customAttributes;
	}


	public void setCustomAttributes(HashMap<String,String> customAttributes) {
		this.customAttributes = customAttributes;
	}

	public String getDefaultValue() {
		return defaultValue;
	}


	public void setDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue;
	}
	
	private boolean containsSpecialCharacters(String str)
	{
	    Pattern pat = Pattern.compile("[^a-z0-9_]",java.util.regex.Pattern.CASE_INSENSITIVE);
	    Matcher m = pat.matcher(str);
	    return m.find();
	}	
	
	/**
	 * This return a Field Object that contains all the Attributes from this
	 * object
	 * 
	 * @return
	 */
	public Field getField() {

		Field f = new Field();
		f.setContainingRecord(this.getBackendObjectInfo());
		f.setDisplayName(this.getFieldName());
		f.setUniqueName(this.getFieldUniqueName());
		f.setLabel(this.getFieldLabel());
		f.setJavaDatatype(this.getJavaDataType());
		// This should not be required!!
		f.setDatatype(InfaTypeSystem.getDataType(f.getJavaDatatype()));

		f.setPrecision(this.getFieldPrecision());

		f.setScale(this.getFieldScale());
		if (f.getScale() > f.getPrecision()) {
			f.setScale(f.getPrecision());
		}

		f.setFilterable(this.isFilterable());
		f.setMandatory(this.isMandatory());
		f.setKey(this.isKey());
		f.setDescription(this.getFieldDescription());
		f.setDefaultValue(this.getDefaultValue());

		// This an example of how to set custom field attributes
		HashMap<String, String> customAttributes = this.getCustomAttributes();
		if (!customAttributes.isEmpty()) {
			List<FieldAttribute> fieldAttribs = new ArrayList<FieldAttribute>();
			for (String atribName : customAttributes.keySet()) {
				FieldAttribute fieldAttrib = new FieldAttribute();
				fieldAttrib.setName(atribName);
				fieldAttrib.setValue(customAttributes.get(atribName));
				fieldAttribs.add(fieldAttrib);
			}
			f.setCustomAttributes(fieldAttribs);
		}
		return f;
	}

}
