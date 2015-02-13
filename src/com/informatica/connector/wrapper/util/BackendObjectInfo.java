package com.informatica.connector.wrapper.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.informatica.cloud.api.adapter.annotation.NotEmpty;
import com.informatica.cloud.api.adapter.annotation.NotNull;
import com.informatica.cloud.api.adapter.metadata.RecordInfo;

public class BackendObjectInfo {

	@NotNull
	@NotEmpty
	private String objectCanonicalName;

	@NotNull
	@NotEmpty
	private String objectName;

	@NotNull
	@NotEmpty
	private String objectLabel;

	private String instanceName;

	private String objectType = "Table";	  
	  
	/**
	 * @param objectCanonicalName
	 *            : The fully qualified name of the object. The same value is
	 *            returned in RecordInfo.getCatalogName()
	 * @param objectName
	 *            : The object name string, cannot contain any spaces or special
	 *            characters. This is what shows up in the source/target object
	 *            list drop down. The same value is returned in
	 *            RecordInfo.getRecordName()
	 * @param objectLabel
	 *            : This is the business name or label of the object
	 */
 	public BackendObjectInfo(String objectCanonicalName, String objectName, String objectLabel) {
			if(objectCanonicalName==null||objectCanonicalName.isEmpty())
				throw new IllegalArgumentException("objectCanonicalName cannot be null");
			else
				this.objectCanonicalName = objectCanonicalName;
			
			if(objectName==null)
				throw new IllegalArgumentException("objectName cannot be null");
			else if(containsSpecialCharacters(objectName))
				throw new IllegalArgumentException("objectName cannot contain space or special characters");
			else
				this.objectName = objectName;

			if(objectLabel!=null)
				this.objectLabel = objectLabel;
			else
				this.objectLabel = objectCanonicalName;	
			
			//Defaults
			this.objectType = "Table";
			this.instanceName = objectName;
		}


	public String getObjectType() {
		return objectType;
	}


	public void setObjectType(String objectType) {
		this.objectType = objectType;
	}


	public String getCanonicalName() {
		return objectCanonicalName;
	}


	public String getObjectName() {
		return objectName;
	}


	public String getLabel() {
		return objectLabel;
	}
	
	  public String getInstanceName() {
		return instanceName;
	}


	public void setInstanceName(String instanceName) {
		this.instanceName = instanceName;
	}

	private boolean containsSpecialCharacters(String str)
	{
	    Pattern pat = Pattern.compile("[^a-z0-9_-]",java.util.regex.Pattern.CASE_INSENSITIVE);
	    Matcher m = pat.matcher(str);
	    return m.find();
	}

	public RecordInfo getRecordInfo()
	{	
		RecordInfo info = new RecordInfo();
		info.setCatalogName(this.getCanonicalName());
		info.setRecordName(this.getObjectName());
		info.setLabel(this.getLabel());
		info.setRecordType(this.getObjectType());
		info.setInstanceName(this.getInstanceName());
		return info;
	}

}
