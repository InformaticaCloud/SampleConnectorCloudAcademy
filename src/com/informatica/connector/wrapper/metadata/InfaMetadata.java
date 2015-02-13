package com.informatica.connector.wrapper.metadata;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.informatica.cloud.api.adapter.common.OperationContext;
import com.informatica.cloud.api.adapter.metadata.DataPreviewException;
import com.informatica.cloud.api.adapter.metadata.Field;
import com.informatica.cloud.api.adapter.metadata.FieldInfo;
import com.informatica.cloud.api.adapter.metadata.FilterInfo;
import com.informatica.cloud.api.adapter.metadata.IImplicitMultiObjectMetadata;
import com.informatica.cloud.api.adapter.metadata.MetadataReadException;
import com.informatica.cloud.api.adapter.metadata.RecordInfo;
import com.informatica.connector.wrapper.connection.InfaConnection;
import com.informatica.connector.wrapper.plugin.InfaPlugin;
import com.informatica.connector.wrapper.util.BackendFieldInfo;
import com.informatica.connector.wrapper.util.BackendObjectInfo;

public class InfaMetadata implements IImplicitMultiObjectMetadata {
	
	private static final Log logger = LogFactory.getLog(InfaMetadata.class);

	InfaConnection connection = null;
	InfaPlugin plugin = null;
  	

	public InfaMetadata(InfaPlugin plugin, InfaConnection connection) {
		logger.debug("InfaMetadata()");
		this.connection = connection;
		this.plugin = plugin;
	}

	@Override
	public List<RecordInfo> filterRecords(Pattern pattern) throws MetadataReadException
	{
		logger.debug("InfaMetadata.filterRecords(\"" + pattern+ "\")");
		List<RecordInfo> recInfoList = new ArrayList<RecordInfo>();
		try
		{
			List<BackendObjectInfo> backendObjectInfoList = this.connection.getConnectorImpl().getObjectList(pattern,plugin.getContext());

			if(backendObjectInfoList==null)
				return recInfoList;
				
			for(BackendObjectInfo backendObjectInfo: backendObjectInfoList)
			{
				recInfoList.add(backendObjectInfo.getRecordInfo());
			}
			return recInfoList;
		}catch(Throwable t)
		{
			logger.error("Failed to fetch list of records from backend", t);
			t.printStackTrace();
			MetadataReadException ex =  new MetadataReadException(t.toString());
			ex.initCause(t);
			throw ex;			
		}
	}
	
	@Override
	public List<RecordInfo> getAllRecords() throws MetadataReadException {
		logger.debug("InfaMetadata.getAllRecords()");
		return filterRecords(Pattern.compile(".*"));
	}
	
	@Override
	public List<RecordInfo> getRelatedRecords(RecordInfo primaryRecord)
			throws MetadataReadException {
		logger.debug("InfaMetadata.getRelatedRecords("+primaryRecord.getCatalogName()+")");
		try
		{
			List<RecordInfo> recInfoList = new ArrayList<RecordInfo>();
			List<BackendObjectInfo> backendObjectInfoList = this.connection.getConnectorImpl().getRelatedObjectList(primaryRecord,plugin.getContext());

			if(backendObjectInfoList==null)
				return recInfoList;

			for(BackendObjectInfo backendObjectInfo: backendObjectInfoList)
			{
				recInfoList.add(backendObjectInfo.getRecordInfo());
			}
			return recInfoList;
		} catch (RuntimeException t) {
			t.printStackTrace();
			MetadataReadException ex =  new MetadataReadException(t.toString());
			ex.initCause(t);
			throw ex;			
		}
	}


	@Override
	 public List<RecordInfo> getContainedRecords(RecordInfo primaryRecord) throws MetadataReadException {			 
		logger.debug("InfaMetadata.getContainedRecords("+primaryRecord.getCatalogName()+")");
		try
		{
			List<RecordInfo> recInfoList = new ArrayList<RecordInfo>();
			List<BackendObjectInfo> backendObjectInfoList = this.connection.getConnectorImpl().getContainedObjectList(primaryRecord,plugin.getContext());

			if(backendObjectInfoList==null)
				return recInfoList;

			for(BackendObjectInfo backendObjectInfo: backendObjectInfoList)
			{
				recInfoList.add(backendObjectInfo.getRecordInfo());
			}
			return recInfoList;
		} catch (RuntimeException t) {
			t.printStackTrace();
			MetadataReadException ex =  new MetadataReadException(t.toString());
			ex.initCause(t);
			throw ex;			
		}
	}
	 

	@Override
	public List<Field> getFields(RecordInfo recordInfo, boolean refreshFields) throws MetadataReadException {
		logger.debug("InfaMetadata.getFields(\"" + recordInfo.getCatalogName() != null ? recordInfo.getCatalogName(): recordInfo.getRecordName() +"\")");	
		List<Field> fields = new ArrayList<Field>();
	
		try 
		{	
			if(recordInfo == null || recordInfo.getRecordName() == null)			
			{
				throw new IllegalArgumentException("Input recordInfo cannot be null");
			}
			
			//This is a work around to fix a bug in the INFA_JUnit test suite
			if(recordInfo.getCatalogName() == null)
			{
					recordInfo = getSelectRecordInfo(recordInfo.getRecordName());
			}			

			List<BackendFieldInfo> backendFieldInfos = this.connection.getConnectorImpl().getFieldList(recordInfo, plugin.getContext());
			
			if(backendFieldInfos==null)
				return fields;

			for(BackendFieldInfo backendFieldInfo:backendFieldInfos)
			{							
				fields.add(backendFieldInfo.getField());
			}

			if (fields.isEmpty()) {
				logger.error("Error fetching metadata for Object ["+ recordInfo.getCatalogName()!=null ? recordInfo.getCatalogName(): recordInfo.getRecordName() + "]");
			}

		} catch (RuntimeException t) {
			t.printStackTrace();
			MetadataReadException ex =  new MetadataReadException(t.toString());
			ex.initCause(t);
			throw ex;			
		}
		return fields;


	}

	@Override
	public List<Field> getFields(RecordInfo childRecordInfo, RecordInfo parentRecordInfo, boolean refreshFields)
			throws MetadataReadException {
		logger.debug("InfaMetadata.getFields(\"" + childRecordInfo.getCatalogName() != null ? childRecordInfo.getCatalogName(): childRecordInfo.getRecordName() +"\",\"" + parentRecordInfo.getCatalogName() != null ? parentRecordInfo.getCatalogName(): parentRecordInfo.getRecordName() +"\")");	
		List<Field> fields = new ArrayList<Field>();	
		
		try 
		{	
			if(childRecordInfo == null || childRecordInfo.getRecordName() == null)			
			{
				throw new IllegalArgumentException("Input recordInfo cannot be null");
			}
			
			List<BackendFieldInfo> backendFieldInfos = this.connection.getConnectorImpl().getFieldList(childRecordInfo,parentRecordInfo,plugin.getContext());
			
			if(backendFieldInfos==null)
				return fields;

			for(BackendFieldInfo backendFieldInfo:backendFieldInfos)
			{							
				fields.add(backendFieldInfo.getField());
			}

			if (fields.isEmpty()) {
				logger.error("Error fetching metadata for Object ["+ childRecordInfo.getCatalogName()!=null ? childRecordInfo.getCatalogName(): childRecordInfo.getRecordName() + "]");
			}

		} catch (RuntimeException t) {
			t.printStackTrace();
			MetadataReadException ex =  new MetadataReadException(t.toString());
			ex.initCause(t);
			throw ex;			
		}
		return fields;
	}

	@Override
	public String[][] getDataPreview(RecordInfo recordInfo, int pageSize,List<FieldInfo> fieldInfoList) throws DataPreviewException {
		logger.debug("InfaMetadata.getDataPreview(\"" + recordInfo.getCatalogName()!=null?recordInfo.getCatalogName():recordInfo.getRecordName() + "\",\""+fieldInfoList+"\")");

		if(fieldInfoList == null)
			fieldInfoList = new ArrayList<FieldInfo>();
		
		try
		{	
			//This is a work around to fix a bug in the INFA_JUnit test suite
			if(recordInfo.getCatalogName() == null)
			{
					recordInfo = getSelectRecordInfo(recordInfo.getRecordName());
			}			

			List<Field> fieldList = getFields(recordInfo,false);	

			if(fieldInfoList.isEmpty())
			{
				for (Field f : fieldList) {
					FieldInfo aFieldInfo = new FieldInfo();
					aFieldInfo.setDisplayName(f.getDisplayName());
					aFieldInfo.setUniqueName(f.getUniqueName());
					aFieldInfo.setLabel(f.getLabel());				
					fieldInfoList.add(aFieldInfo);
				}
			}

			return this.connection.getConnectorImpl().getDataPreview(recordInfo, pageSize, fieldList, plugin.getContext());			
		} catch (RuntimeException t) {
			t.printStackTrace();
			DataPreviewException ex =  new DataPreviewException(t.toString());
			ex.initCause(t);
			throw ex;			
		}catch(Throwable t)
		{
			t.printStackTrace();
			if(t instanceof DataPreviewException)
				throw (DataPreviewException)t;			
			DataPreviewException ex =  new DataPreviewException(t.toString());
			ex.initCause(t);
			throw ex;			
			/*
			if(fieldInfoList.isEmpty())
			{
				FieldInfo aFieldInfo = new FieldInfo();
				aFieldInfo.setDisplayName("Error");
				aFieldInfo.setUniqueName("Error");
				aFieldInfo.setLabel("Error");
				fieldInfoList.add(aFieldInfo);				
			}
			String[][] dataRows = new String[1][fieldInfoList.size()];
			if(fieldInfoList.size()>0)
				dataRows[0][0] = t.getLocalizedMessage();
			return dataRows;
			*/
		}
	}
	
	@Override
	public String[][] getDataPreview(RecordInfo recordInfo, RecordInfo parentRecordInfo,
			int pageSize, List<FieldInfo> fieldInfoList) throws DataPreviewException {
		return this.getDataPreview(recordInfo, pageSize, fieldInfoList);
	}

	
	@Override
	public String serializeFilterCriteria(List<FilterInfo> fieldInfoList,RecordInfo primaryRecord) {
		logger.debug("InfaMetadata.serializeFilterCriteria(\"" + fieldInfoList+ "\")");
		try {
			return this.connection.getConnectorImpl().serializeFilterCriteria(fieldInfoList, primaryRecord);
		} catch (Throwable t) {
			t.printStackTrace();
		}
		return "";
	}
	

	@Override
	public String[] getReadOpDesigntimeAttribValues(String[] names,RecordInfo record) throws MetadataReadException {
		logger.debug("InfaMetadata.getReadOpDesigntimeAttribValues(\"" + names+ "\")");
		try {
			return this.connection.getConnectorImpl().getReadOpDesigntimeAttribValues(names, record);
		} catch (Throwable t) {
			t.printStackTrace();
			MetadataReadException ex =  new MetadataReadException(t.toString());
			ex.initCause(t);
			throw ex;
		}
	}

	@Override
	public String[] getWriteOpDesigntimeAttribValues(String[] names,RecordInfo record) throws MetadataReadException {
		logger.debug("InfaMetadata.getWriteOpDesigntimeAttribValues(\"" + names+ "\")");
		try {
			return this.connection.getConnectorImpl().getWriteOpDesigntimeAttribValues(names, record);
		} catch (Throwable t) {
			t.printStackTrace();
			MetadataReadException ex =  new MetadataReadException(t.toString());
			ex.initCause(t);
			throw ex;
		}
	}


	@Override
	public String[] getReadOpDesigntimeAttribValues(String[] attrNames,
			RecordInfo primaryRecord, List<RecordInfo> secondaryRecords)
			throws MetadataReadException {
		logger.debug("InfaMetadata.getReadOpDesigntimeAttribValues(\"" + attrNames+ "\")");
		try {
			return this.connection.getConnectorImpl().getReadOpDesigntimeAttribValues(attrNames, primaryRecord,secondaryRecords);
		} catch (Throwable t) {
			t.printStackTrace();
			MetadataReadException ex =  new MetadataReadException(t.toString());
			ex.initCause(t);
			throw ex;
		}
	}

	@Override
	public String[] getRecordAttributeValue(String[] attrNames,RecordInfo primaryRecord) throws MetadataReadException {
		logger.debug("InfaMetadata.getRecordAttributeValue(\"" + attrNames+ "\")");
		try {
			return this.connection.getConnectorImpl().getRecordAttributeValue(attrNames, primaryRecord);
		} catch (Throwable t) {
			t.printStackTrace();
			MetadataReadException ex =  new MetadataReadException(t.toString());
			ex.initCause(t);
			throw ex;
		}
	}

	@Override
	public String[] getRecordAttributeValue(String[] attrNames,
			RecordInfo primaryRecord, RecordInfo secondaryRecords) throws MetadataReadException {
		logger.debug("InfaMetadata.getRecordAttributeValue(\"" + attrNames+ "\")");
		try {
			return this.connection.getConnectorImpl().getRecordAttributeValue(attrNames, primaryRecord,secondaryRecords);
		} catch (Throwable t) {
			t.printStackTrace();
			MetadataReadException ex =  new MetadataReadException(t.toString());
			ex.initCause(t);
			throw ex;
		}
	}


	@Override
	public String[] getWriteOpDesigntimeAttribValues(String[] attrNames,
			RecordInfo primaryRecord, List<RecordInfo> secondaryRecords)
			throws MetadataReadException {
		logger.debug("InfaMetadata.getWriteOpDesigntimeAttribValues(\"" + attrNames+ "\")");
		try {
			return this.connection.getConnectorImpl().getWriteOpDesigntimeAttribValues(attrNames, primaryRecord,secondaryRecords);
		} catch (Throwable t) {
			t.printStackTrace();
			MetadataReadException ex =  new MetadataReadException(t.toString());
			ex.initCause(t);
			throw ex;
		}
	}

	public RecordInfo getSelectRecordInfo(String recordName) throws MetadataReadException 
	{
		logger.debug("InfaMetadata.getSelectRecordInfo("+recordName+")");
		try
		{
			return this.connection.getConnectorImpl().getSelectRecordInfo(recordName);
		} catch (RuntimeException t) {
			t.printStackTrace();
			MetadataReadException ex =  new MetadataReadException(t.toString());
			ex.initCause(t);
			throw ex;
		}
	}	
	
	

}
