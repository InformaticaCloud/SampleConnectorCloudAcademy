package com.informatica.connector.wrapper.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

import com.informatica.cloud.api.adapter.common.OperationContext;
import com.informatica.cloud.api.adapter.connection.ConnectionFailedException;
import com.informatica.cloud.api.adapter.connection.InsufficientConnectInfoException;
import com.informatica.cloud.api.adapter.metadata.AdvancedFilterInfo;
import com.informatica.cloud.api.adapter.metadata.DataPreviewException;
import com.informatica.cloud.api.adapter.metadata.Field;
import com.informatica.cloud.api.adapter.metadata.FieldAttribute;
import com.informatica.cloud.api.adapter.metadata.FilterInfo;
import com.informatica.cloud.api.adapter.metadata.MetadataReadException;
import com.informatica.cloud.api.adapter.metadata.RecordAttribute;
import com.informatica.cloud.api.adapter.metadata.RecordAttributeScope;
import com.informatica.cloud.api.adapter.metadata.RecordInfo;
import com.informatica.cloud.api.adapter.metadata.TransformationInfo;
import com.informatica.cloud.api.adapter.runtime.exception.DataConversionException;
import com.informatica.cloud.api.adapter.runtime.exception.FatalRuntimeException;
import com.informatica.cloud.api.adapter.runtime.exception.InitializationException;
import com.informatica.cloud.api.adapter.runtime.exception.ReadException;
import com.informatica.cloud.api.adapter.runtime.exception.ReflectiveOperationException;
import com.informatica.cloud.api.adapter.runtime.exception.WriteException;
import com.informatica.cloud.api.adapter.runtime.utils.IInputDataBuffer;
import com.informatica.cloud.api.adapter.runtime.utils.IOutputDataBuffer;
import com.informatica.cloud.api.adapter.runtime.utils.OperationResult;

public abstract class ISimpleConnector {

	public abstract boolean connect() throws InsufficientConnectInfoException,
			ConnectionFailedException;

	public abstract boolean disconnect();
	
	public abstract void flush();

	public abstract UUID getPluginUUID();

	public RecordInfo getSelectRecordInfo(String recordName) throws MetadataReadException 
	{
		List<BackendObjectInfo> recInfos = this.getObjectList(null, null);
		for(BackendObjectInfo backendObjectInfo:recInfos)
		{
			if(backendObjectInfo.getObjectName().equals(recordName))
			{
				return backendObjectInfo.getRecordInfo();
			}
		}		
		throw new  MetadataReadException("Record {"+recordName+"} not found in system");		
	}

	public abstract List<BackendObjectInfo> getObjectList(Pattern pattern,
			OperationContext operationContext) throws MetadataReadException;

	public abstract List<BackendFieldInfo> getFieldList(RecordInfo recordInfo,
			OperationContext operationContext) throws MetadataReadException;

	public List<BackendFieldInfo> getFieldList(RecordInfo childRecordInfo, RecordInfo parentRecordInfo, OperationContext operationContext)
			throws MetadataReadException {
		return this.getFieldList(childRecordInfo,operationContext);
	}

	
	public String[][] getDataPreview(RecordInfo recordInfo, int pageSize,
			List<Field> fieldList, OperationContext operationContext)
			throws ConnectionFailedException, ReflectiveOperationException,
			ReadException, DataConversionException, FatalRuntimeException,
			MetadataReadException {
		PreviewDataBufferImpl PreviewDataBufferImpl = new PreviewDataBufferImpl(fieldList);
		this.read(PreviewDataBufferImpl, fieldList, recordInfo, null, null,
				null, true, pageSize, null, null, null, false);
		return PreviewDataBufferImpl.getDataPreview();
	}
	
	public String[][] getDataPreview(RecordInfo recordInfo, RecordInfo parentRecordInfo,
			int pageSize, List<Field> fieldList,OperationContext operationContext) throws DataPreviewException, MetadataReadException, ConnectionFailedException, ReflectiveOperationException, ReadException, DataConversionException, FatalRuntimeException {
		PreviewDataBufferImpl PreviewDataBufferImpl = new PreviewDataBufferImpl(fieldList);
		List<RecordInfo> childRecordInfo = new ArrayList<RecordInfo>();
		childRecordInfo.add(recordInfo);
		this.read(PreviewDataBufferImpl, fieldList, parentRecordInfo, null, null,
			null, true, pageSize, null, null, childRecordInfo, false);
		return PreviewDataBufferImpl.getDataPreview();
	}


	public void initializeAndValidateReader(List<Field> fieldList,
			RecordInfo primaryRecordInfo, List<RecordInfo> relatedRecordList,
			List<FilterInfo> filterInfoList,
			AdvancedFilterInfo advancedFilterInfo,
			Map<RecordInfo, Map<String, String>> recordAttributes,
			Map<String, String> readOperationAttributes,
			List<RecordInfo> childRecordList) throws InitializationException {
	}

	public boolean read(IOutputDataBuffer dataBufferInstance,
			List<Field> fieldList, RecordInfo recordInfo,
			List<RecordInfo> relatedRecordInfoList,
			List<FilterInfo> filterInfoList,
			AdvancedFilterInfo advancedFilterInfo, boolean isPreview,
			long pagesize) throws ConnectionFailedException,
			ReflectiveOperationException, ReadException,
			DataConversionException, FatalRuntimeException {
		return false;
	}

	public boolean read(IOutputDataBuffer dataBufferInstance,
			List<Field> fieldList, RecordInfo primaryRecordInfo,
			List<RecordInfo> relatedRecordList,
			List<FilterInfo> filterInfoList,
			AdvancedFilterInfo advancedFilterInfo, boolean isPreview,
			long pagesize,
			Map<RecordInfo, Map<String, String>> recordAttributes,
			Map<String, String> readOperationAttributes,
			List<RecordInfo> childRecordList, boolean isLookup)
			throws ConnectionFailedException, ReflectiveOperationException,
			ReadException, DataConversionException, FatalRuntimeException {
		return this.read(dataBufferInstance, fieldList, primaryRecordInfo,
				relatedRecordList, filterInfoList, advancedFilterInfo,
				isPreview, pagesize);
	}

	public void initializeAndValidateWriter(List<Field> fieldList,
			RecordInfo recordInfo, WriteOperation writeOperation,
			Map<RecordInfo, Map<String, String>> recordAttributes,
			Map<String, String> operationAttributes,
			List<RecordInfo> secondaryRecordInfoList)
			throws InitializationException {
	}

	public List<OperationResult> write(IInputDataBuffer inputDataBuffer,
			List<Field> fieldList, RecordInfo recordInfo,
			WriteOperation writeOperation) throws WriteException,
			FatalRuntimeException, ConnectionFailedException,
			ReflectiveOperationException, DataConversionException,
			FatalRuntimeException {
		List<OperationResult> rowStatus = new ArrayList<OperationResult>();
		return rowStatus;
	}

	public List<OperationResult> write(IInputDataBuffer inputDataBuffer,
			List<Field> fieldList, RecordInfo recordInfo,
			WriteOperation writeOperation,
			Map<RecordInfo, Map<String, String>> recordAttributes,
			Map<String, String> operationAttributes,
			List<RecordInfo> secondaryRecordInfoList) throws WriteException,
			FatalRuntimeException, ConnectionFailedException,
			ReflectiveOperationException, DataConversionException,
			FatalRuntimeException {
		return this.write(inputDataBuffer, fieldList, recordInfo,
				writeOperation);
	}

	public List<OperationResult> delete(IInputDataBuffer inputDataBuffer,
			List<Field> fieldList, RecordInfo primaryRecordInfo)
			throws WriteException, FatalRuntimeException,
			ConnectionFailedException, ReflectiveOperationException,
			DataConversionException, FatalRuntimeException {
		List<OperationResult> rowStatus = new ArrayList<OperationResult>();
		return rowStatus;
	}

	public List<OperationResult> delete(IInputDataBuffer inputDataBuffer,
			List<Field> fieldList, RecordInfo primaryRecordInfo,
			Map<RecordInfo, Map<String, String>> recordAttributes,
			Map<String, String> operationAttributes,
			List<RecordInfo> secondaryRecordInfoList) throws WriteException,
			FatalRuntimeException, ConnectionFailedException,
			ReflectiveOperationException, DataConversionException,
			FatalRuntimeException {
		return this.delete(inputDataBuffer, fieldList, primaryRecordInfo);
	}

	public List<BackendObjectInfo> getRelatedObjectList(
			RecordInfo primaryRecord, OperationContext operationContext)
			throws MetadataReadException {
		List<BackendObjectInfo> recInfoList = new ArrayList<BackendObjectInfo>();
		return recInfoList;
	}

	public List<BackendObjectInfo> getContainedObjectList(
			RecordInfo primaryRecord, OperationContext operationContext)
			throws MetadataReadException {
		List<BackendObjectInfo> recInfoList = new ArrayList<BackendObjectInfo>();
		return recInfoList;
	}

	/*
	 * This method returns custom record attributes
	 */
	public List<RecordAttribute> getRecordAttributes() throws MetadataReadException {
		ArrayList<RecordAttribute> listOfRecordAttribs = new ArrayList<RecordAttribute>();
		return listOfRecordAttribs;
	}

	/*
	 * This method returns a list of custom field attributes
	 */
	public List<FieldAttribute> getFieldAttributes()  throws MetadataReadException {
		ArrayList<FieldAttribute> listOfFieldAttrs = new ArrayList<FieldAttribute>();
		return listOfFieldAttrs;
	}

	/*
	 * This method returns the custom attributes for read operation
	 */
	public List<RecordAttribute> getReadOperationAttributes() throws MetadataReadException {
		ArrayList<RecordAttribute> listOfReadOpAttribs = new ArrayList<RecordAttribute>();
		RecordAttribute rAttrib = new RecordAttribute(
				RecordAttributeScope.RUNTIME);
		rAttrib.setName("Check Parameter");
		rAttrib.setDescription("Check Parameter");
		rAttrib.setDatatype("BOOLEAN");
		rAttrib.setSessionVarsAllowed(false);
		rAttrib.setId(1);
		listOfReadOpAttribs.add(rAttrib);
		return listOfReadOpAttribs;
	}

	/*
	 * This method returns the custom attributes for write operation
	 */
	public List<RecordAttribute> getWriteOperationAttributes() throws MetadataReadException {
		return new ArrayList<RecordAttribute>();
	}

	public List<TransformationInfo> getTransformationOperations() {
		return new ArrayList<TransformationInfo>();
	}

	public List<RecordAttribute> getTransformationAttributes(
			TransformationInfo transform)  throws MetadataReadException {
		return new ArrayList<RecordAttribute>();
	}


	public String serializeFilterCriteria(List<FilterInfo> fieldInfoList,
			RecordInfo primaryRecord) {
		return "";
	}

	public String[] getReadOpDesigntimeAttribValues(String[] names,
			RecordInfo record) throws MetadataReadException {
		String[] retVal = new String[names.length];
		String emptyString = "";
		Arrays.fill(retVal, emptyString);
		return retVal;
	}

	public String[] getWriteOpDesigntimeAttribValues(String[] names,
			RecordInfo record) throws MetadataReadException {
		String[] retVal = new String[names.length];
		String emptyString = "";
		Arrays.fill(retVal, emptyString);
		return retVal;
	}

	public String[] getReadOpDesigntimeAttribValues(String[] attrNames,
			RecordInfo primaryRecord, List<RecordInfo> secondaryRecords)
			throws MetadataReadException {
		String[] retVal = new String[attrNames.length];
		Arrays.fill(retVal, "");
		return retVal;
	}

	public String[] getRecordAttributeValue(String[] attrNames,
			RecordInfo primaryRecord, RecordInfo secondaryRecords)
			throws MetadataReadException {
		String[] attrValues = new String[attrNames.length];
		Arrays.fill(attrValues, "");
		return attrValues;
	}

	public String[] getRecordAttributeValue(String[] attrNames,
			RecordInfo aRecordInfo)  throws MetadataReadException {
		String[] attrValues = new String[attrNames.length];
		Arrays.fill(attrValues, "");
		return attrValues;
	}

	public String[] getWriteOpDesigntimeAttribValues(String[] attrNames,
			RecordInfo primaryRecord, List<RecordInfo> secondaryRecords)
			throws MetadataReadException {
		String[] retVal = new String[attrNames.length];
		Arrays.fill(retVal, "");
		return retVal;
	}

}
