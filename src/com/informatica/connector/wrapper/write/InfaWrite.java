package com.informatica.connector.wrapper.write;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.informatica.cloud.api.adapter.annotation.Deinit;
import com.informatica.cloud.api.adapter.common.ILogger;
import com.informatica.cloud.api.adapter.connection.ConnectionFailedException;
import com.informatica.cloud.api.adapter.metadata.Field;
import com.informatica.cloud.api.adapter.metadata.MetadataReadException;
import com.informatica.cloud.api.adapter.metadata.RecordInfo;
import com.informatica.cloud.api.adapter.plugin.PluginVersion;
import com.informatica.cloud.api.adapter.runtime.IWrite;
import com.informatica.cloud.api.adapter.runtime.exception.DataConversionException;
import com.informatica.cloud.api.adapter.runtime.exception.FatalRuntimeException;
import com.informatica.cloud.api.adapter.runtime.exception.InitializationException;
import com.informatica.cloud.api.adapter.runtime.exception.ReflectiveOperationException;
import com.informatica.cloud.api.adapter.runtime.exception.WriteException;
import com.informatica.cloud.api.adapter.runtime.utils.IInputDataBuffer;
import com.informatica.cloud.api.adapter.runtime.utils.OperationResult;
import com.informatica.connector.wrapper.connection.InfaConnection;
import com.informatica.connector.wrapper.plugin.InfaPlugin;
import com.informatica.connector.wrapper.util.WriteOperation;

public class InfaWrite implements IWrite {

	private static final Log logger = LogFactory.getLog(InfaWrite.class);

	private final InfaPlugin plugin;
	private final InfaConnection connection;
	private final ILogger iLogger;
	private PluginVersion metadataVersion;	
	private RecordInfo primaryRecordInfo;
	private List<RecordInfo> childRecordList = new ArrayList<RecordInfo>();
	private List<Field> fieldList = new ArrayList<Field>();
	private Map<RecordInfo,Map<String, String>> recordAttributes = new HashMap<RecordInfo, Map<String,String>>();
	private Map<String, String> operationAttributes = new HashMap<String, String>();
	
	public InfaWrite(InfaPlugin iPlugin, InfaConnection conn) {
		this.plugin = iPlugin;
		this.connection = conn;
		this.iLogger = this.plugin.getLogger();
	}

	public ILogger getiLogger() {
		return iLogger;
	}
	

	@Override
	public void initializeAndValidate() throws InitializationException {
		logger.debug("InfaWrite.initializeAndValidate()");
		try{
			this.connection.getConnectorImpl().initializeAndValidateWriter(this.fieldList,this.primaryRecordInfo, WriteOperation.INSERT, this.recordAttributes,this.operationAttributes,this.childRecordList);
		} catch (Throwable t) {
			t.printStackTrace();
			throw new InitializationException(t.toString());			
		}				
	}

	@Override
	public void setChildRecords(List<RecordInfo> secondaryRecordInfoList) {
		logger.debug("InfaWrite.setChildRecords(\"" + secondaryRecordInfoList + "\")");		
		this.childRecordList.clear();
		if(secondaryRecordInfoList != null && secondaryRecordInfoList.size() != 0)
		{
			this.childRecordList.addAll(secondaryRecordInfoList);
		}
	}

	@Override
	public void setFieldList(List<Field> fieldList) {
		logger.debug("InfaWrite.setFieldList(\"" + fieldList + "\")");		
		this.fieldList.clear();
		this.fieldList.addAll(fieldList);
	}
	

	@Override
	public void setMetadataVersion(PluginVersion pluginVersion) {
		logger.debug("InfaWrite.setMetadataVersion(\"" + pluginVersion + "\")");
		this.metadataVersion = pluginVersion;
	}

	public PluginVersion getMetadataVersion() {
		return metadataVersion;
	}	
	
	@Override
	public void setOperationAttributes(Map<String, String> runtimeAttribs) {
		logger.debug("InfaWrite.setOperationAttributes(\"" + runtimeAttribs + "\")");		
		this.operationAttributes.clear();
		this.operationAttributes.putAll(runtimeAttribs);
	}

	@Override
	public void setPrimaryRecord(RecordInfo primaryRecordInfo) {
		logger.debug("InfaWrite.setPrimaryRecord(\"" + primaryRecordInfo.getRecordName()+ "\")");		
		this.primaryRecordInfo = primaryRecordInfo;
	}

	@Override
	public void setRecordAttributes(RecordInfo recordInfo, Map<String, String> recordAttributes) {
		logger.debug("InfaWrite.setRecordAttributes(\"" + recordInfo.getRecordName()+ "\")");		
		this.recordAttributes.put(recordInfo , recordAttributes);
	}

	@Override
	public List<OperationResult> insert(IInputDataBuffer inputDataBuffer)
			throws ConnectionFailedException, ReflectiveOperationException,
			WriteException, DataConversionException, FatalRuntimeException {
		try{
    		if(inputDataBuffer==null || primaryRecordInfo == null)
    			return 	new ArrayList<OperationResult>();

			logger.debug("InfaWrite.insert(\"" + primaryRecordInfo.getCatalogName() + "\")");		


    		if(this.fieldList==null || this.fieldList.isEmpty())
				throw new FatalRuntimeException("At least one field should be mapped to Target");
			
			//This is work around to fix a bug in the INFA_JUnit test suite
			if(this.primaryRecordInfo.getCatalogName() == null)
			{
				try {
					this.primaryRecordInfo = this.connection.getConnectorImpl().getSelectRecordInfo(this.primaryRecordInfo.getRecordName());
				} catch (MetadataReadException e) {
					e.printStackTrace();
				}
			}							
			return this.connection.getConnectorImpl().write(inputDataBuffer,this.fieldList,this.primaryRecordInfo, WriteOperation.INSERT, this.recordAttributes,this.operationAttributes,this.childRecordList);
		} catch (RuntimeException t) {
			t.printStackTrace();
			FatalRuntimeException ex =  new FatalRuntimeException(t.toString());
			ex.initCause(t);
			throw ex;			
		}		
	}
	
	@Override
	public List<OperationResult> delete(IInputDataBuffer inputDataBuffer)
			throws ConnectionFailedException, ReflectiveOperationException,
			WriteException, DataConversionException, FatalRuntimeException {
		try{
			logger.debug("InfaWrite.delete("+primaryRecordInfo.getCatalogName()+")");

    		if(inputDataBuffer==null)
    			return 	new ArrayList<OperationResult>();

    		if(fieldList==null || fieldList.isEmpty())
				throw new FatalRuntimeException("At least one field should be mapped to Target");
			
			//This is work around to fix a bug in the INFA_JUnit test suite
			if(primaryRecordInfo.getCatalogName() == null)
			{
				try {
					primaryRecordInfo = this.connection.getConnectorImpl().getSelectRecordInfo(primaryRecordInfo.getRecordName());
				} catch (MetadataReadException e) {
					e.printStackTrace();
				}
			}							
			return this.connection.getConnectorImpl().delete(inputDataBuffer,this.fieldList,this.primaryRecordInfo,this.recordAttributes,this.operationAttributes,this.childRecordList);
		} catch (RuntimeException t) {
			t.printStackTrace();
			FatalRuntimeException ex =  new FatalRuntimeException(t.toString());
			ex.initCause(t);
			throw ex;			
		}		
	}

	
	@Override
	public List<OperationResult> update(IInputDataBuffer inputDataBuffer)
			throws ConnectionFailedException, ReflectiveOperationException,
			WriteException, DataConversionException, FatalRuntimeException {
		try{
			logger.debug("InfaWrite.update(\"" +primaryRecordInfo.getCatalogName()+ "\")");		

    		if(inputDataBuffer==null)
    			return 	new ArrayList<OperationResult>();

    		if(fieldList==null || fieldList.isEmpty())
				throw new FatalRuntimeException("At least one field should be mapped to Target");
			
			//This is work around to fix a bug in the INFA_JUnit test suite
			if(primaryRecordInfo.getCatalogName() == null)
			{
				try {
					primaryRecordInfo = this.connection.getConnectorImpl().getSelectRecordInfo(primaryRecordInfo.getRecordName());
				} catch (MetadataReadException e) {
					e.printStackTrace();
				}
			}										
			return this.connection.getConnectorImpl().write(inputDataBuffer,this.fieldList,primaryRecordInfo, WriteOperation.UPDATE,this.recordAttributes,this.operationAttributes,this.childRecordList);
		} catch (RuntimeException t) {
			t.printStackTrace();
			FatalRuntimeException ex =  new FatalRuntimeException(t.toString());
			ex.initCause(t);
			throw ex;			
		}		
	}

	@Override
	public List<OperationResult> upsert(IInputDataBuffer inputDataBuffer)
			throws ConnectionFailedException, ReflectiveOperationException,
			WriteException, DataConversionException, FatalRuntimeException {
		logger.debug("InfaWrite.upsert(\"" + primaryRecordInfo.getCatalogName() + "\")");		
		try{

    		if(inputDataBuffer==null)
    			return 	new ArrayList<OperationResult>();

    		if(fieldList==null || fieldList.isEmpty())
				throw new FatalRuntimeException("At least one field should be mapped to Target");
			
			
			//This is work around to fix a bug in the INFA_JUnit test suite
			if(primaryRecordInfo.getCatalogName() == null)
			{
				try {
					primaryRecordInfo = this.connection.getConnectorImpl().getSelectRecordInfo(primaryRecordInfo.getRecordName());
				} catch (MetadataReadException e) {
					e.printStackTrace();
				}
			}										
			return this.connection.getConnectorImpl().write(inputDataBuffer,this.fieldList,primaryRecordInfo,WriteOperation.UPSERT,this.recordAttributes,this.operationAttributes,this.childRecordList);
		} catch (RuntimeException t) {
			t.printStackTrace();
			FatalRuntimeException ex =  new FatalRuntimeException(t.toString());
			ex.initCause(t);
			throw ex;			
		}		
	}	  				 		
	
	@Deinit
	public void flush(){
		connection.getConnectorImpl().flush();
	}
	
		 	 
}
