package com.informatica.connector.wrapper.read;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.informatica.cloud.api.adapter.common.ILogger;
import com.informatica.cloud.api.adapter.connection.ConnectionFailedException;
import com.informatica.cloud.api.adapter.metadata.AdvancedFilterInfo;
import com.informatica.cloud.api.adapter.metadata.Field;
import com.informatica.cloud.api.adapter.metadata.FilterInfo;
import com.informatica.cloud.api.adapter.metadata.MetadataReadException;
import com.informatica.cloud.api.adapter.metadata.RecordInfo;
import com.informatica.cloud.api.adapter.plugin.PluginVersion;
import com.informatica.cloud.api.adapter.runtime.IRead;
import com.informatica.cloud.api.adapter.runtime.exception.DataConversionException;
import com.informatica.cloud.api.adapter.runtime.exception.FatalRuntimeException;
import com.informatica.cloud.api.adapter.runtime.exception.InitializationException;
import com.informatica.cloud.api.adapter.runtime.exception.ReadException;
import com.informatica.cloud.api.adapter.runtime.exception.ReflectiveOperationException;
import com.informatica.cloud.api.adapter.runtime.utils.IOutputDataBuffer;
//import com.informatica.cloud.api.adapter.transform.runtime.LookupOutputDataBufferImpl;
import com.informatica.connector.wrapper.connection.InfaConnection;
import com.informatica.connector.wrapper.plugin.InfaPlugin;

public class InfaRead implements IRead {

	private static final Log logger = LogFactory.getLog(InfaRead.class);

	private final InfaPlugin plugin;
	private final InfaConnection connection;

	private final ILogger iLogger;
	public ILogger getiLogger() {
		return iLogger;
	}				
	
	private PluginVersion metadataVersion;
	public PluginVersion getMetadataVersion() {
		return metadataVersion;
	}	

	private RecordInfo primaryRecordInfo;
	private List<RecordInfo> relatedRecordList = new ArrayList<RecordInfo>();
	private List<RecordInfo> childRecordList = new ArrayList<RecordInfo>();
	private List<Field> fieldList = new ArrayList<Field>();
	private List<FilterInfo> filterInfoList = new ArrayList<FilterInfo>();
	private AdvancedFilterInfo advancedFilterInfo;
	private Map<RecordInfo,Map<String, String>> recordAttributes = new HashMap<RecordInfo, Map<String,String>>();
	private Map<String, String> readOperationAttributes = new HashMap<String, String>();

	
	public InfaRead(InfaPlugin plugin, InfaConnection conn) {
		this.plugin = plugin;
		this.connection = conn;
		this.iLogger = this.plugin.getLogger();		
		this.advancedFilterInfo = new AdvancedFilterInfo();
	}
	
	@Override
	public void initializeAndValidate() throws InitializationException {
		logger.debug("InfaRead.initializeAndValidate()");
		try {
			this.connection.getConnectorImpl().initializeAndValidateReader(this.fieldList,this.primaryRecordInfo,this.relatedRecordList,this.filterInfoList,this.advancedFilterInfo,this.recordAttributes,this.readOperationAttributes,this.childRecordList);
		} catch (Throwable e) {
			e.printStackTrace();
			throw new InitializationException(e.toString());
		}
	}


	@Override
	public boolean read(IOutputDataBuffer dataBufferInstance) throws 
		ConnectionFailedException, ReflectiveOperationException, ReadException,
		DataConversionException, FatalRuntimeException 
	{
		try 
		{
			logger.debug("InfaRead.read("+primaryRecordInfo.getCatalogName()+")");

			if(dataBufferInstance==null)
				return false;
			else
			{
				boolean isLookup = false;
			/*	if(dataBufferInstance instanceof LookupOutputDataBufferImpl)
				{
					isLookup = true;
				}*/
				
				//This is work around to fix a bug in the INFA_JUnit test suite
				if(primaryRecordInfo.getCatalogName() == null)
				{
					try {
						primaryRecordInfo = this.connection.getConnectorImpl().getSelectRecordInfo(primaryRecordInfo.getRecordName());
					} catch (MetadataReadException e) {
						e.printStackTrace();
					}
				}				
				return this.connection.getConnectorImpl().read(dataBufferInstance,this.fieldList,this.primaryRecordInfo,this.relatedRecordList,this.filterInfoList,this.advancedFilterInfo, false, Long.MAX_VALUE,this.recordAttributes,this.readOperationAttributes,this.childRecordList, isLookup);
			}
		} catch (RuntimeException t) {
			t.printStackTrace();
			FatalRuntimeException ex =  new FatalRuntimeException(t.toString());
			ex.initCause(t);
			throw ex;			
		}		
	}

	@Override
	public void setAdvancedFilters(AdvancedFilterInfo advancedFilterInfo) {
		logger.debug("InfaRead.setAdvancedFilters("+advancedFilterInfo+")");
		this.advancedFilterInfo = advancedFilterInfo;		
	}

	@Override
	public void setChildRecords(List<RecordInfo> childRecords) {
		logger.debug("InfaRead.setChildRecords("+childRecords+")");
		this.childRecordList = childRecords;
	}	

	@Override
	public void setFieldList(List<Field> fieldList) {
		logger.debug("InfaRead.setFieldList(fieldList.size="+fieldList.size()+")");
		this.fieldList.clear();
		this.fieldList.addAll(fieldList);
	}
	
	@Override
	public void setFilters(List<FilterInfo> filterInfoList) {
		logger.debug("InfaRead.setFilters("+filterInfoList+")");
		this.filterInfoList.clear();
		if(filterInfoList != null && filterInfoList.size() != 0)
			this.filterInfoList.addAll(filterInfoList);
	}

	@Override
	public void setMetadataVersion(PluginVersion pluginVersion) {
		logger.debug("InfaRead.setMetadataVersion("+pluginVersion.getBuild()+")");
		this.metadataVersion = pluginVersion;		
	}

	@Override
	public void setOperationAttributes(Map<String, String> roAttribs) {
		logger.debug("InfaRead.setOperationAttributes("+roAttribs+")");
		this.readOperationAttributes.clear();
		this.readOperationAttributes.putAll(roAttribs);
	}

	@Override
	public void setPrimaryRecord(RecordInfo primaryRecordInfo) {
		logger.debug("InfaRead.setPrimaryRecord("+primaryRecordInfo.getCatalogName()+")");
		this.primaryRecordInfo = primaryRecordInfo;
	}

	@Override
	public void setRecordAttributes(RecordInfo recordInfo, Map<String, String> srcDesigntimeAttribs) {
		logger.debug("InfaRead.setRecordAttributes("+srcDesigntimeAttribs+")");
		this.recordAttributes.put(recordInfo, srcDesigntimeAttribs);		
	}

	@Override
	public void setRelatedRecords(List<RecordInfo> relatedRecordInfoList) {
		logger.debug("InfaRead.setRelatedRecords("+relatedRecordInfoList+")");
		this.relatedRecordList.clear();
		if(relatedRecordInfoList != null && relatedRecordInfoList.size() != 0)
			this.relatedRecordList.addAll(relatedRecordInfoList);
	}

    
}

