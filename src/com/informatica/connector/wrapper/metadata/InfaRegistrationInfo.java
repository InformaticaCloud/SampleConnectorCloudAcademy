package com.informatica.connector.wrapper.metadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import com.informatica.cloud.api.adapter.connection.ConnectionAttribute;
import com.informatica.cloud.api.adapter.metadata.FieldAttribute;
import com.informatica.cloud.api.adapter.metadata.IRegistrationInfo;
import com.informatica.cloud.api.adapter.metadata.MetadataReadException;
import com.informatica.cloud.api.adapter.metadata.RecordAttribute;
import com.informatica.cloud.api.adapter.metadata.RecordAttributeScope;
import com.informatica.cloud.api.adapter.metadata.TransformationInfo;
import com.informatica.cloud.api.adapter.typesystem.ITypeSystem;
import com.informatica.connector.wrapper.typesystem.InfaTypeSystem;
import com.informatica.connector.wrapper.util.ConnectionAttributeUtil;
import com.informatica.connector.wrapper.util.ConnectorUtils;
import com.informatica.connector.wrapper.util.ISimpleConnector;


public class InfaRegistrationInfo implements IRegistrationInfo {

	private ISimpleConnector connectionImpl = null;	
	private String pluginShortName = null;
	private String pluginDescription = null;
	private String name = null;
	private List<ConnectionAttribute> connectionAttributeList = null;	
	private ITypeSystem typeSystemImpl = null;

	//private String pluginPropertyFile = "Plugin_Registration_Info.properties";
	
	public InfaRegistrationInfo(ISimpleConnector connectorImpl) {
		this.connectionImpl = connectorImpl;
		this.pluginShortName = connectorImpl.getClass().getSimpleName();
		this.pluginDescription = pluginShortName+" Connector";
		this.name = "PowerExchange for "+pluginShortName;
	}
	
	public void setName(String pluginName) {
		this.name = pluginName;
	}

	public void setPluginShortName(String pluginShortName) {
		this.pluginShortName = pluginShortName;
	}

	public void setPluginDescription(String pluginDescription) {
		this.pluginDescription = pluginDescription;
	}
		
	/*
	private void loadPluginInfo() throws InstantiationException, ClassNotFoundException {
		Properties pluginProperties = null;
		try {
			pluginProperties = ConnectorUtils.loadProperties(pluginPropertyFile);
			this.setConnectionImplClass(pluginProperties.getProperty("connectionImpl"));
			this.setPluginShortName(pluginProperties.getProperty("pluginShortName"));
			this.setName(pluginProperties.getProperty("pluginName"));
			this.setPluginDescription(pluginProperties.getProperty("pluginDescription"));
			this.setPluginUUID(pluginProperties.getProperty("pluginUUID"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			pluginProperties = new Properties();
			pluginProperties.put("connectionImpl", connectionImpl.getCanonicalName());
			pluginProperties.put("pluginShortName", pluginShortName);
			pluginProperties.put("pluginName", name);	
			pluginProperties.put("pluginDescription", pluginDescription);
			pluginProperties.put("pluginUUID", pluginUUID.toString());
			try {
				ConnectorUtils.saveProp(pluginProperties, pluginPropertyFile);
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			throw new InstantiationException("plugin.properties is not properly initilaized");
		}		
	}
*/

	@Override
	public String getPluginDescription() {
		return this.pluginDescription;
	}

	@Override
	public UUID getPluginUUID() {
		try
		{
			return this.connectionImpl.getPluginUUID();
		}catch(Throwable t)
		{
			t.printStackTrace();
			throw new IllegalArgumentException("getPluginUUID() threw an exception {"+t.toString()+"}");
		}
	}

	@Override
	public String getPluginShortName() {
		if(!ConnectorUtils.isLetterOrDigit(this.pluginShortName))
			throw new IllegalArgumentException("pluginShortName cannot contain spaces or special characters {"+pluginShortName+"}");
		 return this.pluginShortName; 
	}

	@Override
	public String getName() {
		return this.name;
	}

	@Override
	public ITypeSystem getTypeSystem() {
		if(typeSystemImpl == null){
			typeSystemImpl = new InfaTypeSystem();
		}
		return typeSystemImpl;
	}
		
	/*
	 *  This method returns a list of connection attributes
	 *
	 */
	@Override
	public List<ConnectionAttribute> getConnectionAttributes() {
		if(connectionAttributeList == null){
			connectionAttributeList = ConnectionAttributeUtil.getConnectionAttributes(connectionImpl);
		}
		return connectionAttributeList;
	}
	
	/*
	 *  This method returns custom record attributes 
	 */
	@Override
	public List<RecordAttribute> getRecordAttributes() throws MetadataReadException {
		try {
			return connectionImpl.getRecordAttributes();
		} catch (Throwable t) {
			t.printStackTrace();
			throw new MetadataReadException(t.toString());
		}
	}

	/*
	 *  This method returns a list of custom field attributes
	 */
	@Override
	public List<FieldAttribute> getFieldAttributes() throws MetadataReadException {
		try {
			return connectionImpl.getFieldAttributes();
		} catch (Throwable t) {
			t.printStackTrace();
			throw new MetadataReadException(t.toString());
		}
	}


	/*
	 *  This method returns the custom attributes for read operation 
	 */
	@Override
	public List<RecordAttribute> getReadOperationAttributes()  throws MetadataReadException {
		try {
			return connectionImpl.getReadOperationAttributes();
		} catch (Throwable t) {
			t.printStackTrace();
			throw new MetadataReadException(t.toString());			
		}
	}

	@Override
	public List<RecordAttribute> getWriteOperationAttributes()  throws MetadataReadException {
		List<RecordAttribute> listOfWriteOpAttribs = new ArrayList<RecordAttribute>();
		try {
			listOfWriteOpAttribs = connectionImpl.getWriteOperationAttributes();
		} catch (Throwable t) {
			t.printStackTrace();
		}
		if(listOfWriteOpAttribs.isEmpty())
		{
			RecordAttribute rAttrib = new RecordAttribute(RecordAttributeScope.RUNTIME);
			rAttrib.setId(1);
			rAttrib.setName("Loadmode");
			rAttrib.setDescription("How to load the Target");
			rAttrib.setDatatype("STRING");
			rAttrib.setDefaultValue("NORMAL");
			rAttrib.setListOfValues(Arrays.asList("NORMAL","BULK"));
			listOfWriteOpAttribs.add(rAttrib);
		}
		return listOfWriteOpAttribs;
	}
	
	@Override
	public List<TransformationInfo> getTransformationOperations() {
		List<TransformationInfo> listOfTransformationOperations = new ArrayList<TransformationInfo>();
		try {
			listOfTransformationOperations = connectionImpl.getTransformationOperations();
		} catch (Throwable t) {
			t.printStackTrace();
		}
		return listOfTransformationOperations;
	}
	
	@Override
	public List<RecordAttribute> getTransformationAttributes(TransformationInfo transform) throws MetadataReadException {
		try {
			return connectionImpl.getTransformationAttributes(transform);
		} catch (Throwable t) {
			t.printStackTrace();
			throw new MetadataReadException(t.toString());
		}
	}	


}
