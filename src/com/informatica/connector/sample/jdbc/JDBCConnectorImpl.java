package com.informatica.connector.sample.jdbc;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLClassLoader;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.ini4j.Ini;
import org.ini4j.InvalidFileFormatException;
import org.ini4j.Profile.Section;

import com.informatica.cloud.api.adapter.common.OperationContext;
import com.informatica.cloud.api.adapter.connection.ConnectionFailedException;
import com.informatica.cloud.api.adapter.connection.InsufficientConnectInfoException;
import com.informatica.cloud.api.adapter.metadata.AdvancedFilterInfo;
import com.informatica.cloud.api.adapter.metadata.Field;
import com.informatica.cloud.api.adapter.metadata.FieldAttribute;
import com.informatica.cloud.api.adapter.metadata.FilterInfo;
import com.informatica.cloud.api.adapter.metadata.MetadataReadException;
import com.informatica.cloud.api.adapter.metadata.RecordAttribute;
import com.informatica.cloud.api.adapter.metadata.RecordAttributeScope;
import com.informatica.cloud.api.adapter.metadata.RecordInfo;
import com.informatica.cloud.api.adapter.runtime.exception.DataConversionException;
import com.informatica.cloud.api.adapter.runtime.exception.FatalRuntimeException;
import com.informatica.cloud.api.adapter.runtime.exception.ReadException;
import com.informatica.cloud.api.adapter.runtime.exception.ReflectiveOperationException;
import com.informatica.cloud.api.adapter.runtime.exception.WriteException;
import com.informatica.cloud.api.adapter.runtime.utils.IInputDataBuffer;
import com.informatica.cloud.api.adapter.runtime.utils.IOutputDataBuffer;
import com.informatica.cloud.api.adapter.runtime.utils.OperationResult;
import com.informatica.connector.sample.jdbc.util.JDBCUtils;
import com.informatica.connector.wrapper.util.BackendFieldInfo;
import com.informatica.connector.wrapper.util.BackendObjectInfo;
import com.informatica.connector.wrapper.util.ConnectionProperty;
import com.informatica.connector.wrapper.util.ConnectorUtils;
import com.informatica.connector.wrapper.util.ISimpleConnector;
import com.informatica.connector.wrapper.util.WriteOperation;

public class JDBCConnectorImpl extends ISimpleConnector {

	private static final Log logger = LogFactory
			.getLog(JDBCConnectorImpl.class);

	// TODO - 2.1 INFA: Define connection attributes, remember to generate
	// getter and setters

	public String username = null;
	public String password = null;
	public String connectionUrl = null;


	@ConnectionProperty(label = "Schema", required = true)
	public String schema = null;

	@ConnectionProperty(label = "Jdbc Driver", required = true)
	public String jdbcDriverName = null;

	/**
	 * because we cant ship jdbc drivers with the connector (legal and logistic
	 * reason) we are asking the user to specify their JDBC driver folder. We
	 * will add the jdbc drivers jars from the folder to the classloader.
	 * 
	 * This is generally not required. Most connectors ship with their jars in
	 * the lib folder
	 * 
	 */
	@ConnectionProperty(label = "Jdbc Driver Folder", required = true)
	public String jdbcDriverFolder = null;

	@ConnectionProperty(label = "Identifier Quote", required = false)
	public String identifierQuoteString = "DEFAULT";

	@ConnectionProperty(label = "Max Column Size", required = false)
	public int maxColumnSize = 1024 * 32;

	private Connection jdbcConnection = null;

	private int batchsize = 0;

	public String getUserName() {
		return username;
	}

	public void setUserName(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getConnectionUrl() {
		if (this.connectionUrl != null && !this.connectionUrl.isEmpty()) {
			if (this.connectionUrl.indexOf('%') != 1) {
				try {
					this.connectionUrl = URLDecoder.decode(connectionUrl,
							"UTF-8");
				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				}
			}

		}
		return this.connectionUrl;
	}

	private String readFromIni() {
		String absolutePath = null;
		String url = null;
		Ini ini = new Ini();
		try {
			absolutePath = this.getClass().getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
			absolutePath = absolutePath.substring(0, absolutePath.lastIndexOf("/"));
			ini.load(new FileReader(absolutePath + "/jdbc.ini"));
			url = ini.get("URL", "urlOptions");
			try{
				this.batchsize  = Integer.parseInt(ini.get("URL", "batchSize"));				
			} catch(Exception e){
				this.batchsize = 0;
			}
			System.out.println("absolute ini path="+absolutePath + "/jdbc.ini");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return url;
	}

	public void setConnectionUrl(String connectionUrl) {
		this.connectionUrl = connectionUrl;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getSchema() {
		if (schema == null || schema.isEmpty())
			return null;
		return schema;
	}

	public void setSchema(String schema) {
		this.schema = schema;
	}

	public String getJdbcDriverName() {
		return jdbcDriverName;
	}

	public void setJdbcDriverName(String jdbcDriverName) {
		this.jdbcDriverName = jdbcDriverName;
	}

	/**
	 * because we cant ship jdbc drivers with the connector (legal and logistic
	 * reason) we are asking the user to specify their JDBC driver folder. We
	 * will add the jdbc drivers jars from the folder to the classloader.
	 * 
	 * This is generally not required. Most connectors ship with their jars in
	 * the lib folder
	 * 
	 */
	public String getJdbcDriverFolder() {
		if (jdbcDriverFolder == null || jdbcDriverFolder.isEmpty())
			return null;
		return jdbcDriverFolder;
	}

	public void setJdbcDriverFolder(String jdbcDriverFolder) {
		this.jdbcDriverFolder = jdbcDriverFolder;
	}

	public String getIdentifierQuoteString() {
		if (this.identifierQuoteString == null
				|| identifierQuoteString.equalsIgnoreCase("DEFAULT")) {
			if (this.jdbcConnection != null) {
				try {
					this.identifierQuoteString = jdbcConnection.getMetaData()
							.getIdentifierQuoteString();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			} else {
				this.identifierQuoteString = "\"";
			}
		}
		return this.identifierQuoteString;
	}

	public void setIdentifierQuoteString(String identifierQuoteString) {
		this.identifierQuoteString = identifierQuoteString;
	}

	public int getMaxColumnSize() {
		if (maxColumnSize < 0)
			this.maxColumnSize = 1024 * 32;
		return this.maxColumnSize;
	}

	public void setMaxColumnSize(int maxColumnSize) {
		this.maxColumnSize = maxColumnSize;
	}

	public JDBCConnectorImpl() {
		logger.debug("JDBCConnectorImpl()");
		try {
			// addDirectoryToClassLoader(new
			// File("C:\\Program Files (x86)\\Informatica Cloud Secure Agent\\jdbc"));
			if (logger.isTraceEnabled()) {
				logger.trace("java.library.path: "
						+ System.getProperty("java.library.path"));
				logger.trace("user.dir: " + System.getProperty("user.dir"));

				// java.util.Properties props = System.getProperties();
				// props.list(System.out);

				logger.trace("java.class.path: "
						+ System.getProperty("java.class.path"));
				logger.trace("SystemClassLoader Path: "
						+ ConnectorUtils
								.getClassLoaderClassPath((URLClassLoader) ClassLoader
										.getSystemClassLoader()));
				logger.trace("ContextClassLoader Path: "
						+ ConnectorUtils
								.getClassLoaderClassPath((URLClassLoader) Thread
										.currentThread()
										.getContextClassLoader()));
				logger.trace("JDBCConnectorImpl ClassLoader Path: "
						+ ConnectorUtils
								.getClassLoaderClassPath((URLClassLoader) this
										.getClass().getClassLoader()));
			}
		} catch (Throwable t) {
			t.printStackTrace();
		}
	}

	// Begin Connector Implementation

	@Override
	public UUID getPluginUUID() {
		logger.debug("JDBCConnectorImpl.getPluginUUID()");
		return UUID.fromString(JDBCConnectorConstants.PLUGIN_UUID);
		// return
		// UUID.nameUUIDFromBytes(JDBCConnectorImpl.class.getCanonicalName().getBytes());
	}

	// TODO - 2.3 INFA: Connect to your application and validate connection info
	@Override
	public boolean connect() throws InsufficientConnectInfoException,
			ConnectionFailedException {
		logger.debug("JDBCConnectorImpl.connect()");
		if (this.getJdbcDriverFolder() != null
				&& !this.getJdbcDriverFolder().isEmpty()) {
			/**
			 * because we cant ship jdbc drivers with the connector (legal and
			 * logistic reason) we are asking the user to specify their JDBC
			 * driver folder. We will add the jdbc drivers jars from the folder
			 * to the classloader.
			 * 
			 * This is generally not required. Most connectors ship with their
			 * jars in the lib folder
			 * 
			 */
			ConnectorUtils.addDirectoryToClasspath(
					new File(this.getJdbcDriverFolder()), (URLClassLoader) this
							.getClass().getClassLoader());
		}
		String connOptions = readFromIni();
		System.out.println("connOptions=" + connOptions);
		if (connOptions != null && !connOptions.isEmpty()) {
			this.connectionUrl = connOptions;
		} else{
			connOptions = this.getConnectionUrl();
		}


		this.jdbcConnection = JDBCUtils.validateAndConnect(
				this.getJdbcDriverName(), connOptions,
				this.getUsername(), this.getPassword(), this.getSchema(),
				this.getMaxColumnSize());

		this.getIdentifierQuoteString();

		return true;
	}

	@Override
	public boolean disconnect() {
		logger.debug("JDBCConnectorImpl.disconnect()");
		// TODO - 2.4 INFA: Disconnect from your application and do additional
		// cleanup
		boolean status = JDBCUtils.close(jdbcConnection);
		this.jdbcConnection = null;
		return status;
	}

	@Override
	public List<BackendObjectInfo> getObjectList(Pattern pattern,
			OperationContext operationContext) throws MetadataReadException {
		logger.debug("JDBCConnectorImpl.getObjectList(\"" + pattern + "\")");
		// TODO - 3.1 INFA: Get Object List using your application API
		List<BackendObjectInfo> files = new ArrayList<BackendObjectInfo>();
		if(!(operationContext.name().equals(OperationContext.TRANSFORM))) {
		files = JDBCUtils.getObjectList(this.jdbcConnection, pattern,
				this.getSchema(), this.getIdentifierQuoteString());
		}
		return files;
		// ENDTODO
	}

	/*
	 * This methods gets called when a user selects multiple source option in
	 * the UI and click on the siblings button. This method returns a list of
	 * all related objects for the object in the primaryRecordInfo parameter
	 */
	@Override
	public List<BackendObjectInfo> getRelatedObjectList(
			RecordInfo primaryRecordInfo, OperationContext operationContext)
			throws MetadataReadException {
		return JDBCUtils.getRelatedObjectList(this.jdbcConnection,
				primaryRecordInfo, operationContext == OperationContext.WRITE);
	}

	// This method is returns a list of all fields contained in the input Record
	// (recordInfo)
	// If your Record contains fields with special types then you must add code
	// in getAdjustedDataType() to handle the special field types
	@Override
	public List<BackendFieldInfo> getFieldList(RecordInfo recordInfo,
			OperationContext operationContext) throws MetadataReadException {
		logger.debug("JDBCConnectorImpl.getFieldList(\""
				+ recordInfo.getCatalogName() + "\")");
		// TODO - 3.2 INFA: Get Fields for the object primaryRecord
		List<BackendFieldInfo> backendFieldInfos = JDBCUtils.getFieldList(
				this.jdbcConnection, recordInfo,
				(operationContext == OperationContext.WRITE),
				this.getIdentifierQuoteString(), this.getMaxColumnSize());
		return backendFieldInfos;
		// ENDTODO
	}

	@Override
	public boolean read(IOutputDataBuffer dataBufferInstance,
			List<Field> fieldList, RecordInfo recordInfo,
			List<RecordInfo> relatedRecordInfoList,
			List<FilterInfo> filterInfoList,
			AdvancedFilterInfo advancedFilterInfo, boolean isPreview,
			long pagesize,
			Map<RecordInfo, Map<String, String>> recordAttributes,
			Map<String, String> readOperationAttributes,
			List<RecordInfo> childRecordList, boolean isLookup)
			throws ConnectionFailedException, ReflectiveOperationException,
			ReadException, DataConversionException, FatalRuntimeException {
		logger.debug("JDBCConnectorImpl.read(\"" + recordInfo.getCatalogName()
				+ "\")");
		// TODO - 4.1 INFA: Read data from your application and populate the
		// buffer
		return JDBCUtils.read(dataBufferInstance, fieldList, recordInfo,
				relatedRecordInfoList, filterInfoList, advancedFilterInfo,
				isPreview, pagesize, recordAttributes, readOperationAttributes,
				childRecordList, isLookup, this.jdbcConnection,
				this.identifierQuoteString);
	}

	@Override
	public List<OperationResult> delete(IInputDataBuffer inputDataBuffer,
			List<Field> fieldList, RecordInfo recordInfo,
			Map<RecordInfo, Map<String, String>> recordAttributes,
			Map<String, String> operationAttributes,
			List<RecordInfo> secondaryRecordInfoList) throws WriteException,
			FatalRuntimeException, ConnectionFailedException,
			ReflectiveOperationException, DataConversionException,
			FatalRuntimeException {
		logger.debug("JDBCConnectorImpl.delete(\""
				+ recordInfo.getCatalogName() + "\")");
		// TODO - 5.2 INFA: (Optional) Delete data from your application
		return JDBCUtils.write(inputDataBuffer, fieldList, recordInfo,
				WriteOperation.DELETE, this.getIdentifierQuoteString(),
				recordAttributes, operationAttributes, secondaryRecordInfoList,
				this.jdbcConnection, batchsize);
	}

	@Override
	public List<OperationResult> write(IInputDataBuffer inputDataBuffer,
			List<Field> fieldList, RecordInfo recordInfo,
			WriteOperation writeOperation,
			Map<RecordInfo, Map<String, String>> recordAttributes,
			Map<String, String> operationAttributes,
			List<RecordInfo> secondaryRecordInfoList) throws WriteException,
			FatalRuntimeException, ConnectionFailedException,
			ReflectiveOperationException, DataConversionException,
			FatalRuntimeException {
		logger.debug("JDBCConnectorImpl.write(\"" + recordInfo.getCatalogName()
				+ "\")");
		// TODO - 5.1 INFA: Write to your application
		return JDBCUtils.write(inputDataBuffer, fieldList, recordInfo,
				writeOperation, getIdentifierQuoteString(),
				recordAttributes, operationAttributes, secondaryRecordInfoList,
				jdbcConnection, batchsize);
	}

	/*
	 * This method returns a list of custom field attributes
	 */
	@Override
	public List<FieldAttribute> getFieldAttributes() {
		ArrayList<FieldAttribute> listOfFieldAttrs = new ArrayList<FieldAttribute>();
		FieldAttribute fa1 = new FieldAttribute();
		fa1.setId(1);
		fa1.setName("SQL_DATA_TYPE_ID");
		listOfFieldAttrs.add(fa1);
		FieldAttribute fa2 = new FieldAttribute();
		fa2.setId(2);
		fa2.setName("DB_TYPE_NAME");
		listOfFieldAttrs.add(fa2);
		return listOfFieldAttrs;
	}

	/*
	 * This method returns a list of custom write attributes
	 */
	@Override
	public List<RecordAttribute> getWriteOperationAttributes() {
		ArrayList<RecordAttribute> writeOperationAttributeList = new ArrayList<RecordAttribute>();
		RecordAttribute rAttrib = new RecordAttribute(
				RecordAttributeScope.RUNTIME);
		rAttrib.setId(1);
		rAttrib.setName("PRIMARY KEY OVERRIDE");
		rAttrib.setDescription("Column names to use as the primary key, use commas to seperate multiple comma names");
		rAttrib.setDatatype("STRING");
		rAttrib.setDefaultValue("");
		rAttrib.setClientVisible(true);
		rAttrib.setClientEditable(true);
		rAttrib.setMaxLength("255");
		writeOperationAttributeList.add(rAttrib);
		return writeOperationAttributeList;
	}

	/*
	 * This method returns custom record attributes
	 */
	@Override
	public List<RecordAttribute> getRecordAttributes() {
		ArrayList<RecordAttribute> recordAttributeList = new ArrayList<RecordAttribute>();
		/*
		 * RecordAttribute rAttrib = new
		 * RecordAttribute(RecordAttributeScope.DESIGNTIME); rAttrib.setId(1);
		 * rAttrib.setName("PRIMARY KEY OVERRIDE"); rAttrib.setDescription(
		 * "Column names to use as the primary key, use commas to seperate multiple comma names"
		 * ); rAttrib.setDatatype("STRING"); rAttrib.setDefaultValue("");
		 * rAttrib.setClientVisible(true); rAttrib.setClientEditable(true);
		 * rAttrib.setMaxLength("255"); recordAttributeList.add(rAttrib);
		 */
		return recordAttributeList;
	}

	@Override
	public void flush() {
		JDBCUtils.flush();
		
	}

}
