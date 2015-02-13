package com.informatica.connector.sample.jdbc.util;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.informatica.cloud.api.adapter.connection.ConnectionFailedException;
import com.informatica.cloud.api.adapter.connection.InsufficientConnectInfoException;
import com.informatica.cloud.api.adapter.metadata.AdvancedFilterInfo;
import com.informatica.cloud.api.adapter.metadata.Field;
import com.informatica.cloud.api.adapter.metadata.FieldAttribute;
import com.informatica.cloud.api.adapter.metadata.FilterInfo;
import com.informatica.cloud.api.adapter.metadata.FilterOperation;
import com.informatica.cloud.api.adapter.metadata.MetadataReadException;
import com.informatica.cloud.api.adapter.metadata.RecordInfo;
import com.informatica.cloud.api.adapter.runtime.exception.DataConversionException;
import com.informatica.cloud.api.adapter.runtime.exception.FatalRuntimeException;
import com.informatica.cloud.api.adapter.runtime.exception.ReadException;
import com.informatica.cloud.api.adapter.runtime.exception.ReflectiveOperationException;
import com.informatica.cloud.api.adapter.runtime.exception.WriteException;
import com.informatica.cloud.api.adapter.runtime.utils.IInputDataBuffer;
import com.informatica.cloud.api.adapter.runtime.utils.IOutputDataBuffer;
import com.informatica.cloud.api.adapter.runtime.utils.OperationResult;
import com.informatica.connector.sample.jdbc.JDBCConnectorConstants;
import com.informatica.connector.wrapper.util.BackendFieldInfo;
import com.informatica.connector.wrapper.util.BackendObjectInfo;
import com.informatica.connector.wrapper.util.WriteOperation;

public class JDBCUtils {

	private static final Log logger = LogFactory.getLog(com.informatica.connector.sample.jdbc.util.JDBCUtils.class);
	
	//These are operators used by filter	
	private static String EQUALS_OPERATOR = "{0} = {1}";
	private static String NOT_EQUALS_OPERATOR = "{0} <> {1}";
	private static String LESS_THAN_OPERATOR = "{0} < {1}";
	private static String GREATER_THAN_OPERATOR = "{0} > {1}";
	private static String LESS_THAN_OR_EQUALS_OPERATOR = "{0} <= {1}";
	private static String GREATER_THAN_OR_EQUALS_OPERATOR = "{0} >= {1}";
	private static String IS_NULL_OPERATOR = "{0} IS NULL";
	private static String IS_NOT_NULL_OPERATOR = "{0} IS NOT NULL";

	private static int currentBatch = 0;
	private static Connection dbConnection;
	private static final String CONTAINS_OPERATOR = "{0} LIKE {1}";	//User will add the %
	private static final String STARTS_WITH_OPERATOR = "{0} LIKE {1}"; //User will add the %
	private static final String ENDS_WITH_OPERATOR = "{0} LIKE {1}"; //User will add the %
	
	private static final Map<String,String> filterOperatorMap = new HashMap<String,String>();

	private static PreparedStatement insert_pstmt;
	static {
		filterOperatorMap.put(FilterOperation.contains.name(), CONTAINS_OPERATOR);
		filterOperatorMap.put(FilterOperation.endsWith.name(), ENDS_WITH_OPERATOR);
		filterOperatorMap.put(FilterOperation.equals.name(), EQUALS_OPERATOR);
		filterOperatorMap.put(FilterOperation.greaterOrEquals.name(), GREATER_THAN_OR_EQUALS_OPERATOR);
		filterOperatorMap.put(FilterOperation.greaterThan.name(), GREATER_THAN_OPERATOR);
		filterOperatorMap.put(FilterOperation.isNotNull.name(), IS_NOT_NULL_OPERATOR);
		filterOperatorMap.put(FilterOperation.isNull.name(), IS_NULL_OPERATOR);
		filterOperatorMap.put(FilterOperation.lessOrEquals.name(), LESS_THAN_OR_EQUALS_OPERATOR);
		filterOperatorMap.put(FilterOperation.lessThan.name(), LESS_THAN_OPERATOR);
		filterOperatorMap.put(FilterOperation.notEquals.name(), NOT_EQUALS_OPERATOR);
		filterOperatorMap.put(FilterOperation.startsWith.name(), STARTS_WITH_OPERATOR);		
	};	
	

	/**
	 * Validate the connection attributes specified by user
	 * and connects to the to the application 
	 * (In this case opens a file and checks if it exists and is readable/writable)
	 * 
	 * @param jdbcDriverName
	 * @param jdbcUrl
	 * @param userName
	 * @param password
	 * @param schema
	 * @param maxColumnSize 
	 * @return Connection
	 * @throws InsufficientConnectInfoException
	 * @throws ConnectionFailedException
	 */
	public static Connection validateAndConnect(String jdbcDriverName,String jdbcUrl,String userName,String password, String schema, int maxColumnSize) 
			throws InsufficientConnectInfoException, ConnectionFailedException 
	{
		logger.debug("JDBCConnectorUtils.validateAndConnect("+jdbcDriverName+","+jdbcUrl+")");

		Connection jdbcConnection = null;
		
		if (jdbcDriverName == null) {
			String msg = "ERROR: 'Jdbc Driver' is a required connection parameter";
			logger.warn(msg);
			throw new InsufficientConnectInfoException(msg);
		}

		if (jdbcUrl == null) {
			String msg = "ERROR: 'Connection URL' is a required connection parameter";
			logger.warn(msg);
			throw new InsufficientConnectInfoException(msg);
		}

		if (maxColumnSize > 104857600) {
			String msg = "ERROR: 'Max Column Size' cannot be larger than {"+104857600+"}";
			logger.warn(msg);
			throw new InsufficientConnectInfoException(msg);
		}

		try {
			logger.info("jdbcUrl: "+ jdbcUrl);
			logger.info("jdbcDriverName: "+ jdbcDriverName);
			Class.forName(jdbcDriverName);
			jdbcConnection = DriverManager.getConnection(jdbcUrl, userName, password);
		} catch (SQLException e) {
			e.printStackTrace();
			throw new ConnectionFailedException(e.toString());
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
			throw new ConnectionFailedException(e1.toString());
		}
		
		logger.info(aboutDriverandDatabase(jdbcConnection));
		return jdbcConnection;
	}

	
	
	public static boolean close(Connection jdbcConnection) {

		if (jdbcConnection == null)
			return false;

		try {
			jdbcConnection.close();
			return true;
		} catch (SQLException e) {
			logger.warn("An unexpected error occurred while "
					+ "closing the JDBC Connection", e);
			return false;
		}
	}
	
	
	/**
	 * This methods returns the object list (Directories)
	 * The Object List is what shows in the Object dropdown in the cloud UI Wizard (source/target)
	 * 
	 * @param identifierQuoteString 
	 * @param basicDataHandler 
	 * @return List<BackendObjectInfo>
	 * @throws MetadataReadException 
	 */
	public static List<BackendObjectInfo> getObjectList(Connection jdbcConnection, Pattern pattern, String Schema, String identifierQuoteString) throws MetadataReadException 
	{
		logger.debug("JDBC.getObjectList("+Schema+","+pattern+")");
		List<BackendObjectInfo> backendObjectInfoList = new ArrayList<BackendObjectInfo>();
		try{	
		
			String tableNamePattern = null;
			if(pattern != null && !pattern.pattern().equals(".*"))
				tableNamePattern = pattern.pattern();
				
			Collection<JDBCTable> tables = getTables(jdbcConnection,null,Schema, tableNamePattern, null);
			for(JDBCTable ts : tables)
			{
				if(ts.type.equalsIgnoreCase("INDEX"))
					continue;

				if(ts.type.equalsIgnoreCase("SEQUENCE"))
					continue;

			/**
			 *  Method: BackendObjectInfo(String objectCanonicalName, String objectName, String objectLabel)
			 *
			 *	Parameters:
			 *	objectCanonicalName: The fully qualified name string of the object
			 *	objectName: The object name string, cannot contain any spaces or special characters
			 *	objectLabel: This is the business name of the object
			 */
			BackendObjectInfo info = new BackendObjectInfo(ts.fullyQualifiedName,JDBCConnectorConstants.sanitizeName(ts.name),JDBCConnectorConstants.sanitizeName(ts.name));
			backendObjectInfoList.add(info);
		}	
		
	} catch (Throwable t) {
		t.printStackTrace();
		throw new  MetadataReadException(t.toString());
	} finally {
	}

		return backendObjectInfoList;
	}

	

	/**
	 * Gets a list of all Objects related to input Object from Salesforce
	 * 
	 * @param partnerConnection
	 *            Salesforce connection
	 * @param primary
	 *            record The record to get the related objects for
	 * @param isWrite
	 *            Flag to determine if this methods is being called in source or
	 *            target
	 * @return List of Salesforce Objects that are related to the input primary
	 *         Record
	 * @throws MetadataReadException
	 */
	public static List<BackendObjectInfo> getRelatedObjectList(
			Connection jdbcConnection, RecordInfo primaryRecordInfo, boolean isWrite) throws MetadataReadException {

		logger.debug("JDBCUtils.getRelatedObjectList(\""+ primaryRecordInfo.getCatalogName() + "\")");

		// These debug statements should help you understand what is being
		// passed back to your calls. You can comment these out if you like
		logger.info("***");
		logger.info("primaryRecordInfo.getRecordName: "+primaryRecordInfo.getRecordName());
		logger.info("primaryRecordInfo.getCatalogName: "+primaryRecordInfo.getCatalogName());
		logger.info("primaryRecordInfo.getLabel: "+primaryRecordInfo.getLabel());
		logger.info("primaryRecordInfo.getInstanceName: "+primaryRecordInfo.getInstanceName());
		logger.info("primaryRecordInfo.getRecordType: "+primaryRecordInfo.getRecordType());
		logger.info("---");

		
		List<BackendObjectInfo> backendObjectInfoList = new ArrayList<BackendObjectInfo>();
		
		//TODO GET RELATED TABLES based on FOREIGN KEYS
		
		return backendObjectInfoList;
	}



	
	/**
	 * When the user selects an object from the source/target dropdown this
	 * method is called to get fields in the selected Object. The Selected
	 * Object Name and CanonicalName is passed in the RecordInfo object
	 * 
	 * @param recordInfo
	 *            The selected Object from the dropdown
	 * @param partnerConnection
	 *            The connection to use for getting Object metadata
	 * @param isWrite
	 *            Flag to determine if this methods is being called in source or
	 *            target
	 * @return List of Fields in the object
	 * @throws MetadataReadException
	 */
	public static List<BackendFieldInfo> getFieldList(
			Connection jdbcConnection, RecordInfo recordInfo, boolean isWrite,
			String identifierQuoteString, int maxColumnSize) throws MetadataReadException 
	{
		try 
		{
			logger.debug("JDBCUtils.getFieldList(\""+ recordInfo.getCatalogName() + "\")");

			List<BackendFieldInfo> fieldList = new ArrayList<BackendFieldInfo>();
			
			// These debug statements should help you understand what is being
			// passed back to your calls. You can comment these out if you like
			logger.info("***");
			logger.info("getRecordName: "+recordInfo.getRecordName());
			logger.info("getCatalogName: "+recordInfo.getCatalogName());
			logger.info("getLabel: "+recordInfo.getLabel());
			logger.info("getInstanceName: "+recordInfo.getInstanceName());
			logger.info("getRecordType: "+recordInfo.getRecordType());
			logger.info("---");
			
			String fullyQualifiedName = recordInfo.getCatalogName();
			
			JDBCTable jdbcTable = getTableDetails(jdbcConnection, fullyQualifiedName, identifierQuoteString);
			if (jdbcTable != null) 
			{
				logger.debug("\n\n** Table Name: " + jdbcTable.fullyQualifiedName);
				         			         
				// Now, retrieve metadata for each field
				for (int i = 0; i < jdbcTable.columnNames.length; i++) 
				{
					
					logger.info("-*-");
					logger.info("COLUMN_NAME: " + jdbcTable.columnNames[i]);
					logger.info("TYPE_NAME: " + jdbcTable.dbTypeName[i]);
					logger.info("DATA_TYPE: " + jdbcTable.sqlType[i]);
					logger.info("COLUMN_SIZE: " + jdbcTable.precision[i]);
					logger.info("DECIMAL_DIGITS: " + jdbcTable.scale[i]);
					logger.info("---");

					
					String sanitizedColumnName =  JDBCConnectorConstants.sanitizeName(jdbcTable.columnNames[i]);
					
					// Get the Java class corresponding to the JDBC FieldType
					Class<?> infaClazz = JDBCConnectorConstants.getJavaClassFromSqlType(jdbcTable.sqlType[i], jdbcTable.dbTypeName[i]);			        	 			        	 
	            
					// Determine the field Precision
					int precision = JDBCConnectorConstants.getAdjustedPrecision(infaClazz,jdbcTable.precision[i], maxColumnSize, jdbcTable.scale[i]);
					// Determine the field Scale
					int scale = JDBCConnectorConstants.getAdjustedScale(infaClazz,jdbcTable.precision[i], jdbcTable.scale[i]);

					// BackendFieldInfo() constructor takes 5 Parameters:
					//
					// recordInfo:
					// 		The Parent Record for this field
					// fieldName:
					// 		The name string of the this field
					// 		(cannot contain space or special characters), use the
					// 		method setFieldLabel(String) for Setting Business Name
					// fieldType:
					// 		The java class of the field type (must be one
					// 		of: Boolean, byte[], Short, Integer, Long, Float, Double,
					// 		String, java.sql.Timestamp, java.math.BigDecimal,
					// 		java.math.BigInteger)
					// fieldPrecision:
					// 		The field precision/length should be >0,
					// 		for type byte[], BigDecimal or String, Set to -1 to use
					// 		default precision for other types
					// fieldScale:
					// 		The scale of the field, required if type is
					// 		java.math.BigDecimal,set to -1 for other types
					BackendFieldInfo bField = new BackendFieldInfo(recordInfo,
							sanitizedColumnName, infaClazz, precision, scale);
			    		
					// Set the Business Name (Name used in UI)
					bField.setFieldLabel(jdbcTable.columnNames[i]);
					
					bField.setFieldDescription(jdbcTable.columnNames[i]);
					
					// In case of multiple source Objects the column names could
					// clash for example account and contact both have columns
					// named 'Id' Therefore we prefix the column name with the
					// catalog name to make it unique For example Account.Id and
					// Account.Owner.Id
					bField.setFieldUniqueName(jdbcTable.fullyQualifiedName+"."+jdbcTable.columnNames[i]);
					
					// If the field can be used in the Filters
					if(!byte[].class.isAssignableFrom(infaClazz))
						bField.setFilterable(true);

					// If the field is primary key
					bField.setKey(jdbcTable.isKey[i]);
			    		
					// If the field is NOT NULLABLE
					bField.setMandatory(!jdbcTable.isNullable[i]);

					// Set the custom Attribute (we need to use custom
					// attributes because the standard field object does not
					// have place holders for these)
					//Comment this section if you dont need to set any custom attributes			
					HashMap<String,String> fieldAttribs = new HashMap<String,String>();
					fieldAttribs.put("SQL_DATA_TYPE_ID",String.valueOf(jdbcTable.sqlType[i]));
					fieldAttribs.put("DB_TYPE_NAME",String.valueOf(jdbcTable.dbTypeName[i]));

		    		bField.setCustomAttributes(fieldAttribs);

		    		fieldList.add(bField);
				}
			}
			if (fieldList.isEmpty()) {
				logger.error("Error fetching metadata for table/view ["+ recordInfo.getCatalogName() + "]");
			}			
			return fieldList;
		} catch (Throwable t) {
			t.printStackTrace();
			if (t instanceof MetadataReadException)
				throw (MetadataReadException) t;
			else
				throw new MetadataReadException(t.toString());
		}
	}	   
	   

		

	/**
	 * This method reads data from Object specified in recordInfo and passed the
	 * data into the OutputBuffer
	 * 
	 * @param dataBufferInstance
	 * @param fieldList
	 * @param recordInfo
	 * @param relatedRecordInfoList
	 * @param filterInfoList
	 * @param advancedFilterInfo
	 * @param isPreview
	 * @param pageSize
	 * @param recordAttributes
	 * @param readOperationAttributes
	 * @param childRecordList
	 * @param isLookup
	 * @param jdbcConnection
	 * @param identifierQuoteString
	 * @return
	 * @throws ConnectionFailedException
	 * @throws ReflectiveOperationException
	 * @throws ReadException
	 * @throws DataConversionException
	 * @throws FatalRuntimeException
	 */
	public static boolean read(IOutputDataBuffer dataBufferInstance,
			List<Field> fieldList, RecordInfo recordInfo,
			List<RecordInfo> relatedRecordInfoList,
			List<FilterInfo> filterInfoList,
			AdvancedFilterInfo advancedFilterInfo, boolean isPreview,
			long pageSize,
			Map<RecordInfo, Map<String, String>> recordAttributes,
			Map<String, String> readOperationAttributes,
			List<RecordInfo> childRecordList, boolean isLookup,
			Connection jdbcConnection, String identifierQuoteString) throws ConnectionFailedException,
			ReflectiveOperationException, ReadException,
			DataConversionException, FatalRuntimeException 
	{

		ResultSet rs = null;
		PreparedStatement stmt = null;

		try 
		{
			// These debug statements should help you understand what is being
			// passed back to your calls. You can comment these out if you like
			
			logger.debug("***");
			logger.debug("getRecordName: "+recordInfo.getRecordName());
			logger.debug("getCatalogName: "+recordInfo.getCatalogName());
			logger.debug("getLabel: "+recordInfo.getLabel());
			logger.debug("getInstanceName: "+recordInfo.getInstanceName());
			logger.debug("getRecordType: "+recordInfo.getRecordType());
			logger.debug("---");

			if (readOperationAttributes != null) {
				String paramValue = readOperationAttributes
						.get("Check Parameter");
				if (paramValue != null && paramValue.equals("1")) {
					logger.info(" Parameter was checked !!!");
				}
			}
		
			
	    	int j=0;
			String[] fieldNames = new String[fieldList.size()];
			int[] sqlType = new int[fieldList.size()];
			String[] dbType = new String[fieldList.size()];
			for (Field field : fieldList) 
	    	{
	    		fieldNames[j] = JDBCConnectorConstants.unsanitizeName(field.getDisplayName());	
				List<FieldAttribute> customAtributes = field.getCustomAttributes();

				//Get Custom Field Attributes
				for (FieldAttribute customAtribute : customAtributes) {
	    			try 
	    			{
	    					if (customAtribute.getName().equals("SQL_DATA_TYPE_ID")) {
	    						//logger.info("SQL_DATA_TYPE_ID: " + customAtribute.getValue());
	    						sqlType[j] = Integer.parseInt(customAtribute.getValue()!=null && !customAtribute.getValue().isEmpty()? customAtribute.getValue() : "0");
	    					}
	    					else if (customAtribute.getName().equals("DB_TYPE_NAME")) {
	    						//logger.info("DB_TYPE_NAME: " + customAtribute.getValue());
	    						dbType[j] = customAtribute.getValue();
	    					}	    						    					
	    			} catch (Throwable t) {
	    				t.printStackTrace();
	    			}
	    		}
				j++;    	    	    	
	    	}
			
			int colNum = fieldNames.length;
			String tableName = recordInfo.getRecordName();    	    	
	    	JDBCTable ts  = parseQualifiedName(jdbcConnection, recordInfo.getCatalogName());    						
			String selectSql = generateSelectStatement(getDatabaseProductName(jdbcConnection), fieldNames, ts.catalog, ts.schema, tableName,identifierQuoteString,filterInfoList,advancedFilterInfo, isPreview, pageSize, isLookup,jdbcConnection);			

			stmt = jdbcConnection.prepareStatement(selectSql);
			rs = stmt.executeQuery();

			try
			{
				ResultSetMetaData resultSetMetata = rs.getMetaData();
				colNum = resultSetMetata.getColumnCount();    				
			}catch(Throwable t)
			{
				logger.warn("Warning: Query ResultSet does not support Metadata");
				t.printStackTrace();
			}
			
			int rowsSoFar = 0;
			while (rs.next()) 
			{
              	Object[] rowData = new Object[colNum];
              	
				for (int i = 0; i < rowData.length; i++) 
				{
					int index = i+1;
					Object value = rs.getObject(index);
					if (value != null) {
						// Convert the value to a type that The buffer accepts
						// first before setting it in rowData
						value = JDBCConnectorConstants.toInfaJavaDataType(value, fieldList.get(i).getJavaDatatype());
					}
					rowData[i] = value;
				}				
				// Set the output record in the databuffer, this will be
				// synchronously sent to the target for processing
				dataBufferInstance.setData(rowData);
				rowsSoFar++;

				// If its preview exit when the pagesize is reached
				if(isPreview && rowsSoFar>=pageSize)
					break;
			}
			logger.info("Query returned {" + rowsSoFar + "} rows");
		} catch (Throwable t) {
			t.printStackTrace();
			throw new FatalRuntimeException(t.toString());
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
			} catch (SQLException ex) {
				ex.printStackTrace();
			}
			rs = null;
			if (stmt != null)
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			stmt = null;
			try {
				if (!jdbcConnection.getAutoCommit()) {
					jdbcConnection.commit();
				}
			} catch (SQLException ex) {
				ex.printStackTrace();
			}

		}
		return true;
	}	
	
    
	/**
	 * This method reads data from the InputBuffer and writes data to JDBC
	 * 
	 * @param partnerConnection
	 * @param inputDataBuffer
	 * @param fieldList
	 * @param recordInfo
	 * @param writeOperation
	 *            If this is INSERT,UPDATE,UPSERT or DELETE
	 * @return Status for each row in the buffer
	 * @throws WriteException
	 * @throws FatalRuntimeException
	 */
	public static List<OperationResult> write (
			IInputDataBuffer inputDataBuffer, List<Field> fieldList,
			RecordInfo recordInfo, WriteOperation writeOperation,
			String identifierQuoteString,
			Map<RecordInfo, Map<String, String>> recordAttributes,
			Map<String, String> operationAttributes,
			List<RecordInfo> secondaryRecordInfoList, Connection jdbcConnection, int batchSize)
			throws ConnectionFailedException, ReflectiveOperationException,
			WriteException, DataConversionException, FatalRuntimeException
	{
		if(dbConnection == null){
			dbConnection = jdbcConnection;
		}
		logger.debug("JDBCConnectorUtils.write("+recordInfo.getCatalogName()+")");	
		List<OperationResult> rowStatus = new ArrayList<OperationResult>();		
//		insert_pstmt = null;		
		PreparedStatement update_pstmt = null;
		PreparedStatement delete_pstmt = null;		

    	try 
    	{    	    		
    		System.out.println("JDBCUtils.write(), batchsize="+batchSize);
    		if(batchSize > 1){
    			jdbcConnection.setAutoCommit(false);
    		}
    		boolean keyFound = false;

    		int[] sqlType = new int[fieldList.size()];
			String[] dbType = new String[fieldList.size()];
    		boolean[] isKey = new boolean[fieldList.size()];
	    	ArrayList<String> fieldNames = new ArrayList<String>();

	    	Map<Integer,Integer> insert_fieldIndexMap = new HashMap<Integer,Integer>();
	    	Map<Integer,Integer> insert_whereIndexMap = new HashMap<Integer,Integer>();
	    	
	    	Map<Integer,Integer> update_fieldIndexMap = new HashMap<Integer,Integer>();
	    	Map<Integer,Integer> update_whereIndexMap = new HashMap<Integer,Integer>();

	    	Map<Integer,Integer> delete_fieldIndexMap = new HashMap<Integer,Integer>();
	    	Map<Integer,Integer> delete_whereIndexMap = new HashMap<Integer,Integer>();
	    		    	
    		logger.info("operationAttributes:" + operationAttributes);
	    	ArrayList<String> pkeyColumns = new ArrayList<String>();
    		
	    	int j=0;
	    	for (Field field : fieldList)
	    	{
	    		fieldNames.add(JDBCConnectorConstants.unsanitizeName(field.getDisplayName()));

	    			if(field.isKey())
	    			{
		    			isKey[j] = true;
	    				pkeyColumns.add(fieldNames.get(j));
	    				keyFound = true;
	    			}else
	    			{
		    			isKey[j] = false;	    				
	    			}
	    			    		
				List<FieldAttribute> customAtributes = field.getCustomAttributes();    			
	    		for (FieldAttribute customAtribute : customAtributes) 
	    		{
	    			try 
	    			{
	    					if (customAtribute.getName().equals("SQL_DATA_TYPE_ID")) {
	    						//logger.info("SQL_DATA_TYPE_ID: " + customAtribute.getValue());
	    						sqlType[j] = Integer.parseInt(customAtribute.getValue()!=null && !customAtribute.getValue().isEmpty()? customAtribute.getValue() : "0");
	    					}
	    					else if (customAtribute.getName().equals("DB_TYPE_NAME")) {
	    						//logger.info("DB_TYPE_NAME: " + customAtribute.getValue());
	    						dbType[j] = customAtribute.getValue();
	    					}
	    			} catch (Throwable t) {
	    				t.printStackTrace();
	    			}
	    		}
				j++;    	    	    	
	    	}
	    			    	
			JDBCTable ts = parseQualifiedName(jdbcConnection,recordInfo.getCatalogName());
	    	
			// make sure one of the key fields is mapped for upsert or update
			// operations
			if (writeOperation == WriteOperation.UPDATE
					|| writeOperation == WriteOperation.UPSERT || writeOperation == WriteOperation.DELETE ) {

				if (!keyFound) {
					throw new WriteException(
							"You must map a primary key field for UPSERT, UPDATE or DELETE");
				}

			}			
			
	    	if(writeOperation == WriteOperation.DELETE)
	    	{
	    		String deleteSql = generateDeleteStatement(jdbcConnection,
					getDatabaseProductName(jdbcConnection), fieldNames,
					ts.catalog, ts.schema, ts.name, identifierQuoteString, isKey, sqlType, dbType, delete_whereIndexMap);
	    		delete_pstmt = jdbcConnection.prepareStatement(deleteSql);
	    	}else
	    	{
				if(writeOperation == WriteOperation.INSERT || writeOperation == WriteOperation.UPSERT)
		    	{
		    		String insertSql = generateInsertStatement(jdbcConnection,
						getDatabaseProductName(jdbcConnection), fieldNames.toArray(new String[0]),
						ts.catalog, ts.schema, ts.name, identifierQuoteString, isKey, sqlType, dbType, insert_fieldIndexMap, insert_whereIndexMap);
		    		if(insert_pstmt == null || batchSize <= 1){		
		    			System.out.println("JDBCUtils.write(), recreating insert_pstmt, batchsize="+batchSize);
		    			insert_pstmt = jdbcConnection.prepareStatement(insertSql);
		    		}
		    	}
		    	
		    	if(writeOperation == WriteOperation.UPDATE || writeOperation == WriteOperation.UPSERT)
		    	{
		    		String updateSql = generateUpdateStatement(jdbcConnection,
						getDatabaseProductName(jdbcConnection), fieldNames.toArray(new String[0]),
						ts.catalog, ts.schema, ts.name, identifierQuoteString, isKey, sqlType, dbType, update_fieldIndexMap,update_whereIndexMap);
		    		update_pstmt = jdbcConnection.prepareStatement(updateSql);
		    	}
	    	}

	    		System.out.println("JDBCUtils.write(), inputdatabuffer size="+inputDataBuffer.getRowCount());
	    	//Read data from the buffer and write to the target
	    	//Do not return from the method until buffer is empty or an exception/error occurs
			 while(inputDataBuffer.hasMoreRows()) 
			 {
				 try
				 {
						Object[] data = inputDataBuffer.getData();
						
						//This is just a sanity check, data.length should always be equal to fieldList.size
						//The buffer only contains data for fields that are mapped.
						if(data.length != fieldList.size())							
						{
				    		logger.warn("fieldList.size(): "+fieldList.size());
				    		logger.warn("data.length: "+data.length);

							for (int cnt=0;cnt< fieldNames.size();cnt++)
					    		logger.warn(fieldNames.get(cnt));
							throw new FatalRuntimeException("buffer and fieldList length do not match");
						}
						
						HashMap<String,Object> record = new HashMap<String,Object>();
				    	for (int cnt=0;cnt< fieldNames.size();cnt++)
				    		record.put(fieldNames.get(cnt), data[cnt]);				    	
				    	logger.debug("Input Record: "+record);
				    	
				    	int rowCount = 0;

				    	if(writeOperation == WriteOperation.DELETE)
				    	{
				    		setInputData(delete_pstmt, data, recordInfo, fieldList,fieldNames, sqlType, dbType, delete_fieldIndexMap, delete_whereIndexMap);
				    		rowCount = delete_pstmt.executeUpdate();
				    		if(!jdbcConnection.getAutoCommit())
				    			jdbcConnection.commit();
							logger.info("delete impacted {"+rowCount+"} rows");
							delete_pstmt.clearParameters();
							if(rowCount > 0)
							{
								rowStatus.add(new OperationResult(true, null));
								continue; //Skip the insert/update/code code below as update succeeded
							}
							else
								rowStatus.add(new OperationResult(false, new Exception("Invalid value for PrimaryKey column")));
				    	}else
				    	{				    	
					    	if(writeOperation == WriteOperation.UPDATE || writeOperation == WriteOperation.UPSERT)
					    	{
						    		setInputData(update_pstmt, data, recordInfo, fieldList, fieldNames,sqlType, dbType, update_fieldIndexMap, update_whereIndexMap);
						    		rowCount = update_pstmt.executeUpdate();
						    		if(!jdbcConnection.getAutoCommit())
						    			jdbcConnection.commit();
									logger.info("Update impacted {"+rowCount+"} rows");
									update_pstmt.clearParameters();								
							    	if(rowCount > 0)
							    	{
								    	rowStatus.add(new OperationResult(true, null));
							    		continue; //Skip the insert code below as update succeeded
							    	}
							    	else
							    		if(writeOperation == WriteOperation.UPDATE)
							    			rowStatus.add(new OperationResult(false, new Exception("Invalid value for PrimaryKey column")));
							}				    	
					    	
					    	if(writeOperation == WriteOperation.INSERT || writeOperation == WriteOperation.UPSERT)
					    	{
					    		if(batchSize > 1 ){
					    			if(jdbcConnection.getAutoCommit()){
					    				jdbcConnection.setAutoCommit(false);
					    			}
					    			System.out.println("Adding to the batch, currentbatchsize="+currentBatch);
					    			setInputData(insert_pstmt, data, recordInfo, fieldList,fieldNames,sqlType, dbType, insert_whereIndexMap, insert_fieldIndexMap);
					    			insert_pstmt.addBatch();
					    			currentBatch++;
                                    if(currentBatch == batchSize){
					    				System.out.println("Executing the batch, resetting to zero currentbatch="+currentBatch);
					    				int[] results = insert_pstmt.executeBatch();
					    				System.out.println("JDBCUtils.write(), result size="+results.length);
					    				jdbcConnection.commit();
					    				insert_pstmt.clearBatch();
					    				currentBatch = 0;
					    				for(int result: results){
					    					if(result == Statement.SUCCESS_NO_INFO){
					    						rowStatus.add(new OperationResult(true, null));
					    					} else if(result > 0){
					    						for(int i =0; i<result; i++){
					    							rowStatus.add(new OperationResult(true, null));
					    						}
					    					} else if(result == Statement.EXECUTE_FAILED){
					    						rowStatus.add(new OperationResult(false, new Exception("insert failed")));
					    					}
					    				}
					    			}
					    		}else{
					    			System.out.println("batchsize="+batchSize);
					    			System.out.println("row by row inserts");
						    		setInputData(insert_pstmt, data, recordInfo, fieldList,fieldNames,sqlType, dbType, insert_whereIndexMap, insert_fieldIndexMap);
					    			rowCount = insert_pstmt.executeUpdate();
					    			if(!jdbcConnection.getAutoCommit())				    		
					    				jdbcConnection.commit();
					    			if(rowCount > 0)
					    				rowStatus.add(new OperationResult(true, null));
					    			else
					    				rowStatus.add(new OperationResult(false, new Exception("Invalid value")));
					    			insert_pstmt.clearParameters();
					    			logger.info("Insert impacted {"+rowCount+"} rows");
					    		}
					    	}
				    	}
					}catch (SQLException e) {
						e.printStackTrace();
				    	rowStatus.add(new OperationResult(false, e));
						try {
							if (!jdbcConnection.getAutoCommit()) {
								jdbcConnection.rollback();
							}
						} catch (SQLException ex) {ex.printStackTrace();}
					}catch(IndexOutOfBoundsException ioobe){
						//This is to handle the bug in INFA_JUnit IInputDataBufferImpl
						break;
					}
    		}//End While
			 //logger.info("rowStatus size: "+rowStatus.size());
		    return rowStatus;
		}catch (Throwable ex) {
			ex.printStackTrace();
			throw new WriteException(ex.toString());
		} finally {
			
			if (insert_pstmt != null && batchSize <= 1) {
				try {
					System.out.println("JDBCUtils.write(), closing insert_pstmt");
					insert_pstmt.close();
				} catch (SQLException e) {
				}
				System.out.println("JDBCUtils.write(), setting to null insert_pstmt");
				insert_pstmt = null;
			}

			if (update_pstmt != null) {
				try {
					update_pstmt.close();
				} catch (SQLException e) {
				}
				update_pstmt = null;
			}
			
			if (delete_pstmt != null) {
				try {
					delete_pstmt.close();
				} catch (SQLException e) {
				}
				delete_pstmt = null;
			}

		}	        
    }

	
	private static Collection<JDBCTable> getTables(Connection jdbcConnection,String catalog, String schema, String tableName, String[] types) throws SQLException 
	{
		ArrayList<JDBCTable> tables = new ArrayList<JDBCTable>();
		ResultSet rs = null;
		try 
		{
			catalog = getAdjustedCatalog(jdbcConnection, catalog);			
			schema = getAdjustedSchema(getDatabaseProductName(jdbcConnection),schema);
			
			DatabaseMetaData dbmd = jdbcConnection.getMetaData();

			rs = dbmd.getTables(catalog,schema, tableName, types);
			while (rs.next()) {
				JDBCTable ts = new JDBCTable();				
				try {
					ts.catalog = rs.getString("TABLE_CAT");
				} catch (SQLException e) {
					logger.warn("Failed to get TABLE_CAT from MetaData", e);
				}
				try {
					ts.schema = rs.getString("TABLE_SCHEM");
				} catch (SQLException e) {
					logger.warn("Failed to get TABLE_CAT from MetaData", e);
				}
				try {
					ts.name = rs.getString("TABLE_NAME");
				} catch (SQLException e) {
					logger.warn("Failed to get TABLE_NAME from MetaData", e);
					throw e;
				}
				try {
					ts.type = rs.getString("TABLE_TYPE");
				} catch (SQLException e) {
					logger.warn("Failed to get TABLE_TYPE from MetaData", e);
				}

				if(ts.name==null)
					throw new SQLException("Could not fetch table metadata from the database");
				
				ts.fullyQualifiedName = makeQualifiedName(ts.catalog, ts.schema, ts.name, false,"",jdbcConnection);
				logger.debug("Found Object {"+ts.fullyQualifiedName+"} type {"+ts.type+"}");
		
				tables.add(ts);
			}
			if(tables.isEmpty())
			{
				logger.error("Failed to find tables in catalog {"+catalog+"} schema {"+schema+"}");
			}
		} catch (SQLException e) {
			logger.error("Failed to find tables in catalog {"+catalog+"} schema {"+schema+"}");
			e.printStackTrace();
			throw e;
		} finally {
			if (rs != null)
				try {
					rs.close();
				} catch (SQLException e) {e.printStackTrace();}
			rs = null;
		}			
		return tables;
	}
	
	private static JDBCTable getTableDetails(Connection jdbcConnection, String qualifiedName, String identifierQuoteString) throws SQLException  
	{
		// Create backups of everything, in case we need to roll back
		// after catching an exception
		logger.debug("JDBCUtils.getTableDetails(\"" + qualifiedName + "\",\"" + identifierQuoteString + "\")");
		JDBCTable jdbcTable = new JDBCTable();
		
		try 
		{		

			ResultSet rs = null;
			DatabaseMetaData dbmd = null;
			ArrayList<String> tableNames= new ArrayList<String>();
			int count = 0;
			try 
			{
					JDBCTable ts = parseQualifiedName(jdbcConnection, qualifiedName);
					dbmd = jdbcConnection.getMetaData();
					rs = dbmd.getTables(ts.catalog, ts.schema, ts.name, null);
											
					while (rs.next()) 
					{	
						JDBCTable temp = new JDBCTable();
						
						try {
							temp.catalog = rs.getString("TABLE_CAT");
						} catch (SQLException e) {
							logger.warn("Failed to get TABLE_CAT from MetaData", e);
						}
						try {
							temp.schema = rs.getString("TABLE_SCHEM");
						} catch (SQLException e) {
							logger.warn("Failed to get TABLE_CAT from MetaData", e);
						}
						try {
							temp.name = rs.getString("TABLE_NAME");
						} catch (SQLException e) {
							logger.warn("Failed to get TABLE_NAME from MetaData", e);
						}
						try {
							temp.type = rs.getString("TABLE_TYPE");
						} catch (SQLException e) {
							logger.warn("Failed to get TABLE_TYPE from MetaData", e);
						}	
						//Make sure we have the right table name
						if(temp.name!=null && temp.name.equals(ts.name))
						{	
							jdbcTable.catalog = temp.catalog;
							jdbcTable.schema = temp.schema;
							jdbcTable.name = temp.name;
							jdbcTable.type = temp.type;															
						}
						
						tableNames.add(makeQualifiedName(temp.catalog, temp.schema, temp.name, false, identifierQuoteString,jdbcConnection));
						count++;						
					} 
					
			} finally {
				if (rs != null)
					try {
						rs.close();
					} catch (SQLException e) {}
				rs = null;
			}

			if(count<1)
				throw new SQLException("Table [" + qualifiedName + "] does not exist");						
										
			if(count>1)
			{
				//we found more than one table. Warn the user about this						
				logger.warn("Found tables {"+tableNames.toString()+"} matching name {"+qualifiedName+"}");
			}
			
			//JDBCTable ts1 = new JDBCTable();
			//ts1.catalog = jdbcTable.catalog;
			//ts1.schema = jdbcTable.schema;
			//ts1.name = jdbcTable.name;
			//ts1.type = jdbcTable.type;

			
			jdbcTable.fullyQualifiedName = makeQualifiedName(jdbcTable.catalog, jdbcTable.schema, jdbcTable.name, false, "",jdbcConnection);
			
			ArrayList<String> columnNamesList = new ArrayList<String>();
			ArrayList<String> dbTypesList = new ArrayList<String>();
			ArrayList<Integer> columnSizesList = new ArrayList<Integer>();
			ArrayList<Integer> columnScalesList = new ArrayList<Integer>();
			ArrayList<Integer> sqlTypesList = new ArrayList<Integer>();
			ArrayList<Boolean> isNullableList = new ArrayList<Boolean>();
			
			rs = null;
			int cols = 0;
			try {
				rs = dbmd.getColumns(jdbcTable.catalog, jdbcTable.schema, jdbcTable.name, "%");
				while (rs.next()) {

					if(count>1)
					{
						JDBCTable temp = new JDBCTable();						
						try {
							temp.catalog = rs.getString("TABLE_CAT");
						} catch (SQLException e) {
							logger.warn("Failed to get TABLE_CAT from MetaData", e);
						}
						try {
							temp.schema = rs.getString("TABLE_SCHEM");
						} catch (SQLException e) {
							logger.warn("Failed to get TABLE_CAT from MetaData", e);
						}
						try {
							temp.name = rs.getString("TABLE_NAME");
						} catch (SQLException e) {
							logger.warn("Failed to get TABLE_NAME from MetaData", e);
						}
						
						//if tablename matches multiple tables then getColumns() will return columns for all tables
						//We need to pick the columns for our table only
						if(temp.name!= null && !temp.name.equals(jdbcTable.name))
							continue;								
						if(temp.schema!= null && !temp.schema.equals(jdbcTable.schema))
							continue;								
						if(temp.catalog!= null && !temp.catalog.equals(jdbcTable.catalog))
							continue;								
					}
						
					columnNamesList.add(rs.getString("COLUMN_NAME"));

					sqlTypesList.add(rs.getInt("DATA_TYPE"));

					dbTypesList.add(rs.getString("TYPE_NAME"));
					
					int size = rs.getInt("COLUMN_SIZE");
					if (rs.wasNull())
						columnSizesList.add(-1);
					else
						columnSizesList.add(size);
					
					int scale = rs.getInt("DECIMAL_DIGITS");
					if (rs.wasNull())
						columnScalesList.add(-1);
					else
						columnScalesList.add(scale);
					
					
					String isNullable = rs.getString("IS_NULLABLE");
					if(isNullable!=null && (isNullable.equalsIgnoreCase("NO") || isNullable.equalsIgnoreCase("FALSE") || isNullable.equalsIgnoreCase("N") || isNullable.equalsIgnoreCase("0")))
						isNullableList.add(false);
					else
						isNullableList.add(true);
					
					cols++;
				}
			} catch (SQLException e) {
				e.printStackTrace();
				throw (e);
			} finally {
				if (rs != null)
					try {
						rs.close();
					} catch (SQLException e) {e.printStackTrace();}
				rs = null;
			}

			if(cols<1)
				throw new SQLException("Could not find any columns in table [" + jdbcTable.fullyQualifiedName + "]");						
			

	
			ArrayList<String> pkeyColumnNames = getPrimaryKeyColumns(jdbcConnection, jdbcTable);
			if(pkeyColumnNames==null || pkeyColumnNames.isEmpty())
			{
				//No Primary keys check for unique index
				pkeyColumnNames = getUniqueIndexColumns(jdbcConnection, jdbcTable);				
			}

			jdbcTable.columnNames = columnNamesList.toArray(new String[columnNamesList.size()]);
			jdbcTable.dbTypeName = dbTypesList.toArray(new String[columnNamesList.size()]);
			jdbcTable.isNullable = new boolean[columnNamesList.size()];
			jdbcTable.sqlType = new int[columnNamesList.size()];
			jdbcTable.precision = new int[columnNamesList.size()];
			jdbcTable.scale = new int[columnNamesList.size()];
			jdbcTable.isKey = new boolean[columnNamesList.size()];
			
			for (int i = 0; i < columnNamesList.size(); i++) {
				jdbcTable.precision[i] = columnSizesList.get(i);
				jdbcTable.scale[i] = columnScalesList.get(i);
				jdbcTable.sqlType[i] = sqlTypesList.get(i);
				jdbcTable.isNullable[i] = isNullableList.get(i);
				if(pkeyColumnNames.contains(columnNamesList.get(i)))
					jdbcTable.isKey[i] = true;
			}			
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {}
		return jdbcTable;
	}
	
	private static ArrayList<String> getPrimaryKeyColumns(Connection jdbcConnection,JDBCTable ts)  {
		logger.debug( "getPrimaryKey(\"" + ts + "\")");
		ResultSet rs = null;
		ArrayList<String> pkeyColumnNames = new ArrayList<String>();
		try {
			DatabaseMetaData dbmd = jdbcConnection.getMetaData();
			rs = dbmd.getPrimaryKeys(ts.catalog, ts.schema, ts.name);
			while (rs.next()) {
				String pkey_columnName = rs.getString("COLUMN_NAME");
				if(pkey_columnName!=null && !pkey_columnName.isEmpty())
				{
					pkeyColumnNames.add(pkey_columnName);
				}
			}
		} catch (Throwable t) {
			t.printStackTrace();
		} finally {
			if (rs != null)
				try {
					rs.close();
				} catch (SQLException e) {e.printStackTrace();}
			rs = null;
		}
		return pkeyColumnNames;
	}

	private static ArrayList<String> getUniqueIndexColumns(Connection jdbcConnection,JDBCTable table) {
		logger.debug( "getIndexes(\"" + table.fullyQualifiedName + "\")");
		ResultSet rs = null;
		ArrayList<String> uniqueIndexColumnNames = new ArrayList<String>();
		try 
		{
			DatabaseMetaData dbmd = jdbcConnection.getMetaData();
			rs = dbmd.getIndexInfo(table.catalog,
								table.schema,
								table.name,
								true,
								false);
			while (rs.next()) {
                boolean nonUnique = rs.getBoolean("NON_UNIQUE");
                String indexName = rs.getString("INDEX_NAME");
                logger.debug("INDEX_NAME: " + indexName);
                String columnName = rs.getString("COLUMN_NAME");
				if (columnName == null)
					continue;
				if(!nonUnique)
					uniqueIndexColumnNames.add(columnName);
			}
		} catch (Throwable t) {
			t.printStackTrace();			
		} finally {
			if (rs != null)
				try {
					rs.close();
				} catch (SQLException e) {e.printStackTrace();}
			rs = null;
		}
		return uniqueIndexColumnNames;
	}
	
				
	private static String getDatabaseProductName(Connection jdbcConnection) {
		try {
			DatabaseMetaData dbmd = jdbcConnection.getMetaData();			
			return dbmd.getDatabaseProductName();
		} catch (SQLException e) {		
			logger.warn("An unexpected error occurred while getting DatabaseMetaData", e);
			return "JDBC";
		}					
	}

	
	/**
	 * Generate a name that represents a table uniquely to the RDBMS, i.e.
	 * something that can be used in SQL scripts and is guaranteed not to
	 * access the wrong table.
	 */
	private static String makeQualifiedName(String catalog, String schema, String name, boolean quote,String identifierQuoteString,Connection jdbcConnection) {
		
		
		String databaseProductName = getDatabaseProductName(jdbcConnection);
		if(databaseProductName!=null && (databaseProductName.equalsIgnoreCase("Informix") || databaseProductName.equalsIgnoreCase("Informix Dynamic Server"))){
			return makeQualifiedNameWithoutCatalog(schema, name, true, identifierQuoteString,jdbcConnection);
		}
		
		if(!quote)
		{
			//This is for internal use we will deal with null
			return catalog + "." + schema + "." + name;		
		}

		catalog = quoteIdentifier(catalog,identifierQuoteString);
		schema = quoteIdentifier(schema,identifierQuoteString);		
		name = quoteIdentifier(name,identifierQuoteString);
		
		if (catalog != null)
			if (schema != null)
				return catalog + "." + schema + "." + name;
			else
				return catalog + "." + name;
		else if (schema != null)
			return schema + "." + name;
		else
			return name;
	}
   private static String makeQualifiedNameWithoutCatalog(String schema, String name, boolean quote,String identifierQuoteString,Connection jdbcConnection) {
		
		if(!quote)
		{
			//This is for internal use we will deal with null
			return schema + "." + name;		
		}

		
		schema = quoteIdentifier(schema,identifierQuoteString);		
		name = quoteIdentifier(name,identifierQuoteString);
		
		if (schema != null)
			return schema + "." + name;
		else
			return name;
	}

	private static JDBCTable parseQualifiedName(Connection jdbcConnection, String qualifiedName) 
	{
		JDBCTable ts = new JDBCTable();
		if(qualifiedName != null)
		{
			ArrayList<String> al = new ArrayList<String>();
			StringTokenizer tok = new StringTokenizer(qualifiedName, ".");
			while (tok.hasMoreTokens())				
				al.add(tok.nextToken());
				
			int i = al.size();

			if (i > 0)
			{
				ts.name = al.get(--i);
				if(ts.name != null && ts.name.equals("null"))
					ts.name = null;
			}else
				ts.name = null;

			if (i > 0)
			{
				ts.schema = al.get(--i);
				if(ts.schema != null && ts.schema.equals("null"))
					ts.schema = null;
			}else
				ts.schema = null;

			if (i > 0)
			{
				ts.catalog = al.get(--i);			
				if(ts.catalog != null && ts.catalog.equals("null"))
					ts.catalog = null;
			} else
				ts.catalog = null;	
		}
		return ts;
	}
		
	/**
	 * Quotes the identifier. for example Accounts will be [Accounts]
	 * 
	 * @param identifier
	 *            the identifier to quote
	 * @param quoteChar
	 *            The quotes to use
	 * @return
	 */
	private  static String quoteIdentifier(String identifier,String quoteChar) {

		if (identifier == null)
			return null;

		if (quoteChar == null || quoteChar.isEmpty() || quoteChar.equalsIgnoreCase("DEFAULT") )
			return identifier;
		else 
		{
			String beginQuote = "";
			String endQuote = "";
			if(quoteChar != null && !quoteChar.isEmpty())
			{
				if(quoteChar.length()==2)
				{
					beginQuote = quoteChar.substring(0, 1);
					endQuote = quoteChar.substring(1, 2);				
				}else
				{
					beginQuote = quoteChar.substring(0, 1);
					endQuote = quoteChar.substring(0, 1);				
				}
			}
			
			//Commented this check to always quote identifiers
			/*
			int sl = s.length();
			for (int i = 0; i < sl; i++) {
				char c = s.charAt(i);
				if (!((c >= 'A' && c <= 'Z')
							|| (c >= 'a' && c <= 'z')
							|| (c >= '0' && c <= '9')
							|| c == '_'))
			*/
			return String.format("%s%s%s", beginQuote, identifier, endQuote);
		}
	}
		

	/**
	 * This will quote literal values
	 * 
	 * @param literalValue
	 *            The literal value to quote
	 * @param quoteChar
	 *            The quote character. It is single quote (') most databases
	 * @param escapeChar
	 *            The escape character for the quotes inside of the literal. It
	 *            is single quote (') most databases
	 * @return String Quoted Literal value
	 */
	private  static String quoteLiteral(String literalValue,String quoteChar,String escapeChar) 
	{
		if (literalValue == null)
			return null;
		
		if(quoteChar==null || quoteChar.isEmpty() || quoteChar.equalsIgnoreCase("DEFAULT"))
			quoteChar = "'";

		if(escapeChar==null || escapeChar.isEmpty() || escapeChar.equalsIgnoreCase("DEFAULT"))
			escapeChar = "'";
		
		if(literalValue.length() > 1 && literalValue.startsWith(quoteChar) && literalValue.endsWith(quoteChar))
			return literalValue;

		return String.format("%s%s%s", quoteChar, literalValue.replaceAll(Pattern.quote(quoteChar), Matcher.quoteReplacement(escapeChar+quoteChar)), quoteChar);
	}


	static private String generateSelectStatement(String databaseType, String[] fieldNames,String catalog,String schema,
			String tableName, String identifierQuoteString,List<FilterInfo> filterInfoList, AdvancedFilterInfo advancedFilterInfo,boolean isPreview, long pageSize, boolean isLookup,Connection jdbConnection) throws ReadException
	{

    	StringBuilder select= new StringBuilder("SELECT ");
		
		if (fieldNames.length == 0) {
			// if no field name, assume we want to select all
			select.append("*");
		} else {
			select.append(quoteIdentifier(fieldNames[0], identifierQuoteString));
			for (int i = 1; i < fieldNames.length; i++) {
				select.append(",");
				select.append(quoteIdentifier(fieldNames[i], identifierQuoteString));
			}
		}

		select.append(" FROM ");	
	     select.append(makeQualifiedName(catalog, schema, tableName, true, identifierQuoteString,jdbConnection));
		
		String whereClause = getWhereClause(filterInfoList,advancedFilterInfo, identifierQuoteString, isLookup);

		if(whereClause !=null && !whereClause.isEmpty())
		{
			select.append(whereClause);
		}
		

		if(isPreview)
		{
			//PREFIX 
			logger.info("database type: " + databaseType);
			if (databaseType.equalsIgnoreCase("sqlserver")
	                || databaseType.equalsIgnoreCase("MS_ACCESS") || databaseType.equalsIgnoreCase("Microsoft SQL Server") || databaseType.equalsIgnoreCase("Adaptive Server Enterprise")) {
				select.insert(select.indexOf("SELECT")+"SELECT".length()+1,"TOP " + pageSize + " ");
			}else if (databaseType.equalsIgnoreCase("informix") || databaseType.equalsIgnoreCase("Informix Dynamic Server")) {
				select.insert(select.indexOf("SELECT")+"SELECT".length()+1,"FIRST " + pageSize + " ");
			} //SUFFIX
			else if(databaseType.equalsIgnoreCase("oracle")){
				if(whereClause ==null || whereClause.isEmpty())
					select.append(" WHERE ");
				else
					select.append(" AND ");
				select.append("ROWNUM <= "+pageSize);
			} else if (databaseType.equalsIgnoreCase("mysql")) {
				select.append(" LIMIT " + pageSize);
			}else
				select.append(" LIMIT " + pageSize);		
		}

		String selectsql = select.toString();
		logger.info("Select SQL: " + selectsql);
		return selectsql;
    }
	
	private static String generateInsertStatement(Connection jdbcConnection,
			String databaseType, String[] fieldNames, String catalog,
			String schema, String tableName, String identifierQuoteString, boolean[] isKey, int[] sqlType, String[] dbType,  Map<Integer, Integer> insert_fieldIndexMap, Map<Integer, Integer> insert_whereIndexMap)
			throws ReadException {
		StringBuilder select = new StringBuilder("INSERT INTO ");
		select.append(makeQualifiedName(catalog, schema, tableName, true,
				identifierQuoteString,jdbcConnection));

		if (fieldNames.length != 0) {
			select.append(" (");
			int cnt=0;
			for (int i = 0; i < fieldNames.length; i++) {
								
				if (cnt > 0) select.append(",");
				select.append(quoteIdentifier(fieldNames[i], identifierQuoteString));
				cnt++;
				
			}
			select.append(")");

			select.append(" VALUES ");

			select.append("(");
			cnt=0;
			for (int i = 0; i < fieldNames.length; i++) 
			{
				if (cnt > 0)
					select.append(",");
				select.append("?");
				cnt++;
				insert_fieldIndexMap.put(i, cnt);
			}
			select.append(")");
		}

		String selectsql = select.toString();
		logger.info("INSERT SQL: " + selectsql);
		return selectsql;
	}

	private static String generateUpdateStatement(Connection jdbcConnection,
			String databaseType, String[] fieldNames, String catalog,
			String schema, String tableName, String identifierQuoteString, boolean[] isKey, int[] sqlType, String[] dbType, Map<Integer, Integer> update_fieldIndexMap, Map<Integer, Integer> update_whereIndexMap)
			throws ReadException {

		StringBuffer updateSql = new StringBuffer("UPDATE ");
		StringBuffer whereSql = new StringBuffer("");

		updateSql.append(makeQualifiedName(catalog, schema, tableName, true,
				identifierQuoteString,jdbcConnection));

		updateSql.append(" SET ");

		int where_cnt = 0;
		int field_cnt = 0;
		
		if (fieldNames.length != 0) 
		{
			for (int i = 0; i < fieldNames.length; i++) 
			{
		    	
		    	if(isKey[i])
		    		continue;

				if (field_cnt > 0)
					updateSql.append(",");
				updateSql.append(quoteIdentifier(fieldNames[i],identifierQuoteString));
				updateSql.append(" = ");
				updateSql.append("?");
				field_cnt++;
				update_fieldIndexMap.put(i, field_cnt);
			}
					
			for (int i = 0; i < fieldNames.length; i++) 
			{										    	
		    	if(isKey[i])
				{
			        if (where_cnt > 0) whereSql.append(" AND ");
			        whereSql.append(quoteIdentifier(fieldNames[i], identifierQuoteString));
			        whereSql.append(" = ");
			        whereSql.append("?");
			        where_cnt++;
			        update_whereIndexMap.put(i, field_cnt+where_cnt);
				}
			}
			
		    if (where_cnt > 0)
		    {
		    	updateSql.append(" WHERE ");
		    	updateSql.append(whereSql);
				//select.append(")");
		    }			
		}

		String selectsql = updateSql.toString();
		logger.info("UPDATE SQL: " + selectsql);
		return selectsql;
	}
    
	private static String generateDeleteStatement(Connection jdbcConnection,
			String databaseProductName, ArrayList<String> fieldNames,
			String catalog, String schema, String tableName,
			String identifierQuoteString, boolean[] isKey, int[] sqlType,
			String[] dbType, Map<Integer, Integer> delete_whereIndexMap)
	{
		StringBuffer updateSql = new StringBuffer("DELETE FROM ");
		StringBuffer whereSql = new StringBuffer("");

		updateSql.append(makeQualifiedName(catalog, schema, tableName, true,
				identifierQuoteString,jdbcConnection));

			int where_cnt = 0;		
			for (int i = 0; i < fieldNames.size(); i++) 
			{										    	
		    	if(isKey[i])
				{
			        if (where_cnt > 0) whereSql.append(" AND ");
			        whereSql.append(quoteIdentifier(fieldNames.get(i), identifierQuoteString));
			        whereSql.append(" = ");
			        whereSql.append("?");
			        where_cnt++;
			        delete_whereIndexMap.put(i, where_cnt);
				}
			}
			
		    if (where_cnt > 0)
		    {
		    	updateSql.append(" WHERE ");
		    	updateSql.append(whereSql);
		    }			
		
		String selectsql = updateSql.toString();
		logger.info("DELETE SQL: " + selectsql);
		return selectsql;
    }


	private static String getWhereClause(List<FilterInfo> filterInfoList, AdvancedFilterInfo advancedFilterInfo, String identifierQuoteString, boolean isLookup) throws ReadException 
	{		
		if(advancedFilterInfo != null && advancedFilterInfo.getFilterCondition() != null && !advancedFilterInfo.getFilterCondition().isEmpty())
		{
			return " WHERE " + advancedFilterInfo.getFilterCondition();
		}else
		if(filterInfoList != null && !filterInfoList.isEmpty())
		{
			StringBuffer query = new StringBuffer();
			int cnt = 0;
			for (FilterInfo filterInfo : filterInfoList) {				
				if(filterInfo.getField() != null && filterInfo.getValues() != null)
				{
					logger.info("filterInfo.getOperator(): "+filterInfo.getOperator());
					logger.info("filterInfo.getField().getDisplayName(): "+filterInfo.getField().getDisplayName());
					logger.info("filterInfo.getValues(): "+filterInfo.getValues());
					
						if(cnt!=0)
							query.append(" AND ");
						else
							query.append(" WHERE ");

						String columnName = JDBCConnectorConstants.unsanitizeName(filterInfo.getField().getDisplayName());
						logger.info("columnName: "+columnName);
						
			        	String clause = EQUALS_OPERATOR;
						if(filterInfo.getOperator() !=null)
							clause = filterOperatorMap.get(filterInfo.getOperator().name());
				        
				        clause = clause.replaceAll(Pattern.quote("{0}"), Matcher.quoteReplacement(quoteIdentifier(columnName, identifierQuoteString)));

				        if(filterInfo.getOperator() != FilterOperation.isNotNull && filterInfo.getOperator() != FilterOperation.isNull)
				        {
					        if(filterInfo.getValues().isEmpty()) //This should never happen				       
						        clause = clause.replaceAll(Pattern.quote("{1}"), Matcher.quoteReplacement("null"));
					        else
					        	clause = clause.replaceAll(Pattern.quote("{1}"), Matcher.quoteReplacement(quoteLiteral((String)filterInfo.getValues().get(0), "'", "'")));
				        }

				        query.append(clause);
					    cnt++;
				}
			}			      
			return query.toString();								
		}else
		{
			return "";				
		}
	}


	private static void setInputData(PreparedStatement pstmt, Object[] data,
			RecordInfo primaryRecordInfo, List<Field> fieldList, List<String> columnNames,  int[] sqlType, String[] dbType, Map<Integer, Integer> fieldIndexMap, Map<Integer, Integer> whereIndexMap)
			throws SQLException 
	{
		for (int i = 0; i < columnNames.size(); i++) 
		{
			
			if(sqlType[i]==0)
				throw new IllegalArgumentException("Field Metadata is missing custom attribute {SQL_DATA_TYPE_ID}");
			
			if(dbType[i]==null || dbType[i].isEmpty())
				throw new IllegalArgumentException("Field Metadata is missing custom attribute {DB_TYPE_NAME}");

			Integer field_index = fieldIndexMap.get(i);
			if(field_index !=null && field_index > 0)
			{ 
				// TODO Handle value for non-jdbc types like oracle.Datum.*
				// TODO Handle java.sql.Types.OTHER
								
				if(data[i]!=null)
				{
					switch (sqlType[i]) {
					case java.sql.Types.OTHER:
						if (data[i] instanceof String)
							pstmt.setString(field_index, (String) data[i]);
						else if (data[i] instanceof byte[])
							pstmt.setBytes(field_index, (byte[]) data[i]);
						else
							// Handle other types
							pstmt.setObject(field_index, data[i], sqlType[i]);
						break;
					default:
						pstmt.setObject(field_index, data[i], sqlType[i]);
						break;
					}
				}else
				{
					switch (sqlType[i]) {
					case java.sql.Types.OTHER:
						//TODO Handle other types
						pstmt.setString(field_index, null);				
						break;
					default:
						pstmt.setNull(field_index, sqlType[i]);				
						break;
					}
				}
			}
			
			Integer where_index = whereIndexMap.get(i);
			if(where_index!=null && where_index > 0)
			{
				// TODO Handle value for non-jdbc types like oracle.Datum.*
				// TODO Handle java.sql.Types.OTHER

				if(data[i]!=null)
				{
					switch (sqlType[i]) {
					case java.sql.Types.OTHER:
						if (data[i] instanceof String)
							pstmt.setString(where_index, (String) data[i]);
						else if (data[i] instanceof byte[])
							pstmt.setBytes(where_index, (byte[]) data[i]);
						else // Handle other types							
							pstmt.setObject(where_index, data[i], sqlType[i]);
						break;
					default:
						pstmt.setObject(where_index, data[i], sqlType[i]);
						break;
					}
				}else
				{
					switch (sqlType[i]) {
					case java.sql.Types.OTHER:
						// TODO Handle other types
						pstmt.setString(where_index, null);
						break;
					default:
						pstmt.setNull(where_index, sqlType[i]);
						break;
					}
				}
			}
		}
	}
	
	private static String getAdjustedSchema(String databaseProductName,String schema) 
	{
		try 
		{
			logger.debug("getAdjustedSchema("+schema+")");
			if(databaseProductName.equalsIgnoreCase("Oracle"))
			{		if(schema !=null && !schema.isEmpty())
						schema = schema.toUpperCase();
			}else if(databaseProductName.equalsIgnoreCase("sqlserver"))
			{
				if(schema ==null || schema.isEmpty())
					schema="dbo";				
			}else if(databaseProductName.equalsIgnoreCase("Microsoft SQL Server"))
			{
				if(schema ==null || schema.isEmpty())
					schema="dbo";								
			}						
		} catch (Throwable e) {
			e.printStackTrace();			
		}					
		return schema;
	}


	public static String getAdjustedCatalog(Connection jdbcConnection, String catalog) {
		if (catalog == null || catalog.isEmpty()) {
			try {
				return jdbcConnection.getCatalog();
			} catch (SQLException e) {
				logger.warn("Failed to get Catalog from DB", e);
				return null;
			}
		} else
			return catalog;
	}
	

	/**
	 * @param jdbcConnection
	 * @return
	 */
	private static String aboutDriverandDatabase(Connection jdbcConnection) 
	{
		if(jdbcConnection==null)
			return "Connection failed";
		
		StringBuffer buf = new StringBuffer();
		try 
		{
			DatabaseMetaData dbmd = jdbcConnection.getMetaData();
			buf.append("\n - - - ");
			buf.append("\nDatabase product name: ");
			try {
				buf.append(dbmd.getDatabaseProductName());
			} catch (SQLException e) {
				e.printStackTrace();
			}
			buf.append("\nDatabase product version: ");
			try {
				buf.append(dbmd.getDatabaseProductVersion());
			} catch (SQLException e) {
				e.printStackTrace();
			}
			buf.append("\nJDBC driver name: ");
			try {
				buf.append(dbmd.getDriverName());
			} catch (SQLException e) {
				e.printStackTrace();
			}
			buf.append("\nJDBC driver version: ");
			try {
				buf.append(dbmd.getDriverVersion());
			} catch (SQLException e) {
				e.printStackTrace();
			}
			buf.append("\nJDBC driver SearchStringEscape: ");
			try {
				buf.append(dbmd.getSearchStringEscape());
			} catch (SQLException e) {
				e.printStackTrace();
			}
			buf.append("\nJDBC driver IdentifierQuoteString: ");
			try {
				buf.append(dbmd.getIdentifierQuoteString());
			} catch (SQLException e) {
				e.printStackTrace();
			}
			buf.append("\nJDBC driver supportsSchemasInTableDefinitions: ");
			try {
				buf.append(dbmd.supportsSchemasInTableDefinitions());
			} catch (SQLException e) {
				e.printStackTrace();
			}
			buf.append("\nJDBC driver supportsCatalogsInTableDefinitions: ");
			try {
				buf.append(dbmd.supportsCatalogsInTableDefinitions());
			} catch (SQLException e) {
				e.printStackTrace();
			}
			buf.append("\nJDBC driver supportsCatalogsInDataManipulation: ");
			try {
				buf.append(dbmd.supportsCatalogsInDataManipulation());
			} catch (SQLException e) {
				e.printStackTrace();
			}
			buf.append("\nJDBC driver supportsSchemasInDataManipulation: ");
			try {
				buf.append(dbmd.supportsSchemasInDataManipulation());
			} catch (SQLException e) {
				e.printStackTrace();
			}
			buf.append("\n - - - \n");
		} catch (Throwable t) {
			t.printStackTrace();
		}
		return buf.toString();
	}



	public static void flush() {
		System.out.println("JDBCUtils.flush(), flush record count="+currentBatch);
		if(insert_pstmt != null && currentBatch > 0){
			try {
				int[] results = insert_pstmt.executeBatch();
				dbConnection.commit();
			int flushCount = 0;
			System.out.println("JDBCUtils.flush(), resultcount="+results.length);
			for(int result: results){
				if(result == Statement.SUCCESS_NO_INFO || result > 0){
					flushCount++;
				}				
				
			}
			System.out.println("JDBCUtils.flush(),successfully flushed =" + flushCount);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}

		
	}


}
