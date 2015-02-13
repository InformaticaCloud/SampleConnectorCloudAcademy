package com.informatica.connector.sample.jdbc.test;


import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.informatica.cloud.api.adapter.common.OperationContext;
import com.informatica.cloud.api.adapter.connection.ConnectionAttribute;
import com.informatica.cloud.api.adapter.connection.ConnectionFailedException;
import com.informatica.cloud.api.adapter.metadata.Field;
import com.informatica.cloud.api.adapter.metadata.IMetadata;
import com.informatica.cloud.api.adapter.metadata.IRegistrationInfo;
import com.informatica.cloud.api.adapter.metadata.MetadataReadException;
import com.informatica.cloud.api.adapter.metadata.RecordInfo;
import com.informatica.cloud.api.adapter.plugin.IPlugin;
import com.informatica.cloud.api.adapter.runtime.IRead;
import com.informatica.cloud.api.adapter.runtime.exception.DataConversionException;
import com.informatica.cloud.api.adapter.runtime.exception.FatalRuntimeException;
import com.informatica.cloud.api.adapter.runtime.exception.ReadException;
import com.informatica.cloud.api.adapter.runtime.exception.ReflectiveOperationException;
import com.informatica.connector.sample.jdbc.JDBCConnectorDescriptor;
import com.informatica.connector.wrapper.connection.InfaConnection;
import com.informatica.connector.wrapper.util.OutputDataBufferImpl;
import com.informatica.connector.wrapper.util.TestUtils;

public class InfaReadTest {
	
	// TODO SETUP your test parameters ********************************

	//Specify your connector descriptor class
	IPlugin plugin = new JDBCConnectorDescriptor();

	// Set to true if you want to test all Objects, otherwise it will use
	// Object name specified in parameter 'WriteNames' from the ini file 
	// in INIFiles folder
	boolean testAllObjects = false;

	// Semicolon separated list of regex patterns used to exclude object
	// names from test (only applicable when testAllObjects = true)
	//String ignoreObjectPattern = "Apex.*;.*Document.*;Group;Folder;.*Permission.*;.*History.*;StaticResource;SetupEntityAccess";
	String ignoreObjectPattern = "ALL_TYPES";

	// regex pattern (single) that is used to match object 
	// ( ".*" means match all objects)
	String includeObjectPattern = ".*";

	// Optional Specify your write Operation Attributes (name, value)
	// For Example: 
	// String[][] operationAttributes = new String[][] {
	// {"PRIMARY KEY OVERRIDE","ID" } };
	String[][] operationAttributes = null;

	// END TODO *******************************************************
	
	//DO NOT MAKE ANY CHANGES BELOW THIS	
	Map<String, String> connAttribs = new HashMap<String, String>();
	InfaConnection con = null;
	IMetadata metaData = null;
	IRead iRead = null;	
	IRegistrationInfo registrationInfo = null;
	HashMap<String,String> opAttribs = new HashMap<String,String>();	
	List<String> sourceRecordNameList = null;
	
	@Before
	public void setUp() throws Exception {
		registrationInfo = plugin.getRegistrationInfo();		
		assertNotNull("PluginID is null",registrationInfo.getPluginUUID());
		assertNotNull("PluginShortName is null",registrationInfo.getPluginShortName());

		connAttribs.putAll(TestUtils.getConnectionAtributes(registrationInfo));		
		assertTrue("No ConnectionAttributes found in test ini file",!connAttribs.isEmpty());

		List<ConnectionAttribute> connectionAttributeList = registrationInfo.getConnectionAttributes();
		assertNotNull("No ConnectionAttributes found",connectionAttributeList);
		assertTrue("No ConnectionAttributes found",!connectionAttributeList.isEmpty());

		plugin.setContext(OperationContext.READ);		

		con = (InfaConnection) plugin.getConnection();
		assertNotNull("Connection is null",con);
		con.setConnectionAttributes(connAttribs);
		assertTrue("connect() returned false",con.connect());

		metaData = plugin.getMetadata(con);
		assertNotNull("Metadata is null",metaData);		
		
		iRead = plugin.getReader(con);		
		assertNotNull("iRead is null",iRead);		

		sourceRecordNameList = TestUtils.getSourceRecords(registrationInfo);
		assertNotNull("No ReadNames defined in ini file",sourceRecordNameList);
		assertTrue("No ReadNames defined in ini file",!sourceRecordNameList.isEmpty());
	}

	@Test
	public void testRead() {
		try
		{

			Pattern pattern = Pattern.compile(".*");
			if(includeObjectPattern!=null)
				pattern = Pattern.compile(includeObjectPattern);

			Pattern[] ignorePattern = null;
			if(ignoreObjectPattern!=null)
			{
				String temp[] = ignoreObjectPattern.split(";");
				if(temp!=null)
				{
					ignorePattern = new Pattern[temp.length];
					for(int i=0;i<temp.length;i++)
					{
						ignorePattern[i] = Pattern.compile(temp[i]);
					}
				}
			}			
			
			plugin.setContext(OperationContext.READ);
			List<RecordInfo> list = metaData.filterRecords(pattern);
			assertNotNull("GetAllRecords should return atleast one Record ", list);
			assertTrue("GetAllRecords should return atleast one Record ",!list.isEmpty());
			RecordInfo selectedRecord = null;
			for (RecordInfo r : list) 
			{
				assertNotNull("CatalogName in RecordInfo is null",r.getCatalogName());
				assertNotNull("RecordName in RecordInfo is null",r.getRecordName());

				boolean ignore = false;
				if(testAllObjects && ignorePattern != null)
				{	
					for(int i=0;i<ignorePattern.length;i++)
						if(ignorePattern[i].matcher(r.getRecordName()).matches())
							ignore = true;
				}
				
				if(ignore)
					continue;

				if(testAllObjects || sourceRecordNameList.contains(r.getRecordName()))				
					selectedRecord = r;
				else
					continue;		
			
				System.out.println(" *-* "+ selectedRecord.getCatalogName()+ " *-* ");								
				List<Field> fields = metaData.getFields(selectedRecord, true);
				assertNotNull("metaData.getFields() returned field list is null",fields);
				assertTrue("metaData.getFields() returned field list is empty",!fields.isEmpty());
				for (Field f : fields) 
				{
					assertNotNull("Field.getContainingRecord() returned null",f.getContainingRecord());
					assertNotNull("Field.getDisplayName() returned null",f.getDisplayName());
					assertNotNull("Field.getJavaDatatype() returned null",f.getJavaDatatype());
					assertNotNull("Field.getDatatype() returned null",f.getDatatype());
					assertTrue("Field.getPrecision() is not > 0",f.getPrecision()>0);
					assertTrue("Field.getScale() is < -1",f.getScale()>=0);
				}
				
				iRead.setPrimaryRecord(selectedRecord);
				iRead.setFieldList(fields);
				if(operationAttributes!=null)
				{					
					for(String[] opat:operationAttributes)
					{
						opAttribs.put(opat[0], opat[1]);
					}
					if(!opAttribs.isEmpty())
						iRead.setOperationAttributes(opAttribs);
				}
								
			    String fileName = selectedRecord.getRecordName() + ".csv";
				OutputDataBufferImpl iodb = new OutputDataBufferImpl(fileName, fields);
				try {
					iRead.read(iodb);
				} catch (ConnectionFailedException e) {
					e.printStackTrace();
					fail(e.toString());
				} catch (ReflectiveOperationException e) {
					e.printStackTrace();
					fail(e.toString());
				} catch (ReadException e) {
					e.printStackTrace();
					fail(e.toString());
				} catch (DataConversionException e) {
					e.printStackTrace();
					fail(e.toString());
				} catch (FatalRuntimeException e) {
					e.printStackTrace();
					fail(e.toString());
				}								
				iodb.flush();				
			}
			if(selectedRecord==null)
				System.err.println("No Object Found with name {"+sourceRecordNameList+"}");
			assertNotNull("No Object Found with name {"+sourceRecordNameList+"}",selectedRecord);			
		} catch (MetadataReadException e) {
				e.printStackTrace();
				fail(e.toString());
		} catch (IOException e) {
			e.printStackTrace();
			fail(e.toString());
		}

	}
	
	  @After
	  public void tearDown() {
		  if(con!=null)
			  con.disconnect();
	  }	

}
