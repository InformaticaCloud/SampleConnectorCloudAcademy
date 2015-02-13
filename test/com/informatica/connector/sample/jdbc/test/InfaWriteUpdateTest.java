package com.informatica.connector.sample.jdbc.test;


import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
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
import com.informatica.cloud.api.adapter.runtime.IWrite;
import com.informatica.cloud.api.adapter.runtime.exception.DataConversionException;
import com.informatica.cloud.api.adapter.runtime.exception.FatalRuntimeException;
import com.informatica.cloud.api.adapter.runtime.exception.ReflectiveOperationException;
import com.informatica.cloud.api.adapter.runtime.exception.WriteException;
import com.informatica.cloud.api.adapter.runtime.utils.OperationResult;
import com.informatica.connector.sample.jdbc.JDBCConnectorDescriptor;
import com.informatica.connector.wrapper.connection.InfaConnection;
import com.informatica.connector.wrapper.util.InputDataBufferImpl;
import com.informatica.connector.wrapper.util.TestUtils;

public class InfaWriteUpdateTest {
	
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

	// Flag to tell the test if ObjectName_Update.csv is not found then use
	// ObjectName.csv as input
	boolean useReadTestOutputAsInputForWrite = true;

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
	IWrite iWrite = null;
	HashMap<String,String> opAttribs = new HashMap<String,String>();	
	IRegistrationInfo registrationInfo = null;
	List<String> targetRecordNameList = null;
	
		
	
	@Before
	public void setUp() throws Exception {
		//plugin = new JDBCConnectorDescriptor();
		registrationInfo = plugin.getRegistrationInfo();		
		assertNotNull("PluginID is null",registrationInfo.getPluginUUID());
		assertNotNull("PluginShortName is null",registrationInfo.getPluginShortName());

		connAttribs.putAll(TestUtils.getConnectionAtributes(registrationInfo));		
		assertTrue("No ConnectionAttributes found in test ini file",!connAttribs.isEmpty());

		List<ConnectionAttribute> connectionAttributeList = registrationInfo.getConnectionAttributes();
		assertNotNull("No ConnectionAttributes found",connectionAttributeList);
		assertTrue("No ConnectionAttributes found",!connectionAttributeList.isEmpty());
	
		plugin.setContext(OperationContext.WRITE);		

		con = (InfaConnection) plugin.getConnection();
		assertNotNull("Connection is null",con);
		con.setConnectionAttributes(connAttribs);
		assertTrue("connect() returned false",con.connect());

		metaData = plugin.getMetadata(con);
		assertNotNull("Metadata is null",metaData);		
		
		iWrite = plugin.getWriter(con);
		assertNotNull("iWrite is null",iWrite);		

		targetRecordNameList = TestUtils.getTargetRecords(registrationInfo);
		assertNotNull("No WriteNames defined in ini file",targetRecordNameList);
		assertTrue("No WriteNames defined in ini file",!targetRecordNameList.isEmpty());

	}

	@Test
	public void testUpdate() {
		try
		{
			String TestInterface = "Update"; //Valid values are Write/Update/Upsert/Delete

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
			
			plugin.setContext(OperationContext.WRITE);
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
				
				if(testAllObjects || targetRecordNameList.contains(r.getRecordName()))				
					selectedRecord = r;
				else
					continue;

				System.out.println(" *-* "+ selectedRecord.getCatalogName()+ " *-* ");								
				List<Field> fields = metaData.getFields(selectedRecord, true);
				assertNotNull("metaData.getFields() returned field list is null",fields);
				assertTrue("metaData.getFields() returned empty field list",!fields.isEmpty());
				for (Field f : fields) 
				{
					assertNotNull("Field.getContainingRecord() returned null",f.getContainingRecord());
					assertNotNull("Field.getDisplayName() returned null",f.getDisplayName());
					assertNotNull("Field.getJavaDatatype() returned null",f.getJavaDatatype());
					assertNotNull("Field.getDatatype() returned null",f.getDatatype());
					assertTrue("Field.getPrecision() is not > 0",f.getPrecision()>0);
					assertTrue("Field.getScale() is < -1",f.getScale()>=0);
				}
				
				iWrite.setPrimaryRecord(selectedRecord);
				iWrite.setFieldList(fields);
				if(operationAttributes!=null)
				{					
					for(String[] opat:operationAttributes)
					{
						opAttribs.put(opat[0], opat[1]);
					}
					if(!opAttribs.isEmpty())
						iWrite.setOperationAttributes(opAttribs);
				}
			    String fileName = selectedRecord.getRecordName() + (TestInterface.isEmpty() ?  "": "_" + TestInterface) + ".csv";
			    File targetFile = new File("CSV",fileName);
			    if(!targetFile.exists())
			    {
					assertTrue("File {"+targetFile+"} does not exist",useReadTestOutputAsInputForWrite);
					File sourceFile = new File("CSV", selectedRecord.getRecordName()+".csv");
					assertTrue("File {"+sourceFile+"} does not exist. Run the Read test first",sourceFile.exists());
					TestUtils.CopyFile(sourceFile, targetFile);
			    }
				InputDataBufferImpl iidb = new InputDataBufferImpl(fileName, fields);
				List<OperationResult> result = null;
				try {					
					if(TestInterface.equals("Write")) {
						result = iWrite.insert(iidb);
					}else if(TestInterface.equals("Update")) {
						result = iWrite.update(iidb);
					}else if(TestInterface.equals("Upsert")) {
						result = iWrite.upsert(iidb);
					}else if(TestInterface.equals("Delete")) {
						result = iWrite.delete(iidb);
					}					
				} catch (ConnectionFailedException e) {
					e.printStackTrace();
					fail(e.toString());
				} catch (ReflectiveOperationException e) {
					e.printStackTrace();
					fail(e.toString());
				} catch (WriteException e) {
					e.printStackTrace();
					fail(e.toString());
				} catch (DataConversionException e) {
					e.printStackTrace();
					fail(e.toString());
				} catch (FatalRuntimeException e) {
					e.printStackTrace();
					fail(e.toString());
				}
				assertNotNull("OperationResult is null",result);				
				assertTrue("List<OperationResult> is not equal to Number of rows in buffer",result.size()==iidb.getRowCount());
				for(OperationResult or:result)
				{
					assertTrue(or.getException()!=null?or.getException().toString():"task failed but did not return any Exception",or.getValue());
				}
			}
			if(selectedRecord==null)
				System.err.println("No Object Found with name {"+targetRecordNameList+"}");
			assertNotNull("No Object Found with name {"+targetRecordNameList+"}",selectedRecord);						
		} catch (MetadataReadException e) {
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