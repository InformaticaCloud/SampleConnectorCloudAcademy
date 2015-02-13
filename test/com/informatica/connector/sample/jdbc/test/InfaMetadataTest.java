package com.informatica.connector.sample.jdbc.test;


import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.informatica.cloud.api.adapter.common.OperationContext;
import com.informatica.cloud.api.adapter.connection.ConnectionAttribute;
import com.informatica.cloud.api.adapter.connection.IConnection;
import com.informatica.cloud.api.adapter.metadata.DataPreviewException;
import com.informatica.cloud.api.adapter.metadata.Field;
import com.informatica.cloud.api.adapter.metadata.FieldInfo;
import com.informatica.cloud.api.adapter.metadata.IMetadata;
import com.informatica.cloud.api.adapter.metadata.IRegistrationInfo;
import com.informatica.cloud.api.adapter.metadata.MetadataReadException;
import com.informatica.cloud.api.adapter.metadata.RecordInfo;
import com.informatica.cloud.api.adapter.plugin.IPlugin;
import com.informatica.connector.sample.jdbc.JDBCConnectorDescriptor;
import com.informatica.connector.wrapper.util.TestUtils;

public class InfaMetadataTest {

	// TODO SETUP your test parameters ********************************

	//Specify your connector descriptor class
	IPlugin plugin = new JDBCConnectorDescriptor();

	// Set to true if you want to test all Objects, otherwise it will use
	// Object name specified in parameter 'WriteNames' from the ini file 
	// in INIFiles folder
	boolean testAllObjects = false;

	// Semicolon separated list of regex patterns used to exclude object
	// names from test (only applicable when testAllObjects = true)
	//String ignoreObjectPattern = "Apex.*;.*Document.*;.*Permission.*;.*History.*;StaticResource;SetupEntityAccess";
	String ignoreObjectPattern = "ALL_TYPES";
	
	// regex pattern (single) that is used to match object 
	// ( ".*" means match all objects)
	String includeObjectPattern = ".*";

	// END TODO *******************************************************

	//DO NOT MAKE ANY CHANGES BELOW THIS	
	Map<String, String> connAttribs = new HashMap<String, String>();
	IRegistrationInfo registrationInfo = null;
	IConnection con = null;
	IMetadata metaData = null;
	List<String> targetRecordNameList = null;
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
		
		con = plugin.getConnection();
		assertNotNull("Connection is null",con);
		con.setConnectionAttributes(connAttribs);
		assertTrue("connect() returned false",con.connect());
		
		metaData = plugin.getMetadata(con);
		assertNotNull("Metadata is null",metaData);	
		
		targetRecordNameList = TestUtils.getTargetRecords(registrationInfo);
		assertNotNull("No WriteNames defined in ini file",targetRecordNameList);
		assertTrue("No WriteNames defined in ini file",!targetRecordNameList.isEmpty());

		sourceRecordNameList = TestUtils.getSourceRecords(registrationInfo);
		assertNotNull("No ReadNames defined in ini file",sourceRecordNameList);
		assertTrue("No ReadNames defined in ini file",!sourceRecordNameList.isEmpty());

	}

	@Test
	public void testGetAllRecords() {
		try {
			plugin.setContext(OperationContext.READ);
			List<RecordInfo> list = metaData.getAllRecords();
			assertNotNull("GetAllRecords should return atleast one Record ", list);
			assertTrue("GetAllRecords should return atleast one Record ",!list.isEmpty());
			for (RecordInfo r : list) {
				System.out.println(r.getCatalogName());				
				assertNotNull("CatalogName in RecordInfo is null",r.getCatalogName());
				assertNotNull("RecordName in RecordInfo is null",r.getRecordName());
			}
		} catch (MetadataReadException e) {
			e.printStackTrace();
			fail(e.toString());
		}
	}

	@Test
	public void testGetFields() 
	{
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
			assertNotNull("No Object Found with pattern {pattern}",list);						
			assertTrue("No Object Found with pattern {pattern}",!list.isEmpty());
			RecordInfo selectedRecord = null;
			for (RecordInfo r : list) 
			{
				//System.out.println(r.getCatalogName());
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

				System.out.println(" *-* "+ r.getRecordName() + " *-* ");				
	
				if(testAllObjects || targetRecordNameList.contains(r.getRecordName()) || sourceRecordNameList.contains(r.getRecordName()))				
					selectedRecord = r;
				else
					continue;
				
				System.out.println(" *-* "+ selectedRecord.getCatalogName()+ " *-* ");				

				List<Field> fields = metaData.getFields(selectedRecord, true);

				System.out.print("RecordName" + "\t\t\t");
				System.out.print("DisplayName" + "\t\t\t");
				System.out.print("JavaDatatype" + "\t\t\t");
				System.out.print("Datatype" + "\t\t\t");
				System.out.print("Precision"  + "\t\t\t");
				System.out.println("Scale");
								
				for (Field f : fields) 
				{
					System.out.print(f.getContainingRecord().getCatalogName() + "\t\t\t");
					System.out.print(f.getDisplayName() + "\t\t\t");
					System.out.print(f.getJavaDatatype() + "\t\t\t");
					System.out.print(f.getDatatype() + "\t\t\t");
					System.out.print(f.getPrecision()  + "\t\t\t");
					System.out.println(f.getScale());
				}

				for (Field f : fields) 
				{
					assertNotNull("Field.getContainingRecord() returned null",f.getContainingRecord());
					assertNotNull("Field.getDisplayName() returned null",f.getDisplayName());
					assertNotNull("Field.getJavaDatatype() returned null",f.getJavaDatatype());
					assertNotNull("Field.getDatatype() returned null",f.getDatatype());
					assertTrue("Field.getPrecision() is not > 0",f.getPrecision()>0);
					assertTrue("Field.getScale() is < -1",f.getScale()>=0);
				}
			}
			if(selectedRecord==null)
				System.err.println("No Object Found with name {"+sourceRecordNameList+","+targetRecordNameList+"}");
			assertNotNull("No Object Found with name {"+sourceRecordNameList+","+targetRecordNameList+"}",selectedRecord);						
		} catch (MetadataReadException e) {
			e.printStackTrace();
			fail(e.toString());
		}
		
	}
	
	@Test
	public void testGetDataPreview() 
	{
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
			assertNotNull("No Object Found with pattern {pattern}",list);						
			assertTrue("No Object Found with pattern {pattern}",!list.isEmpty());
			RecordInfo selectedRecord = null;
			for (RecordInfo r : list) 
			{
				//System.out.println(r.getCatalogName());
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
				
				if(testAllObjects || targetRecordNameList.contains(r.getRecordName()) || sourceRecordNameList.contains(r.getRecordName()))				
					selectedRecord = r;
				else
					continue;
				
				System.out.println(" *-* "+ selectedRecord.getCatalogName()+ " *-* ");				
				List<FieldInfo> fieldInfoList = new ArrayList<FieldInfo>();
				String[][] dataRows = metaData.getDataPreview(selectedRecord,5, fieldInfoList);	
				assertNotNull("FieldList is null",fieldInfoList);
				assertTrue("FieldList is empty",!fieldInfoList.isEmpty());
				for(FieldInfo fi:fieldInfoList)
				{
					System.out.print(fi.getDisplayName()+"\t\t\t");				
				}
				System.out.println("");										
				for(int i=0;i<dataRows.length;i++)
				{
					for(int j=0;j<dataRows[i].length;j++)
					{
						System.out.print(dataRows[i][j]+"\t\t\t");
					}
					System.out.println("");										
				}

				for(int i=0;i<dataRows.length;i++)
				{
					assertTrue(dataRows[i].length==fieldInfoList.size());
				}				
			}				
		} catch (DataPreviewException e) {
			e.printStackTrace();
			fail(e.toString());
		} catch (MetadataReadException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	

	  @After
	  public void tearDown() {
		  if(con!=null)
			  assertTrue("disconnect() returned false",con.disconnect());
	  }	

}
