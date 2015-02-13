package com.informatica.connector.sample.jdbc.test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.informatica.cloud.api.adapter.connection.ConnectionAttribute;
import com.informatica.cloud.api.adapter.connection.ConnectionAttributeType;
import com.informatica.cloud.api.adapter.connection.ConnectionFailedException;
import com.informatica.cloud.api.adapter.connection.IConnection;
import com.informatica.cloud.api.adapter.connection.InsufficientConnectInfoException;
import com.informatica.cloud.api.adapter.metadata.IMetadata;
import com.informatica.cloud.api.adapter.metadata.IRegistrationInfo;
import com.informatica.cloud.api.adapter.metadata.RecordAttribute;
import com.informatica.cloud.api.adapter.plugin.IPlugin;
import com.informatica.connector.sample.jdbc.JDBCConnectorDescriptor;
import com.informatica.connector.wrapper.util.TestUtils;

public class InfaConnectionTest {

	// TODO SETUP your test parameters ********************************
	
	IPlugin plugin = new JDBCConnectorDescriptor();

	// END TODO *******************************************************

	//DO NOT MAKE ANY CHANGES BELOW THIS
	Map<String, String> connAttribs = new HashMap<String, String>();;
	IRegistrationInfo registrationInfo = null;
	IConnection con = null;
	IMetadata metaData = null;

	List<RecordAttribute> recordAttributeList = null;	
	RecordAttribute recordAttribute = null;

	
	@Before
	public void setUp() throws Exception {	
		registrationInfo = plugin.getRegistrationInfo();
		assertNotNull("PluginID is null",registrationInfo.getPluginUUID());
		assertNotNull("PluginShortName is null",registrationInfo.getPluginShortName());
		connAttribs.putAll(TestUtils.getConnectionAtributes(registrationInfo));		
		recordAttributeList  = plugin.getRegistrationInfo().getRecordAttributes();
		assertNotNull("Record Attribute List cannot be null",recordAttributeList);
		for (RecordAttribute attrib : recordAttributeList) {
			System.out.println("-------------------Record Attribute----------------------");
			System.out.println(attrib.getId());
			System.out.println(attrib.getScope());
			System.out.println(attrib.getDatatype());
			System.out.println(attrib.getDefaultValue());
			System.out.println(attrib.getDescription());
			System.out.println(attrib.getGroupName());
			System.out.println(attrib.getName());
			System.out.println("-----------------------------                ------------");
		}
	}

	
	@Test
	public void testConnect() {
		List<ConnectionAttribute> connectionAttributeList = registrationInfo.getConnectionAttributes();
		assertNotNull("No ConnectionAttributes found",connectionAttributeList);
		assertTrue("No ConnectionAttributes found",!connectionAttributeList.isEmpty());
		
		System.out.print("Attribute Name");
		System.out.print("\t\t\t"+"Type");
		System.out.print("\t\t"+"isMandatory");
		System.out.print("\t\t"+"DefaultValue");
		System.out.println("\t\t\t"+"ListValues");			

		for (ConnectionAttribute ca : connectionAttributeList) 
		{
			System.out.print(ca.getName());
			System.out.print("\t\t\t"+ca.getType());
			System.out.print("\t\t"+ca.isMandatory());
			System.out.print("\t\t"+ca.getDefaultValue());
			System.out.println("\t\t\t"+ca.getListValues());
			if(ca.getType() == ConnectionAttributeType.LIST_TYPE)
			{
				assertNotNull("ConnectionAttributeType.LIST_TYPE must have ListValues",ca.getListValues());
			}
		}		
		
		con = plugin.getConnection();		
		con.setConnectionAttributes(connAttribs);
		try {
			assertTrue("Connection Failed",con.connect());
			System.out.println("*** Connection Successfull ***");
			assertTrue("Disconnect Failed",con.disconnect());
			System.out.println("*** Disconnect Successfull ***");
		} catch (InsufficientConnectInfoException e) {
			e.printStackTrace();
			fail(e+"");
		} catch (ConnectionFailedException e) {
			e.printStackTrace();
			fail(e+"");
		}
	}

}
