package com.informatica.connector.sample.jdbc;

import com.informatica.cloud.api.adapter.plugin.PluginVersion;
import com.informatica.connector.wrapper.plugin.InfaPlugin;

public class JDBCConnectorDescriptor extends InfaPlugin {
	
	/**
	 *  Make sure you call super with an instance of the class that implements ISimpleConnector
	 */
	public JDBCConnectorDescriptor() {
		//TODO - 1.0 INFA: Call super with an instance of the class that implements ISimpleConnectors
		super(new JDBCConnectorImpl());		
	}

	@Override
	public PluginVersion getVersion() {
		return new PluginVersion(1, 0, 1);
	}

}
