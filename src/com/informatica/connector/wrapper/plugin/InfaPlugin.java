package com.informatica.connector.wrapper.plugin;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.informatica.cloud.api.adapter.common.ILogger;
import com.informatica.cloud.api.adapter.common.OperationContext;
import com.informatica.cloud.api.adapter.connection.IConnection;
import com.informatica.cloud.api.adapter.metadata.Capability;
import com.informatica.cloud.api.adapter.metadata.IMetadata;
import com.informatica.cloud.api.adapter.metadata.IRegistrationInfo;
import com.informatica.cloud.api.adapter.plugin.IPlugin;
import com.informatica.cloud.api.adapter.runtime.IRead;
import com.informatica.cloud.api.adapter.runtime.IWrite;
import com.informatica.connector.wrapper.connection.InfaConnection;
import com.informatica.connector.wrapper.metadata.InfaMetadata;
import com.informatica.connector.wrapper.metadata.InfaRegistrationInfo;
import com.informatica.connector.wrapper.read.InfaRead;
import com.informatica.connector.wrapper.util.ConnectorUtils;
import com.informatica.connector.wrapper.util.ISimpleConnector;
import com.informatica.connector.wrapper.write.InfaWrite;

public abstract class InfaPlugin implements IPlugin {
	
	//private static final boolean islog4JConfigured = ConnectorUtils.configureLog4j();	
	private static final boolean isJdk14LoggerConfigured = ConnectorUtils.configureLog4j();	
		
	private static Log logger = LogFactory.getLog(InfaPlugin.class);;
	
	private IRegistrationInfo iRegInfo;
	private IConnection iConnection;
	private IMetadata iMetadata;
	private ILogger iLogger;
	private OperationContext context;
	private IRead iReader;
	private IWrite iWriter;

	//private Class<?> connectionImplClass = null;		
	private ISimpleConnector connectorImpl = null;

	/**
	 * @deprecated, replaced by {@link #InfaPlugin(ISimpleConnector connectorImpl)}
	 */	
	@Deprecated public InfaPlugin(Class<?> connectionImplClass) {
		logger.debug("*#*#*#*#* InfaPlugin("+connectionImplClass.getCanonicalName()+") *#*#*#*#* ");
		try {
			this.connectorImpl = (ISimpleConnector) connectionImplClass.newInstance();
			init();
		}catch(Throwable t)
		{
			t.printStackTrace();
		}		
	}

	/**
	 * @param ISimpleConnector
	 */
	public InfaPlugin(ISimpleConnector connectorImpl) {
		logger.debug("InfaPlugin("+connectorImpl.getClass().getCanonicalName()+")");
		logger.debug("tookit wrapper version: 1.1.0");
		this.connectorImpl = connectorImpl;
		init();
	}

	private void init() {
		logger.debug("InfaPlugin.init()");
		iRegInfo = new InfaRegistrationInfo(this.connectorImpl);		
		iConnection = new InfaConnection(this.connectorImpl);
		iMetadata = new InfaMetadata(this, (InfaConnection)iConnection);
	}

	
	@Override
	public IConnection getConnection() {
		logger.debug("InfaPlugin("+this.getClass().getCanonicalName()+").getConnection()");		
		if(iConnection == null){
			iConnection = new InfaConnection(this.connectorImpl);
		}
		return iConnection;
	}

	@Override
	public IMetadata getMetadata(IConnection conn) {
		logger.debug("InfaPlugin("+this.getClass().getCanonicalName()+").getMetadata()");		
		if (iMetadata == null) {
			iMetadata = new InfaMetadata(this, (InfaConnection)getConnection());
		}
		return iMetadata;	}

	@Override
	public IRegistrationInfo getRegistrationInfo() {
		if(iRegInfo == null){
			iRegInfo = new InfaRegistrationInfo(this.connectorImpl);
		}
		return iRegInfo;
	}

	@Override
	public void setContext(OperationContext context) {
		this.context = context;
	}

	public OperationContext getContext() {
		return this.context;
	}

	@Override
	public void setLogger(ILogger ilogger) {
		this.iLogger = ilogger;		
	}

	@Override
	public IRead getReader(IConnection conn) {
		if (iReader == null) {
			iReader = new InfaRead(this, (InfaConnection)conn);
		}
		return iReader;
	}
	
	@Override
	public IWrite getWriter(IConnection conn){
		if (iWriter == null) {
			iWriter = new InfaWrite(this, (InfaConnection)conn);
		}
		return iWriter;
	}
	
	public ILogger getLogger() {
		return this.iLogger;
	}

	/*
	@Override
	public PluginVersion getVersion() {
		return new PluginVersion(1, 0, 1);
	}
	 */

	@Override
	public List<Capability> getCapabilities() {
		List<Capability> capabilities = new ArrayList<Capability>();
		capabilities.add(Capability.SINGLE_OBJECT_READ);
		capabilities.add(Capability.SINGLE_OBJECT_WRITE);
		return capabilities;
	}

	public static boolean isIsjdk14loggerconfigured() {
		return isJdk14LoggerConfigured;
	}

}
