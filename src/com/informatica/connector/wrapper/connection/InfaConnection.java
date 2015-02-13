package com.informatica.connector.wrapper.connection;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.informatica.cloud.api.adapter.connection.ConnectionFailedException;
import com.informatica.cloud.api.adapter.connection.IConnection;
import com.informatica.cloud.api.adapter.connection.InsufficientConnectInfoException;
import com.informatica.cloud.api.adapter.plugin.InvalidArgumentException;
import com.informatica.connector.wrapper.util.ConnectionAttributeUtil;
import com.informatica.connector.wrapper.util.ISimpleConnector;

public class InfaConnection implements IConnection {

	//private static final boolean islog4JConfigured = ConnectorUtils.configureLog4j();
	private static final Log logger = LogFactory.getLog(InfaConnection.class);

	private ISimpleConnector connectorImpl = null;
    private Map<String, String> connParams = null;
    private boolean isConnected = false;
    
	public InfaConnection(ISimpleConnector connectorImpl) {
			this.connectorImpl = connectorImpl;
	}

	@Override
	public boolean connect() throws InsufficientConnectInfoException,ConnectionFailedException 
	{
		logger.debug("InfaConnection.connect()");			
		try
		{
			this.isConnected = connectorImpl.connect();
			return this.isConnected;
		} catch (RuntimeException t) {
			t.printStackTrace();
			ConnectionFailedException ex =  new ConnectionFailedException(t.toString());
			ex.initCause(t);
			throw ex;			
		}catch(Throwable t)
		{
			t.printStackTrace();
			if(t instanceof InsufficientConnectInfoException)
			{
				throw (InsufficientConnectInfoException)t;
			}
			if(t instanceof ConnectionFailedException)
			{
				throw (ConnectionFailedException)t;
			}else
			{
				ConnectionFailedException ex =  new ConnectionFailedException(t.toString());
				ex.initCause(t);
				throw ex;
			}
		}		
	}

	@Override
	public boolean disconnect() {
		logger.debug("InfaConnection.disconnect()");
		try
		{			
			if(this.isConnected)
			{
				this.isConnected =  !connectorImpl.disconnect();
				this.connParams = null;
				return !this.isConnected;
			}else
			{
				this.connParams = null;
				return this.isConnected;
			}
		}catch(Throwable t)
		{
			t.printStackTrace();
			return false;
		}			
	}

	@Override
	public void setConnectionAttributes(Map<String, String> connParams) {
		logger.debug("InfaConnection.setConnectionAttributes()");
		this.connParams = connParams;
		ConnectionAttributeUtil.setConnectionAttributes(connectorImpl, connParams);		
	}

	@Override
	public boolean validate() throws InvalidArgumentException {
		logger.debug("InfaConnection.validate()");
		try {
			return connectorImpl.connect();
		}catch(Throwable t)
		{
			t.printStackTrace();
			throw new com.informatica.cloud.api.adapter.plugin.InvalidArgumentException(t.toString());
		}
	}

	public ISimpleConnector getConnectorImpl() {
		return connectorImpl;
	}

	public Map<String, String> getConnParams() {
		return connParams;
	}

	public void setConnParams(Map<String, String> connParams) {
		this.connParams = connParams;
	}

	public boolean isConnected() {
		return isConnected;
	}

	public void setConnected(boolean isConnected) {
		this.isConnected = isConnected;
	}

}
