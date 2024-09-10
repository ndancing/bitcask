package com.github.ndancing.kv.client.socket;

import java.io.IOException;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;

public final class SocketClientFactory implements PooledObjectFactory<SocketClient> {

	private final String host;
	private final int port;
	private final int timeOutMils;

	public SocketClientFactory(String host, int port, int timeOutMils) {
		this.host = host;
		this.port = port;
		this.timeOutMils = timeOutMils;
	}

	@Override
	public PooledObject<SocketClient> makeObject() throws IOException {
		return new DefaultPooledObject<>(new SocketClient(this.host, this.port, this.timeOutMils));
	}

	@Override
	public void destroyObject(PooledObject<SocketClient> p) {
		p.getObject().close();
	}

	@Override
	public boolean validateObject(PooledObject<SocketClient> p) {
		return p.getObject().isValid();
	}

	@Override
	public void activateObject(PooledObject<SocketClient> p) {
		p.getObject().activate();
	}

	@Override
	public void passivateObject(PooledObject<SocketClient> p) {
		p.getObject().passivate();
	}
}
