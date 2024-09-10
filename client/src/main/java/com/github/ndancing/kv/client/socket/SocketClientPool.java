package com.github.ndancing.kv.client.socket;

import com.github.ndancing.kv.client.ClientPool;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SocketClientPool implements ClientPool<SocketClient> {

	private static final Logger LOGGER = LoggerFactory.getLogger(SocketClientPool.class);

	private final SocketClientObjectPool socketClientPool;

	private SocketClientPool(SocketClientObjectPool socketClientPool) throws Exception {
		this.socketClientPool = socketClientPool;
		this.socketClientPool.preparePool();
	}

	public SocketClientPool(String host, int port, int timeOutMils) throws Exception {
		this(host, port, timeOutMils, new GenericObjectPoolConfig());
	}

	public SocketClientPool(String host, int port, int timeOutMils, GenericObjectPoolConfig poolConfig) throws Exception {
		this(new SocketClientObjectPool(new SocketClientFactory(host, port, timeOutMils), poolConfig));
	}

	@Override
	public SocketClient borrowClient() {
		try {
			return this.socketClientPool.borrowObject();
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
			return null;
		}
	}

	@Override
	public void returnClient(SocketClient socketClient) {
		this.socketClientPool.returnObject(socketClient);
	}

	@Override
	public void invalidateClient(SocketClient socketClient) {
		try {
			this.socketClientPool.invalidateObject(socketClient);
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	private static class SocketClientObjectPool extends GenericObjectPool<SocketClient> {

		public SocketClientObjectPool(PooledObjectFactory<SocketClient> factory) {
			super(factory);
		}

		public SocketClientObjectPool(PooledObjectFactory<SocketClient> factory, GenericObjectPoolConfig config) {
			super(factory, config);
		}
	}

}
