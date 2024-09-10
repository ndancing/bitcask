package com.github.ndancing.kv.client;

import org.apache.commons.lang3.StringUtils;

import com.github.ndancing.kv.client.exception.BitcaskException;
import com.github.ndancing.kv.client.socket.SocketClientPool;

public class Bitcask implements BitcaskClient {

	private static final int DEFAULT_TIMEOUT_MILS = 3000;

	private final ClientPool clientPool;
	private final CommandExecutor commandExecutor;

	private Bitcask(String host, int port, int timeOutMils) {
		try {
			this.clientPool = new SocketClientPool(host, port, timeOutMils);
			this.commandExecutor = new ByteArrayCommandExecutor();
		} catch (Exception e) {
			throw new BitcaskException("Error when init Bitcask client", e);
		}
	}

	private Bitcask(Builder builder) {
		this(builder.host, builder.port, builder.timeOutMils);
	}

	public static Builder builder() {
		return new Builder();
	}

	@Override
	public BKey<String> getKey(String key) {
		if (StringUtils.isEmpty(key) || StringUtils.containsWhitespace(key)) {
			throw new BitcaskException("Key must not be empty or contains any whitespace");
		}
		return new BitcaskKey(clientPool, commandExecutor, key);
	}

	public static class Builder {
		private String host;
		private int port;
		private int timeOutMils = DEFAULT_TIMEOUT_MILS;

		public Builder host(String host) {
			this.host = host;
			return this;
		}

		public Builder port(int port) {
			this.port = port;
			return this;
		}

		public Builder timeOutMils(int timeOutMils) {
			this.timeOutMils = timeOutMils;
			return this;
		}

		public Bitcask build() {
			if (host == null || host.isEmpty()) {
				throw new BitcaskException("Invalid config host = " + host);
			} else if (port <= 0) {
				throw new BitcaskException("Invalid config port = " + port);
			} else if (timeOutMils <= 0) {
				throw new BitcaskException("Invalid config timeOutMils = " + timeOutMils);
			}

			return new Bitcask(this);
		}
	}
}
