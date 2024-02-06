package com.github.example.kv.client;

import com.github.example.kv.client.exception.BitcaskNotExistedKeyException;

public class BitcaskKey extends BitcaskData<String> implements BKey<String> {
	private final String key;

	public BitcaskKey(ClientPool<Client> clientPool, CommandExecutor<String> clientCommandExecutor, String key) {
		super(clientPool, clientCommandExecutor);
		this.key = key;
	}

	@Override
	public String getValue() {
		return commandExecute(String.format("GET %s", this.key), response -> {
			if ("nil".equals(response)) {
				throw new BitcaskNotExistedKeyException(String.format("Key %s is not existed", this.key));
			} else if ("error".equals(response)) {
				return null;
			} else {
				return response;
			}
		});
	}

	@Override
	public boolean setValue(String value) {
		return commandExecute(String.format("SET %s \"%s\"", this.key, value), response -> {
			if (Integer.valueOf(response) < 0) {
				return false;
			}
			return true;
		});
	}

	@Override
	public boolean remove() {
		return commandExecute(String.format("RMV %s", this.key), response -> {
			final Integer errCode = Integer.valueOf(response);
			if (errCode == 0) {
				throw new BitcaskNotExistedKeyException(String.format("Key %s is not existed", this.key));
			} else if (errCode < 0) {
				return false;
			} else {
				return true;
			}
		});
	}
}
