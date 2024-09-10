package com.github.ndancing.kv.server.command;

import java.io.IOException;

import com.github.ndancing.kv.storage.KeyValueStorage;
import com.github.ndancing.kv.storage.exception.BitcaskStorageException;

public class BitcaskSetCommand extends BitcaskStorageCommand<Integer> {

	private final String key;
	private final String value;

	protected BitcaskSetCommand(String key, String value) {
		this.key = key;
		this.value = value;
	}

	public String getKey() {
		return key;
	}

	public String getValue() {
		return value;
	}

	@Override
	public Integer execute(KeyValueStorage<String, String> kvStorage) {
		try {
			kvStorage.set(this.key, this.value);
			return 1;
		} catch (BitcaskStorageException se) {
			throw se;
		} catch (IOException e) {
			throw new BitcaskStorageException(String.format("Error occurs when execute SET key=%s; value=%s", this.key, this.value), e);
		}
	}

	@Override
	public Integer error() {
		return -1;
	}
}
