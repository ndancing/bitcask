package com.github.ndancing.kv.server.command;

import java.io.IOException;

import com.github.ndancing.kv.storage.KeyValueStorage;
import com.github.ndancing.kv.storage.exception.BitcaskNotExistedKeyStorageException;
import com.github.ndancing.kv.storage.exception.BitcaskStorageException;

public class BitcaskRemoveCommand extends BitcaskStorageCommand<Integer> {

	private final String key;

	protected BitcaskRemoveCommand(String key) {
		this.key = key;
	}

	public String getKey() {
		return key;
	}

	@Override
	public Integer execute(KeyValueStorage<String, String> kvStorage) {
		try {
			kvStorage.remove(this.key);
			return 1;
		} catch (BitcaskNotExistedKeyStorageException nke) {
			return 0;
		} catch (BitcaskStorageException se) {
			throw se;
		} catch (IOException e) {
			throw new BitcaskStorageException(String.format("Error occurs when execute RMV key=%s", this.key), e);
		}
	}

	@Override
	public Integer error() {
		return -1;
	}

}
