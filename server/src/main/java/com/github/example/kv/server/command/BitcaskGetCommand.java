package com.github.example.kv.server.command;

import java.io.IOException;

import com.github.example.kv.storage.KeyValueStorage;
import com.github.example.kv.storage.exception.BitcaskNotExistedKeyStorageException;
import com.github.example.kv.storage.exception.BitcaskStorageException;

public class BitcaskGetCommand extends BitcaskStorageCommand<String> {

	private final String key;

	protected BitcaskGetCommand(String key) {
		this.key = key;
	}

	public String getKey() {
		return key;
	}

	@Override
	public String execute(KeyValueStorage<String, String> kvStorage) {
		try {
			return kvStorage.get(this.key);
		} catch (BitcaskNotExistedKeyStorageException nke) {
			return "nil";
		} catch (BitcaskStorageException se) {
			throw se;
		} catch (IOException e) {
			throw new BitcaskStorageException(String.format("Error occurs when execute GET key=%s", this.key), e);
		}
	}

	@Override
	public String error() {
		return "error";
	}
}
