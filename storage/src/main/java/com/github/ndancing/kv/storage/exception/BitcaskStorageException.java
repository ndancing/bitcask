package com.github.ndancing.kv.storage.exception;

public class BitcaskStorageException extends RuntimeException {

	public BitcaskStorageException(String msg) {
		super(msg);
	}

	public BitcaskStorageException(String msg, Throwable t) {
		super(msg, t);
	}
}
