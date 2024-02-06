package com.github.example.kv.storage.exception;

public class BitcaskNotExistedKeyStorageException extends BitcaskStorageException {

	public BitcaskNotExistedKeyStorageException(String msg) {
		super(msg);
	}

	public BitcaskNotExistedKeyStorageException(String msg, Throwable t) {
		super(msg, t);
	}

}
