package com.github.ndancing.kv.client.exception;

public class BitcaskNotExistedKeyException extends BitcaskException {

	public BitcaskNotExistedKeyException(String msg) {
		super(msg);
	}

	public BitcaskNotExistedKeyException(String msg, Throwable t) {
		super(msg, t);
	}

}
