package com.github.ndancing.kv.client.exception;

public class BitcaskException extends RuntimeException {
	public BitcaskException(String msg) {
		super(msg);
	}

	public BitcaskException(String msg, Throwable t) {
		super(msg, t);
	}

}
