package com.github.example.kv.server.exception;

public class BitcaskServerException extends RuntimeException {

	public BitcaskServerException(String msg) {
		super(msg);
	}

	public BitcaskServerException(String msg, Throwable t) {
		super(msg, t);
	}
}
