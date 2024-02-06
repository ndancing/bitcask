package com.github.example.kv.server.command;

import com.github.example.kv.server.exception.BitcaskServerException;

public class BitcaskStorageCommandParser implements KVCommandParser<String, String> {

	private static final String GET_OPERATOR = "GET";
	private static final String SET_OPERATOR = "SET";
	private static final String REMOVE_OPERATOR = "RMV";

	@Override
	public KVCommand<String, String, ?> parse(String input) {
		int cursor = 0;
		cursor = ignoreWhiteSpaceChars(input, cursor);
		if (cursor + 2 >= input.length()) {
			throw new BitcaskServerException("No operation specified in the command");
		}
		/**
		 * Parse operation (GET, SET, RMV)
		 * */
		final String operation = input.substring(cursor, cursor + 3).toUpperCase();
		cursor += 3;
		/**
		 * Check operation parameters
		 * */
		cursor = ignoreWhiteSpaceChars(input, cursor);
		if (cursor == input.length()) {
			throw new BitcaskServerException("No operation parameters specified in the command");
		}

		switch (operation) {
			case GET_OPERATOR:
				return parseGetCommand(input, cursor);
			case SET_OPERATOR:
				return parseSetCommand(input, cursor);
			case REMOVE_OPERATOR:
				return parseRemoveCommand(input, cursor);
			default:
				throw new BitcaskServerException("Operation not supported");
		}
	}

	protected KVCommand<String, String, ?> parseGetCommand(String input, int cursor) {
		return new BitcaskGetCommand(parseKey(input, cursor));
	}

	protected KVCommand<String, String, ?> parseSetCommand(String input, int cursor) {
		final StringBuilder sb = new StringBuilder();
		/**
		 * Parse key
		 * */
		while (cursor < input.length() && input.charAt(cursor) != ' ') {
			sb.append(input.charAt(cursor++));
		}
		final String key = sb.toString();
		/**
		 * Parse value
		 * */
		sb.setLength(0);
		cursor = ignoreWhiteSpaceChars(input, cursor);
		if (cursor == input.length()) {
			throw new BitcaskServerException("Operation value parameter need to be specified");
		}
		if (input.charAt(cursor) != '\"') {
			throw new BitcaskServerException("Invalid value parameter format");
		}
		cursor++;
		while (cursor < input.length() && input.charAt(cursor) != '\"') {
			sb.append(input.charAt(cursor++));
		}
		if (cursor == input.length() && input.charAt(cursor - 1) != '\"') {
			throw new BitcaskServerException("Invalid value parameter format");
		} else {
			cursor++;
		}
		final String value = sb.toString();
		/**
		 * Check unneeded params
		 * */
		cursor = ignoreWhiteSpaceChars(input, cursor);
		if (cursor != input.length()) {
			throw new BitcaskServerException("Operation doesn't contain parameters in addition to key and value");
		}

		return new BitcaskSetCommand(key, value);
	}

	protected KVCommand<String, String, ?> parseRemoveCommand(String input, int cursor) {
		return new BitcaskRemoveCommand(parseKey(input, cursor));
	}

	private String parseKey(String input, int cursor) throws BitcaskServerException {
		final StringBuilder sb = new StringBuilder();
		while (cursor < input.length() && input.charAt(cursor) != ' ') {
			sb.append(input.charAt(cursor++));
		}
		final String key = sb.toString();
		/**
		 * Check unneeded params
		 * */
		cursor = ignoreWhiteSpaceChars(input, cursor);
		if (cursor != input.length()) {
			throw new BitcaskServerException("Operation doesn't contain parameters in addition to key");
		}
		return key;
	}

	private int ignoreWhiteSpaceChars(String input, int cursor) {
		int charIndex = cursor;
		while (charIndex < input.length() && input.charAt(charIndex) == ' ') {
			charIndex++;
		}
		return charIndex;
	}
}
