package com.github.ndancing.kv.server.command;

import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.ndancing.kv.server.exception.BitcaskServerException;
import com.github.ndancing.kv.storage.KeyValueStorage;
import com.github.ndancing.kv.storage.exception.BitcaskStorageException;

public class BitcaskStorageCommandHandler implements KVCommandHandler<String> {

	private static final Logger LOGGER = LoggerFactory.getLogger(BitcaskStorageCommandHandler.class);

	private final KeyValueStorage<String, String> kvStorage;
	private final KVCommandParser<String, String> commandParser;

	public BitcaskStorageCommandHandler(KeyValueStorage<String, String> kvStorage, KVCommandParser commandParser) {
		this.kvStorage = kvStorage;
		this.commandParser = commandParser;
	}

	@Override
	public String handle(String input) {
		KVCommand<String, String, ?> command = null;
		try {
			command = commandParser.parse(input);
			final Object exeResult = command.execute(this.kvStorage);
			if (String.class.equals(exeResult.getClass())) {
				return (String)exeResult;
			} else if (Integer.class.equals(exeResult.getClass())) {
				return String.valueOf((int)exeResult);
			}
		} catch (BitcaskStorageException ste) {
			LOGGER.error(ste.getMessage(), ste);
			/**
			 * Error response
			 * */
			if (Objects.nonNull(command)) {
				final Object errResult = command.error();
				if (String.class.equals(errResult.getClass())) {
					return (String)errResult;
				} else if (Integer.class.equals(errResult.getClass())) {
					return String.valueOf((int)errResult);
				}
			}
		} catch (BitcaskServerException se) {
			/**
			 * No response in case invalid commands
			 * */
			LOGGER.error(se.getMessage(), se);
		}
		return null;
	}
}
