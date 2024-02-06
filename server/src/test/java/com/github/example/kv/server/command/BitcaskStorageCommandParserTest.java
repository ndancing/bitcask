package com.github.example.kv.server.command;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.github.example.kv.server.exception.BitcaskServerException;

public class BitcaskStorageCommandParserTest {

	private KVCommandParser<String, String> parser = new BitcaskStorageCommandParser();

	@Test
	public void testParseGetCommand() {
		String input = "   GET test_key   ";
		KVCommand<String, String, ?> command = parser.parse(input);
		assertTrue(command instanceof BitcaskGetCommand);
		BitcaskGetCommand getCommand = (BitcaskGetCommand) command;
		assertEquals("test_key", getCommand.getKey());
	}

	@Test(expected = BitcaskServerException.class)
	public void testParseGetCommand_Error_Format() {
		String input = "   GET test_key   aaa";
		KVCommand<String, String, ?> command = parser.parse(input);
	}

	@Test
	public void testParseSetCommand() {
		String input = "SET test_key \"test value\"  ";
		KVCommand<String, String, ?> command = parser.parse(input);
		assertTrue(command instanceof BitcaskSetCommand);
		BitcaskSetCommand setCommand = (BitcaskSetCommand) command;
		assertEquals("test_key", setCommand.getKey());
		assertEquals("test value", setCommand.getValue());
	}

	@Test(expected = BitcaskServerException.class)
	public void testParseSetCommand_Error_Format1() {
		String input = "SET test_key \"test value  ";
		KVCommand<String, String, ?> command = parser.parse(input);
	}

	@Test(expected = BitcaskServerException.class)
	public void testParseSetCommand_Error_Format2() {
		String input = "SET test_key test value\"  ";
		KVCommand<String, String, ?> command = parser.parse(input);
	}

	@Test(expected = BitcaskServerException.class)
	public void testParseSetCommand_Error_Format3() {
		String input = "SET test_key test value  ";
		KVCommand<String, String, ?> command = parser.parse(input);
	}

	@Test(expected = BitcaskServerException.class)
	public void testParseSetCommand_Error_Format4() {
		String input = "SET test key \"test value\"";
		KVCommand<String, String, ?> command = parser.parse(input);
	}

	@Test
	public void testParseRemoveCommand() {
		String input = "   RMV test_key   ";
		KVCommand<String, String, ?> command = parser.parse(input);
		assertTrue(command instanceof BitcaskRemoveCommand);
		BitcaskRemoveCommand removeCommand = (BitcaskRemoveCommand) command;
		assertEquals("test_key", removeCommand.getKey());
	}

	@Test(expected = BitcaskServerException.class)
	public void testParseRemoveCommand_Error_Format() {
		String input = "   RMV test_key   aaa";
		KVCommand<String, String, ?> command = parser.parse(input);
	}

	@Test(expected = BitcaskServerException.class)
	public void testParse_Invalid_Operation() {
		String input = "   INVALID a_parameter ";
		KVCommand<String, String, ?> command = parser.parse(input);
	}

}