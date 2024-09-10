package com.github.ndancing.kv.server.command;

public interface KVCommandParser<K, V> {

	KVCommand<K, V, ?> parse(String input);
}
