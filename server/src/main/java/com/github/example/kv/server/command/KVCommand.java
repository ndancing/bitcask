package com.github.example.kv.server.command;

import com.github.example.kv.storage.KeyValueStorage;

/**
 *
 * @param <K> key type
 * @param <V> value type
 * @param <R> command result type
 */
public interface KVCommand<K, V, R> {

	R execute(KeyValueStorage<K, V> kvStorage);

	R error();
}
