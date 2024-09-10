package com.github.ndancing.kv.server.command;

import com.github.ndancing.kv.storage.KeyValueStorage;

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
