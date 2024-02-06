package com.github.example.kv.client;

public interface BitcaskClient {

	/**
	 * Get Bitcask key-value data type
	 * @param key
	 * @param <V> value type
	 * @return BKey<V>
	 * throws BitcaskException if key is empty or contains any whitespace
	 */
	<V> BKey<V> getKey(String key);
}
