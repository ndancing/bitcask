package com.github.example.kv.client;

/**
 * Bitcask Key Data Type
 * @param <V> value type
 */
public interface BKey<V> {

	/**
	 * Get value of key
	 * <V> value type
	 * return value of key
	 * throws BitcaskNotExistedKeyException if key is not existed
	 */
	V getValue();

	/**
	 * Set value to key
	 * <V> value type
	 * return true if set value successfully, otherwise return false
	 */
	boolean setValue(V value);

	/**
	 * Remove key
	 * return true if remove key successfully, otherwise return false
	 * throws BitcaskNotExistedKeyException if key is not existed
	 */
	boolean remove();

}
