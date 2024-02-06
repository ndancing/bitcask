package com.github.example.kv.storage;

import java.io.IOException;

public interface KeyValueStorage<K, V> {
	V get(K key) throws IOException;

	void set(K key, V value) throws IOException;

	void remove(K key) throws IOException;
}
