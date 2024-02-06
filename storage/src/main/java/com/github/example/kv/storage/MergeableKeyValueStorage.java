package com.github.example.kv.storage;

import java.io.IOException;

public interface MergeableKeyValueStorage<K, V> extends KeyValueStorage<K, V> {

	void merge() throws IOException;
}
