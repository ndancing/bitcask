package com.github.ndancing.kv.storage;

import java.io.IOException;

public interface MergeableKeyValueStorage<K, V> extends KeyValueStorage<K, V> {

	void merge() throws IOException;
}
