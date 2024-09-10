package com.github.ndancing.kv.storage.io;

import java.io.IOException;

import com.github.ndancing.kv.storage.KeyValue;

public interface KeyValueWriter<T extends KeyValue, R extends KeyValueWriteResult> {

	R write(T data) throws IOException;

}
