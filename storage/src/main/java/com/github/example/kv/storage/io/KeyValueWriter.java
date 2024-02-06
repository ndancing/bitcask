package com.github.example.kv.storage.io;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;

import com.github.example.kv.storage.KeyValue;

public interface KeyValueWriter<T extends KeyValue, R extends KeyValueWriteResult> {

	R write(T data) throws IOException;

}
