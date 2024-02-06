package com.github.example.kv.storage.io;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;

import com.github.example.kv.storage.KeyValue;

public interface KeyValueWriter<T extends KeyValue, S extends Serializable, R extends KeyValueWriteResult> {

	R write(T data) throws IOException;

	R writeTo(T data, S target, OutputStream outputStream) throws IOException;

}
