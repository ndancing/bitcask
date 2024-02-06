package com.github.example.kv.storage.io;

import java.io.File;
import java.io.IOException;

import com.github.example.kv.storage.log.FileLog;

public interface FileLogIO extends KeyValueWriter<FileLog, File, FileLogWriteResult> {

	String read(String filepath, int offset, int length) throws IOException;

	void remove(File file);

}
