package com.github.example.kv.storage.io;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;

import com.github.example.kv.storage.log.FileLog;

public interface FileLogIO extends KeyValueWriter<FileLog, FileLogWriteResult> {

	String read(String filepath, int offset, int length) throws IOException;

	void removeFile(File file);

	FileLogWriteResult writeTo(FileLog data, File file, FileChannel channel) throws IOException;

}
