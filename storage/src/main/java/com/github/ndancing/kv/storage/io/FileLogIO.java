package com.github.ndancing.kv.storage.io;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;

import com.github.ndancing.kv.storage.log.BitcaskFileLog;

public interface FileLogIO extends KeyValueWriter<BitcaskFileLog, FileLogWriteResult> {

	String read(String filepath, int offset, int length) throws IOException;

	void removeFile(File file);

	FileLogWriteResult writeTo(BitcaskFileLog data, File file, FileChannel channel) throws IOException;

}
