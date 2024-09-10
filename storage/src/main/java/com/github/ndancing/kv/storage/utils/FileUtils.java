package com.github.ndancing.kv.storage.utils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public final class FileUtils {

	private FileUtils() {
	}

	public static byte[] readBytes(RandomAccessFile randomAccessFile, long byteOffset, int byteLength) throws IOException {
		final byte[] data = new byte[byteLength];
		randomAccessFile.seek(byteOffset);
		randomAccessFile.read(data, 0, byteLength);
		return data;
	}

	public static void createFileIfNotExists(String filePath, boolean isDirectory) throws IOException {
		final File file = new File(filePath);
		if (isDirectory) {
			file.mkdir();
		} else {
			assert file.createNewFile();
		}
	}
}
