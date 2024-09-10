package com.github.ndancing.kv.storage.io;

public class FileLogWriteResult implements KeyValueWriteResult {

	private final String writeFilePath;
	private final int valueByteOffset;

	public FileLogWriteResult(String writeFilePath, int valueByteOffset) {
		this.writeFilePath = writeFilePath;
		this.valueByteOffset = valueByteOffset;
	}

	public String getWriteFilePath() {
		return writeFilePath;
	}

	public int getValueByteOffset() {
		return valueByteOffset;
	}
}
