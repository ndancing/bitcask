package com.github.example.kv.storage.log;

public final class FileLogConstant {

	private FileLogConstant() {}

	/**
	 * ----------------------------------------------------
	 * | Timestamp | Key size | Value size | Key | Value |
	 * |   8 bytes |  4 bytes |   4 bytes  |    byte[]   |
	 * ----------------------------------------------------
	 */
	public static final int TIMESTAMP_BYTE_OFFSET = 0;
	public static final int TIMESTAMP_BYTE_LENGTH = 8;
	public static final int KEY_SIZE_BYTE_OFFSET = 8;
	public static final int KEY_SIZE_BYTE_LENGTH = 4;
	public static final int VALUE_SIZE_BYTE_OFFSET = 12;
	public static final int VALUE_SIZE_BYTE_LENGTH = 4;
	public static final int KEY_BYTE_OFFSET = 16;
	public static final String TOMBSTONE = "tmbstn";
	public static final String COMPACT_FILE_SUFFIX = "~cmpct";
}
