package com.github.example.kv.storage.log;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import com.github.example.kv.storage.KeyValue;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

public class FileLog extends KeyValue<String, String> {
	protected final long timestamp;
	protected final int keySize;
	protected final int valueSize;

	public FileLog(long timestamp, int keySize, int valueSize, String key, String value) {
		super(key, value);
		this.timestamp = timestamp;
		this.keySize = keySize;
		this.valueSize = valueSize;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public int getKeySize() {
		return keySize;
	}

	public int getValueSize() {
		return valueSize;
	}

	public static FileLog valueOf(byte[] bytes) {
		final long timestamp = parseTimestamp(bytes);
		final int keySize = parseKeySize(bytes);
		final int valueSize = parseValueSize(bytes);
		final String key = parseKey(bytes, keySize);
		final String value = parseValue(bytes, FileLogConstant.KEY_BYTE_OFFSET + keySize, valueSize);

		return new FileLog(timestamp, keySize, valueSize, key, value);
	}

	public byte[] toBytes() throws IOException {
		final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		outputStream.write(Longs.toByteArray(timestamp));
		outputStream.write(Ints.toByteArray(keySize));
		outputStream.write(Ints.toByteArray(valueSize));
		outputStream.write(key.getBytes());
		outputStream.write(value.getBytes());

		return outputStream.toByteArray();
	}

	private static long parseTimestamp(byte[] bytes) {
		return Longs.fromByteArray(Arrays.copyOfRange(bytes, FileLogConstant.TIMESTAMP_BYTE_OFFSET, FileLogConstant.TIMESTAMP_BYTE_OFFSET + FileLogConstant.TIMESTAMP_BYTE_LENGTH));
	}

	private static int parseKeySize(byte[] bytes) {
		return Ints.fromByteArray(Arrays.copyOfRange(bytes, FileLogConstant.KEY_SIZE_BYTE_OFFSET, FileLogConstant.KEY_SIZE_BYTE_OFFSET + FileLogConstant.KEY_SIZE_BYTE_LENGTH));
	}

	private static int parseValueSize(byte[] bytes) {
		return Ints.fromByteArray(Arrays.copyOfRange(bytes, FileLogConstant.VALUE_SIZE_BYTE_OFFSET, FileLogConstant.VALUE_SIZE_BYTE_OFFSET + FileLogConstant.VALUE_SIZE_BYTE_LENGTH));
	}

	private static String parseKey(byte[] bytes, int keySize) {
		return new String(Arrays.copyOfRange(bytes, FileLogConstant.KEY_BYTE_OFFSET, FileLogConstant.KEY_BYTE_OFFSET + keySize), StandardCharsets.UTF_8);
	}

	private static String parseValue(byte[] bytes, int valuePosition, int valueSize) {
		return new String(Arrays.copyOfRange(bytes, valuePosition, valuePosition + valueSize), StandardCharsets.UTF_8);
	}
}
