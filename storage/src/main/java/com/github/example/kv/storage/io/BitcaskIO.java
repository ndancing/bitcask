package com.github.example.kv.storage.io;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.util.HashMap;
import java.util.Map;

import com.github.example.kv.storage.log.FileLog;
import com.github.example.kv.storage.utils.FileUtils;
import com.google.common.primitives.Ints;

public final class BitcaskIO implements FileLogIO {

	private String fileLogDirectory;
	private File activeFile;
	private OutputStream activeFileOutputStream;
	private final ComparableFileName comparableFileName;
	private final int fileSizeLimit;
	private final Map<String, RandomAccessFile> fileAccessors;

	public BitcaskIO(String fileLogDirectory, ComparableFileName comparableFileName, int fileSizeLimit) throws IOException {
		this.fileLogDirectory = fileLogDirectory;
		this.comparableFileName = comparableFileName;
		this.fileSizeLimit = fileSizeLimit;
		this.fileAccessors = new HashMap<>();
		createNewActiveFile(fileLogDirectory);
	}

	@Override
	public String read(String filepath, int offset, int length) throws IOException {
		if (!this.fileAccessors.containsKey(filepath)) {
			this.fileAccessors.put(filepath, new RandomAccessFile(filepath, "r"));
		}
		return new String(FileUtils.readBytes(this.fileAccessors.get(filepath), offset, length), StandardCharsets.UTF_8);
	}

	@Override
	public FileLogWriteResult write(FileLog data) throws IOException {
		if (this.activeFile.length() >= this.fileSizeLimit) {
			createNewActiveFile(this.fileLogDirectory);
		}
		return writeTo(data, this.activeFile, this.activeFileOutputStream);
	}

	@Override
	public FileLogWriteResult writeTo(FileLog fileLog, File file, OutputStream outputStream) throws IOException {
		final byte[] fileLogBytes = fileLog.toBytes();
		/**
		 * crc (4), timestamp(8), key size (4), value size (4), key
		 * Total 20 bytes adding with key length from current EOF to value byte offset
		 */
		final int valueByteOffset = (int)file.length() + 20 + fileLog.getKey().getBytes().length;
		outputStream.write(Ints.toByteArray(fileLogBytes.length));
		outputStream.write(fileLogBytes);
		return new FileLogWriteResult(file.getPath(), valueByteOffset);
	}

	@Override
	public void remove(File file) {
		this.fileAccessors.remove(file.getPath());
		file.delete();
	}

	private void createNewActiveFile(String fileLogDir) throws IOException {
		if (this.activeFileOutputStream != null) {
			this.activeFileOutputStream.close();
		}
		this.activeFile = new File(fileLogDir + FileSystems.getDefault().getSeparator()
			+ comparableFileName.format(System.currentTimeMillis()));
		assert (this.activeFile.createNewFile());
		this.activeFileOutputStream = new FileOutputStream(this.activeFile, true);
	}
}
