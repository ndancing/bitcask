package com.github.example.kv.storage.io;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.example.kv.storage.log.FileLog;
import com.github.example.kv.storage.utils.FileUtils;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;

public final class BitcaskIO implements FileLogIO {

	private static final Logger LOGGER = LoggerFactory.getLogger(BitcaskIO.class);

	private String fileLogDirectory;
	private File activeFile;
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
			this.fileAccessors.put(filepath, new RandomAccessFile(filepath, "rw"));
		}
		return new String(FileUtils.readBytes(this.fileAccessors.get(filepath), offset, length), StandardCharsets.UTF_8);
	}

	@Override
	public FileLogWriteResult write(FileLog data) throws IOException {
		if (this.activeFile.length() >= this.fileSizeLimit) {
			createNewActiveFile(this.fileLogDirectory);
		}
		return writeTo(data, this.activeFile, this.fileAccessors.get(this.activeFile.getPath()).getChannel());
	}

	@Override
	public FileLogWriteResult writeTo(FileLog data, File file, FileChannel channel) throws IOException {
		final byte[] fileLogBytes = data.toBytes();
		/**
		 * crc (4), timestamp(8), key size (4), value size (4), key
		 * Total 20 bytes adding with key length from current EOF to value byte offset
		 */
		final int valueByteOffset = (int)channel.size() + 20 + data.getKey().getBytes().length;
		channel.write(ByteBuffer.wrap(Bytes.concat(Ints.toByteArray(fileLogBytes.length), fileLogBytes)));
		return new FileLogWriteResult(file.getPath(), valueByteOffset);
	}

	@Override
	public void removeFile(File file) {
		if (this.fileAccessors.containsKey(file.getPath())) {
			final RandomAccessFile randomAccessFile = this.fileAccessors.get(file.getPath());
			try {
				randomAccessFile.getChannel().close();
				randomAccessFile.close();
			} catch (IOException e) {
				LOGGER.error(e.getMessage(), e);
			}
			this.fileAccessors.remove(file.getPath());
		}
		file.delete();
	}

	private void createNewActiveFile(String fileLogDir) throws IOException {
		this.activeFile = new File(fileLogDir + FileSystems.getDefault().getSeparator()
			+ comparableFileName.format(System.currentTimeMillis()));
		assert (this.activeFile.createNewFile());
		this.fileAccessors.put(this.activeFile.getPath(), new RandomAccessFile(this.activeFile.getPath(), "rw"));
	}
}
