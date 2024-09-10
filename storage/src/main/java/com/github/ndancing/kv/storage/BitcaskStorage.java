package com.github.ndancing.kv.storage;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.ndancing.kv.storage.exception.BitcaskNotExistedKeyStorageException;
import com.github.ndancing.kv.storage.io.BitcaskFileName;
import com.github.ndancing.kv.storage.io.BitcaskIO;
import com.github.ndancing.kv.storage.io.ComparableFileName;
import com.github.ndancing.kv.storage.io.FileLogIO;
import com.github.ndancing.kv.storage.io.FileLogWriteResult;
import com.github.ndancing.kv.storage.log.BitcaskFileLog;
import com.github.ndancing.kv.storage.log.BitcaskFileLogConstant;
import com.github.ndancing.kv.storage.utils.FileUtils;
import com.google.common.primitives.Ints;

public class BitcaskStorage implements MergeableKeyValueStorage<String, String> {

	private static final Logger LOGGER = LoggerFactory.getLogger(BitcaskStorage.class);
	private static final ComparableFileName<?> BITCASK_FILE_NAME = new BitcaskFileName();
	private final BitcaskStorageConfig storageConfig;
	private final Map<String, MetaData> metaData;
	private final FileLogIO fileIO;
	private Map<String, String> internalCache = null;

	public BitcaskStorage(BitcaskStorageConfig storageConfig) throws IOException {
		FileUtils.createFileIfNotExists(storageConfig.getStorageDir(), true);
		this.storageConfig = storageConfig;
		this.metaData = new HashMap<>();
		this.fileIO = new BitcaskIO(storageConfig.getStorageDir(), BITCASK_FILE_NAME, storageConfig.getFileSizeLimit());
		if (storageConfig.isCacheEnabled()) {
			this.internalCache = new LinkedHashMap<String, String>() {
				@Override
				protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
					return size() > storageConfig.getCacheSize();
				}
			};
		}
		/**
		 * Init metadata from stored files
		 */
		warmUp();
	}

	@Override
	public String get(String key) throws IOException {
		if (!this.metaData.containsKey(key)) {
			LOGGER.warn("Key {} is not existed", key);
			throw new BitcaskNotExistedKeyStorageException(String.format("Key is not existed, key=%s", key));
		}
		if (this.storageConfig.isCacheEnabled() && this.internalCache.containsKey(key)) {
			return this.internalCache.get(key);
		}
		final MetaData keyMetaData = this.metaData.get(key);
		final String value = this.fileIO.read(keyMetaData.getFilePath(), keyMetaData.getValueByteOffset(), keyMetaData.getValueSize());
		if (BitcaskFileLogConstant.TOMBSTONE.equals(value)) {
			LOGGER.warn("Key {} was deleted", key);
			this.metaData.remove(key);
			throw new BitcaskNotExistedKeyStorageException(String.format("Key is not existed, key=%s", key));
		}
		if (this.storageConfig.isCacheEnabled()) {
			this.internalCache.put(key, value);
		}
		return value;
	}

	@Override
	public void set(String key, String value) throws IOException {
		final BitcaskFileLog fileLog = new BitcaskFileLog(System.currentTimeMillis(), key.getBytes().length, value.getBytes().length, key, value);
		final FileLogWriteResult writeResult = this.fileIO.write(fileLog);
		this.metaData.put(key, MetaData.valueOf(fileLog, writeResult.getWriteFilePath(), writeResult.getValueByteOffset()));
		if (this.storageConfig.isCacheEnabled()) {
			this.internalCache.remove(key);
		}
	}

	@Override
	public void remove(String key) throws IOException {
		if (!this.metaData.containsKey(key)) {
			LOGGER.warn("Key {} is not existed", key);
			throw new BitcaskNotExistedKeyStorageException(String.format("Key is not existed, key=%s", key));
		}
		final BitcaskFileLog fileLog = new BitcaskFileLog(System.currentTimeMillis(), key.getBytes().length, BitcaskFileLogConstant.TOMBSTONE.getBytes().length,
			key, BitcaskFileLogConstant.TOMBSTONE);
		this.fileIO.write(fileLog);
		this.metaData.remove(key);
		if (this.storageConfig.isCacheEnabled()) {
			this.internalCache.remove(key);
		}
	}

	@Override
	@SuppressWarnings("java:S899")
	public void merge() throws IOException {
		final int numberOfStoredFiles = Objects.requireNonNull(new File(storageConfig.getStorageDir()).listFiles()).length;
		/**
		 * No merge action in case there is none or only 1 file in addition to currently active file
		 * */
		if (numberOfStoredFiles <= 2) {
			return;
		}
		/**
		 * Get all stored file exclude currently active file -> Mergeable files
		 * */
		final List<File> mergeableFiles = Arrays.stream(getStoredFilesSortedByCreationTime(ComparableFileName.SortType.ASC))
			.limit(numberOfStoredFiles - 1L)
			.toList();

		/**
		 * Scan from closest to furthest mergeable files -> Build a key-value mapping from each one
		 * Using a metadata container to store last key-metadata mapping
		 * */
		final Map<String, MetaData> mergedFilesMetaDataContainer = new HashMap<>();
		final Map<String, String> mergedFilesKeyValueMapping = new HashMap<>();

		for (int i = mergeableFiles.size() - 1; i >= 0; i--) {
			final Map<String, String> keyValueMapping = processStoredFile(mergeableFiles.get(i), mergedFilesMetaDataContainer);
			keyValueMapping.entrySet().forEach(entry -> {
				if (!mergedFilesKeyValueMapping.containsKey(entry.getKey())) {
					mergedFilesKeyValueMapping.put(entry.getKey(), entry.getValue());
				}
			});
		}

		/**
		 * Create a compacted file to merge all processed files
		 * Give it a name like to the closest merged file append with a suffix (to avoid duplicated file name)
		 * Write all merged mapping data to this compact file
		 * Update the merged metadata with the closest merged file path
		 * Delete all merged files then rename the compact file to closest merged file
		 * */
		final String compactedFilePath = mergeableFiles.get(mergeableFiles.size() - 1).getPath() + BitcaskFileLogConstant.COMPACT_FILE_SUFFIX;
		final File compactedFile = new File(compactedFilePath);
		compactedFile.createNewFile();

		final File closestMergedFile = new File(
			compactedFilePath.substring(0, compactedFilePath.length() - BitcaskFileLogConstant.COMPACT_FILE_SUFFIX.length())
		);

		try (FileOutputStream compactedFileOutputStream = new FileOutputStream(compactedFile.getPath())) {
			final FileChannel compactedFileChannel = compactedFileOutputStream.getChannel();
			final Iterator<Map.Entry<String, MetaData>> iterator = mergedFilesMetaDataContainer.entrySet().iterator();
			while (iterator.hasNext()) {
				final Map.Entry<String, MetaData> entry = iterator.next();
				final BitcaskFileLog fileLog = new BitcaskFileLog(entry.getValue().getTimestamp(), entry.getKey().getBytes().length, entry.getValue().getValueSize(),
					entry.getKey(), mergedFilesKeyValueMapping.get(entry.getKey())
				);
				/**
				 * Remove deleted key (tombstone value)
				 * */
				if (BitcaskFileLogConstant.TOMBSTONE.equals(fileLog.getValue())) {
					iterator.remove();
				} else {
					try {
						final FileLogWriteResult writeResult = fileIO.writeTo(fileLog, compactedFile, compactedFileChannel);
						entry.setValue(MetaData.valueOf(fileLog, closestMergedFile.getPath(), writeResult.getValueByteOffset()));
					} catch (IOException e) {
						LOGGER.error("Error occurs when write to compacted file {}, file log={}", compactedFile.getPath(), fileLog, e);
					}
				}
			}
			compactedFileChannel.close();
		}

		mergeableFiles.forEach(fileIO::removeFile);

		compactedFile.renameTo(closestMergedFile);

		/**
		 * Update the merged mapping metadata to storage
		 * */
		mergedFilesMetaDataContainer.entrySet().forEach(entry -> {
			if (this.metaData.containsKey(entry.getKey()) && this.metaData.get(entry.getKey()).getTimestamp() == entry.getValue().getTimestamp()) {
				this.metaData.put(entry.getKey(), entry.getValue());
			}
		});
	}

	protected void warmUp() throws IOException {
		final File[] storedFiles = getStoredFilesSortedByCreationTime(ComparableFileName.SortType.DESC);
		Arrays.stream(storedFiles).forEach(f -> {
			try {
				processStoredFile(f, this.metaData);
			} catch (IOException e) {
				LOGGER.error("Error occurs when process stored file {} during startup", f.getPath(), e);
			}
		});
	}

	@SuppressWarnings("java:S899")
	private File[] getStoredFilesSortedByCreationTime(ComparableFileName.SortType sortType) throws IOException {
		final File storageDir = new File(storageConfig.getStorageDir());
		File[] storedFiles = storageDir.listFiles();

		final Optional<File> oCompactFile = Arrays.stream(Objects.requireNonNull(storedFiles))
			.filter(file -> file.getPath().endsWith(BitcaskFileLogConstant.COMPACT_FILE_SUFFIX))
			.findFirst();

		if (oCompactFile.isPresent()) {
			final File compactFile = oCompactFile.get();
			final String noSuffixFilePath = compactFile.getPath()
				.substring(0, compactFile.getPath().length() - BitcaskFileLogConstant.COMPACT_FILE_SUFFIX.length());

			if (Arrays.stream(storedFiles).map(File::getPath).anyMatch(path -> path.equals(noSuffixFilePath))) {
				Files.delete(compactFile.toPath());
			} else {
				compactFile.renameTo(new File(noSuffixFilePath));
			}
			storedFiles = storageDir.listFiles();
		}

		Arrays.sort(Objects.requireNonNull(storedFiles), (f1, f2) -> BITCASK_FILE_NAME.compare(f1, f2, sortType));
		return storedFiles;
	}

	/**
	 * Build key value mapping from a stored file
	 * @param storedFile
	 * @param metaDataContainer
	 * @return
	 * @throws IOException
	 */
	@SuppressWarnings({"java:S899", "java:S2674"})
	private Map<String, String> processStoredFile(File storedFile, Map<String, MetaData> metaDataContainer) throws IOException {
		final Map<String, String> keyValueMapping = new HashMap<>();
		final byte[] bytes = new byte[(int)storedFile.length()];
		try (FileInputStream inputStream = new FileInputStream(storedFile)) {
			inputStream.read(bytes);
		}
		int byteCursor = 0;
		while (byteCursor < bytes.length) {
			/**
			 * Read fileLog byte size in crc and cursor seeking (4 bytes)
			 * */
			final int fileLogByteSize = Ints.fromByteArray(Arrays.copyOfRange(bytes, byteCursor, byteCursor + 4));
			byteCursor += 4;

			/**
			 * Parse fileLog
			 * */
			final byte[] fileLogBytes = Arrays.copyOfRange(bytes, byteCursor, byteCursor + fileLogByteSize);
			final BitcaskFileLog fileLog = BitcaskFileLog.valueOf(fileLogBytes);

			/**
			 * timestamp(8), key size (4), value size (4), key
			 * Total 16 bytes adding with key length from current cursor to value byte offset
			 */
			final int valueByteOffset = byteCursor + 16 + fileLog.getKey().getBytes().length;

			/**
			 * Write to metaData container if key is not existed and process exactly the last stored file (in compaction). Update key value mapping
			 * */
			if (!metaDataContainer.containsKey(fileLog.getKey()) || metaDataContainer.get(fileLog.getKey()).getFilePath().equals(storedFile.getPath())) {
				metaDataContainer.put(fileLog.getKey(), MetaData.valueOf(fileLog, storedFile.getPath(), valueByteOffset));
				keyValueMapping.put(fileLog.getKey(), fileLog.getValue());
			}

			/**
			 * Cursor seeking
			 * */
			byteCursor += fileLogByteSize;
		}
		return keyValueMapping;
	}

	private static class MetaData {
		private final String filePath;
		private final int valueByteOffset;
		private final int valueSize;
		private final long timestamp;

		public MetaData(String filePath, int valueByteOffset, int valueSize, long timestamp) {
			this.filePath = filePath;
			this.valueByteOffset = valueByteOffset;
			this.valueSize = valueSize;
			this.timestamp = timestamp;
		}

		public static MetaData valueOf(BitcaskFileLog fileLog, String filePath, int valueByteOffset) {
			return new MetaData(filePath, valueByteOffset, fileLog.getValueSize(), fileLog.getTimestamp());
		}

		public String getFilePath() {
			return filePath;
		}

		public int getValueByteOffset() {
			return valueByteOffset;
		}

		public int getValueSize() {
			return valueSize;
		}

		public long getTimestamp() {
			return timestamp;
		}
	}
}
