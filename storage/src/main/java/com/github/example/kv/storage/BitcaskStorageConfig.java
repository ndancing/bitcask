package com.github.example.kv.storage;

import com.github.example.kv.storage.exception.BitcaskInvalidConfigStorageException;

public class BitcaskStorageConfig {

	private BitcaskStorageConfig() {
	}

	public static final String DEFAULT_STORAGE_DIR = "/bcask";
	public static final int DEFAULT_FILE_SIZE_LIMIT = 10240000;
	public static final boolean DEFAULT_CACHE_ENABLED = true;
	public static final int DEFAULT_CACHE_SIZE = 10000;
	public static final int DEFAULT_MERGE_PERIOD_MILS = 300000;

	private String storageDir;
	private int fileSizeLimit;
	private boolean cacheEnabled;
	private int cacheSize;
	private int mergePeriodMils;

	protected BitcaskStorageConfig(Builder builder) {
		this.storageDir = builder.storageDir;
		this.fileSizeLimit = builder.fileSizeLimit;
		this.cacheEnabled = builder.cacheEnabled;
		this.cacheSize = builder.cacheSize;
		this.mergePeriodMils = builder.mergePeriodMils;
	}

	public static Builder builder() {
		return new Builder();
	}

	public String getStorageDir() {
		return storageDir;
	}

	public int getFileSizeLimit() {
		return fileSizeLimit;
	}

	public boolean isCacheEnabled() {
		return cacheEnabled;
	}

	public int getCacheSize() {
		return cacheSize;
	}

	public int getMergePeriodMils() {
		return mergePeriodMils;
	}

	public static class Builder {

		private String storageDir = DEFAULT_STORAGE_DIR;
		private int fileSizeLimit = DEFAULT_FILE_SIZE_LIMIT;
		private boolean cacheEnabled = DEFAULT_CACHE_ENABLED;
		private int cacheSize = DEFAULT_CACHE_SIZE;
		private int mergePeriodMils = DEFAULT_MERGE_PERIOD_MILS;

		public Builder storageDir(String storageDir) {
			this.storageDir = storageDir;
			return this;
		}

		public Builder fileSizeLimit(int fileSizeLimit) {
			this.fileSizeLimit = fileSizeLimit;
			return this;
		}

		public Builder cacheEnabled(boolean cacheEnabled) {
			this.cacheEnabled = cacheEnabled;
			return this;
		}

		public Builder cacheSize(int cacheSize) {
			this.cacheSize = cacheSize;
			return this;
		}

		public Builder mergePeriodMils(int mergePeriodMils) {
			this.mergePeriodMils = mergePeriodMils;
			return this;
		}

		public BitcaskStorageConfig build() {
			if (storageDir == null || storageDir.isEmpty()) {
				throw new BitcaskInvalidConfigStorageException("Invalid config storageDir = " + storageDir);
			} else if (fileSizeLimit <= 0) {
				throw new BitcaskInvalidConfigStorageException("Invalid config fileSizeLimit = " + fileSizeLimit);
			} else if (cacheEnabled && cacheSize <= 0) {
				throw new BitcaskInvalidConfigStorageException("Invalid config cacheSize = " + cacheSize);
			} else if (mergePeriodMils <= 0) {
				throw new BitcaskInvalidConfigStorageException("Invalid config mergePeriodMils = " + mergePeriodMils);
			}

			return new BitcaskStorageConfig(this);
		}

	}
}
