package com.github.example.kv.storage.io;

import java.io.File;

public final class BitcaskFileName implements ComparableFileName<Long> {

	private static final String FILE_LOG_PREFIX = "bcask_";

	@Override
	public String format(Long timestamp) {
		return FILE_LOG_PREFIX + timestamp;
	}

	@Override
	public int compare(File f1, File f2, SortType sortType) {
		final long l1 = Long.parseLong(f1.getName().split("_")[1]);
		final long l2 = Long.parseLong(f2.getName().split("_")[1]);
		return SortType.DESC.equals(sortType) ? (int) (l2 - l1) : (int) (l1 - l2);
	}
}
