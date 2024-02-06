package com.github.example.kv.storage.io;

import java.io.File;

public interface ComparableFileName<T> {

	enum SortType {
		ASC,
		DESC
	}

	String format(T obj);

	int compare(File f1, File f2, SortType sortType);
}
