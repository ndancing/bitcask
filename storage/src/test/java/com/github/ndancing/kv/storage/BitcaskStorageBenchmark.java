package com.github.ndancing.kv.storage;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BitcaskStorageBenchmark {

	private static MergeableKeyValueStorage<String, String> storage;
	private static Map<String, String> keyValues = new HashMap<>();

	private static final int NUMBER_OF_KEYS = 1000000;
	private static final int KEY_BYTE_SIZE = 8;
	private static final int VALUE_BYTE_SIZE = 128;
	private static final String STORAGE_DIR = "/Users/files/bcask";

	@BeforeClass
	public static void setup() throws IOException {
		final BitcaskStorageConfig config = BitcaskStorageConfig.builder()
			.storageDir(STORAGE_DIR)
			.cacheEnabled(false)
			.fileSizeLimit(10240000)
			.mergePeriodMils(300000)
			.build();

		storage = new BitcaskStorage(config);

		for (int i = 0; i < NUMBER_OF_KEYS; i++) {
			keyValues.put(RandomStringUtils.generate(KEY_BYTE_SIZE), RandomStringUtils.generate(VALUE_BYTE_SIZE));
		}
	}

	@AfterClass
	public static void tearDown() {
		Arrays.stream(Objects.requireNonNull(new File(STORAGE_DIR).listFiles()))
			.forEach(File::delete);
	}

	@Test
	public void benchmark() {
		/**
		 * Put benchmark
		 * */
		final Runtime runtime = Runtime.getRuntime();
		long timestampBefore = System.currentTimeMillis();
		keyValues.entrySet().forEach(entry -> {
				try {
					storage.set(entry.getKey(), entry.getValue());
				} catch (Exception e) {
					System.err.println(e.getMessage());
				}
			}
		);
		long timestampAfter = System.currentTimeMillis();
		Set<String> keySet = new HashSet<>(keyValues.keySet());
		keyValues.clear();
		long memUsageAfter = runtime.totalMemory() - runtime.freeMemory();
		System.out.println("Put Duration: " + (timestampAfter - timestampBefore) + " mils");
		System.out.println("Memory Usage: " + memUsageAfter / 1048576 + " MB");

		/**
		 * Get benchmark
		 * */
		timestampBefore = System.currentTimeMillis();
		keySet.forEach(key -> {
				try {
					storage.get(key);
				} catch (Exception e) {
					System.err.println(e.getMessage());
				}
			}
		);
		timestampAfter = System.currentTimeMillis();
		System.out.println("Get Duration: " + (timestampAfter - timestampBefore) + " mils");
	}

}