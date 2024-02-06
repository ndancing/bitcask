package com.github.example.kv.client;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;

import com.github.example.kv.client.exception.BitcaskException;
import com.github.example.kv.client.exception.BitcaskNotExistedKeyException;

public class BitcaskClientTest {

	private static final int NUMBER_OF_KEYS = 1;

	private BitcaskClient client;

	@Before
	public void setup() {
		client = Bitcask.builder()
			.host("127.0.0.1")
			.port(6868)
			.timeOutMils(3000)
			.build();
	}

	@Test
	public void testPut() {
		Map<String, String> kvMap = new HashMap<>();
		for (int i = 0; i < NUMBER_OF_KEYS; i++) {
			kvMap.put("key" + i, "value" + i);
		}

		AtomicInteger putCount = new AtomicInteger();

		long start = System.currentTimeMillis();
		kvMap.entrySet().forEach(e -> {
				BKey<String> bkey = client.getKey(e.getKey());
				if (bkey.setValue(e.getValue())) {
					putCount.getAndIncrement();
				}
			}
		);

		long end = System.currentTimeMillis();

		System.out.println("Total time mils: " + (end - start));
		System.out.println("Total successfully put key: " + putCount);
		assertEquals(NUMBER_OF_KEYS, putCount);
	}

	@Test
	public void testGet() {
		List<String> keys = new ArrayList<>();
		for (int i = 0; i < NUMBER_OF_KEYS; i++) {
			keys.add("key" + i);
		}
		int notExistKeyCount = 0;

		long start = System.currentTimeMillis();
		for (String key : keys) {
			try {
				String value = (String) client.getKey(key).getValue();
				System.out.println(value);
			} catch (BitcaskNotExistedKeyException e) {
				notExistKeyCount++;
			}
		}

		long end = System.currentTimeMillis();

		System.out.println("Total time mils: " + (end - start));

		System.out.println("Total not exist key: " + notExistKeyCount);
		assertEquals(0, notExistKeyCount);
	}

	@Test
	public void testRemove() {
		List<String> keys = new ArrayList<>();
		for (int i = 0; i < NUMBER_OF_KEYS; i++) {
			keys.add("key" + i);
		}
		int notExistKeyCount = 0;

		long start = System.currentTimeMillis();
		for (String key : keys) {
			try {
				client.getKey(key).remove();
			} catch (BitcaskNotExistedKeyException e) {
				notExistKeyCount++;
			}
		}

		long end = System.currentTimeMillis();

		System.out.println("Total time mils: " + (end - start));
		System.out.println("Total not exist key: " + notExistKeyCount);
		assertEquals(0, notExistKeyCount);
	}

	@Test(expected = BitcaskException.class)
	public void testEmptyKey() {
		client.getKey("");
	}

	@Test(expected = BitcaskException.class)
	public void testKeyContainsWhiteSpace() {
		client.getKey("a key with spaces");
	}

}