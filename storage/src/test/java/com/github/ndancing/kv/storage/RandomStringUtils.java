package com.github.ndancing.kv.storage;

import java.util.Random;

public class RandomStringUtils {

	private RandomStringUtils() {
	}

	private static final Random random = new Random();
	private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

	public static String generate(int byteLength) {
		final StringBuilder sb = new StringBuilder(byteLength);
		for (int i = 0; i < byteLength; i++) {
			sb.append(CHARACTERS.charAt(random.nextInt(CHARACTERS.length())));
		}
		return sb.toString();
	}
}
