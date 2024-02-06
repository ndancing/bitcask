package com.github.example.kv.server;

import java.io.IOException;

import com.github.example.kv.storage.BitcaskStorageConfig;

public class Application {


	public static void main(String[] args) throws IOException {
		final int port = Integer.valueOf(System.getProperty("port"));
		final String storageDir = System.getProperty("storageDir");

		if (port <= 0 || storageDir.isEmpty()) {
			System.out.println("Invalid required parameters");
			System.exit(-2);
		}

		final BitcaskStorageConfig config = BitcaskStorageConfig.builder()
			.storageDir(storageDir)
			.cacheEnabled(true).cacheSize(10000)
			.fileSizeLimit(1024000).mergePeriodMils(300000)
			.build();

		final BitcaskServer server = new BitcaskServer(port, config);
		server.start();
	}
}
