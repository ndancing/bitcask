package com.github.example.kv.server;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.example.kv.server.command.BitcaskStorageCommandHandler;
import com.github.example.kv.server.command.BitcaskStorageCommandParser;
import com.github.example.kv.server.command.KVCommandHandler;
import com.github.example.kv.storage.BitcaskStorage;
import com.github.example.kv.storage.BitcaskStorageConfig;
import com.github.example.kv.storage.MergeableKeyValueStorage;

public class BitcaskServer {

	private static final Logger LOGGER = LoggerFactory.getLogger(BitcaskServer.class);
	private final int port;
	private final BitcaskStorageConfig storageConfig;
	private final MergeableKeyValueStorage<String, String> storage;
	protected final KVCommandHandler<String> commandHandler;
	private final ScheduledExecutorService storageMergeActionExecutor;

	/**
	 * Default storage configurations
	 * */
	public BitcaskServer(int port) throws IOException {
		this(port, BitcaskStorageConfig.builder().build());
	}

	public BitcaskServer(int port, BitcaskStorageConfig storageConfig) throws IOException {
		this.port = port;
		this.storageConfig = storageConfig;
		this.storage = new BitcaskStorage(storageConfig);
		this.commandHandler = new BitcaskStorageCommandHandler(this.storage, new BitcaskStorageCommandParser());
		this.storageMergeActionExecutor = Executors.newScheduledThreadPool(1);
	}

	public void start() {
		scheduleStorageMerge();
		new BServerSocket().start();
		new BCommandLine().start();
	}

	private void scheduleStorageMerge() {
		this.storageMergeActionExecutor.scheduleAtFixedRate(() -> {
			try {
				this.storage.merge();
			} catch (IOException e) {
				LOGGER.error(e.getMessage(), e);
			}
		}, this.storageConfig.getMergePeriodMils(), this.storageConfig.getMergePeriodMils(), TimeUnit.MILLISECONDS);
	}

	private class BServerSocket extends Thread {
		@Override
		public void run() {
			try (ServerSocket serverSocket = new ServerSocket(port)) {
				while (true) {
					try (Socket socket = serverSocket.accept();
						 BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
						 DataOutputStream output = new DataOutputStream(socket.getOutputStream())) {
						String commandInput;
						while ((commandInput = input.readLine()) != null) {
							LOGGER.info(commandInput);
							final String serverResult = commandHandler.handle(commandInput);
							if (serverResult != null) {
								output.writeBytes(serverResult + "\n");
							}
						}
					}
				}
			} catch (IOException e) {
				LOGGER.error(e.getMessage(), e);
			}
		}
	}

	private class BCommandLine extends Thread {
		@Override
		public void run() {
			try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
				String commandInput;
				while ((commandInput = reader.readLine()) != null) {
					if ("exit".equals(commandInput)) {
						System.exit(2);
					}
					final String serverResult = commandHandler.handle(commandInput);
					if (serverResult != null) {
						System.out.println(serverResult);
					}
				}
			} catch (IOException e) {
				LOGGER.error(e.getMessage(), e);
			}
		}
	}
}
