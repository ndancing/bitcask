package com.github.example.kv.client;

import java.io.IOException;
import java.util.function.Function;

import com.github.example.kv.client.exception.BitcaskException;

public abstract class BitcaskData<E> {

	protected final ClientPool<Client> clientPool;
	protected final CommandExecutor<E> clientCommandExecutor;

	protected BitcaskData(ClientPool<Client> clientPool, CommandExecutor<E> clientCommandExecutor) {
		this.clientPool = clientPool;
		this.clientCommandExecutor = clientCommandExecutor;
	}

	protected <R> R commandExecute(String command, Function<E, R> responseHandler) {
		final Client client = clientPool.borrowClient();
		if (client == null) {
			throw new BitcaskException("No connection");
		}
		try {
			final E result = this.clientCommandExecutor.execute(client, command);
			clientPool.returnClient(client);
			return responseHandler.apply(result);
		} catch (IOException e) {
			clientPool.invalidateClient(client);
			throw new BitcaskException("Bad request");
		}
	}
}
