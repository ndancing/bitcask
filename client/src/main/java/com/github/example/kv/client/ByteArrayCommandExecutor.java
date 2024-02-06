package com.github.example.kv.client;

import java.io.IOException;

final class ByteArrayCommandExecutor implements CommandExecutor<String> {

	@Override
	public String execute(Client client, String command) throws IOException {
		return new String((byte[])client.execute(command.getBytes()));
	}

}
