package com.github.example.kv.client;

import java.io.IOException;

public interface CommandExecutor<R> {

	R execute(Client client, String command) throws IOException;

}
