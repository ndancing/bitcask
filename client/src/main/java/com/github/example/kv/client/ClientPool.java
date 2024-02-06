package com.github.example.kv.client;

public interface ClientPool<T extends Client> {

	T borrowClient();

	void returnClient(T client);

	void invalidateClient(T client);
}
