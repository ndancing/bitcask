package com.github.ndancing.kv.client;

import java.io.IOException;

public interface Client<I, R> {
	R execute(I input) throws IOException;

}
