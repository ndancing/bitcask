package com.github.ndancing.kv.server.command;

import java.io.Serializable;

/**
 *
 * @param <R> Response Type
 */
public interface KVCommandHandler<R extends Serializable> {

	R handle(String input);
}
