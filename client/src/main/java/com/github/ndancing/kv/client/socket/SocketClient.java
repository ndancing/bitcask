package com.github.ndancing.kv.client.socket;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;

import com.github.ndancing.kv.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SocketClient implements Client<byte[], byte[]> {

	private static final Logger LOGGER = LoggerFactory.getLogger(SocketClient.class);
	private Socket client;
	private BufferedReader reader;
	private BufferedWriter writer;

	public SocketClient(String host, int port, int timeOutMils) throws IOException {
		try {
			final SocketAddress socketAddress = new InetSocketAddress(host, port);
			this.client = new Socket();

			this.client.connect(socketAddress, timeOutMils);
			this.client.setSoTimeout(timeOutMils);
			this.client.setTcpNoDelay(true);
			this.client.setKeepAlive(true);

			this.reader = new BufferedReader(new InputStreamReader(this.client.getInputStream()));
			this.writer = new BufferedWriter(new OutputStreamWriter(this.client.getOutputStream()));
			this.writer.flush();
		} catch (IOException e) {
			throw new IOException(String.format("Fail to init socket client with host %s; port %d", host, port), e);
		}
	}

	@Override
	public byte[] execute(byte[] messageBytes) throws IOException {
		final String message = new String(messageBytes);
		try {
			this.writer.write(message);
			this.writer.newLine();
			this.writer.flush();

			final String line = reader.readLine();
			return line.getBytes();
		} catch (IOException e) {
			throw new IOException(String.format("Error sending message %s to server socket", message), e);
		}
	}

	public void close() {
		if (this.client != null) {
			try {
				this.client.close();
			} catch (IOException e) {
				LOGGER.error("Error to close socket client", e);
			} finally {
				this.client = null;
			}
		}
	}

	public boolean isValid() {
		if (this.client != null) {
			return this.client.isClosed();
		}
		return false;
	}

	public void activate() {
		// Not implemented yet
	}

	public void passivate() {
		// Not implemented yet
	}
}
