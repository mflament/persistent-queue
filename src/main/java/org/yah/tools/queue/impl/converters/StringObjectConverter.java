package org.yah.tools.queue.impl.converters;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import org.yah.tools.queue.impl.ObjectConverter;
import org.yah.tools.ringbuffer.impl.RingBufferUtils;

public class StringObjectConverter implements ObjectConverter<String> {

	public static final ObjectConverter<String> INSTANCE = new StringObjectConverter();

	private static final int MAX_SIZE = 1 << 16;

	@Override
	public String read(InputStream inputStream) throws IOException {
		int s = readSize(inputStream);
		byte[] buffer = RingBufferUtils.readFully(inputStream, s);
		return new String(buffer, StandardCharsets.UTF_8);
	}

	@Override
	public void write(String string, OutputStream outputStream) throws IOException {
		byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
		writeSize(outputStream, bytes.length);
		outputStream.write(bytes);
	}

	private static final int readSize(InputStream is) throws IOException {
		int read = safeRead(is) << 8;
		read |= safeRead(is);
		return read;
	}

	private static final void writeSize(OutputStream os, int size) throws IOException {
		if (size > MAX_SIZE)
			throw new IllegalArgumentException("Invalid size " + size + ", max is " + MAX_SIZE);
		os.write((size >> 8) & 0xFF);
		os.write(size & 0xFF);
	}

	private static int safeRead(InputStream is) throws IOException {
		int res = is.read();
		if (res == -1)
			throw new EOFException();
		return res;
	}
}
