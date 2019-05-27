package org.yah.tools.ringbuffer.impl;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

public final class RingBufferUtils {

	private RingBufferUtils() {}
	
	@FunctionalInterface
	public interface IOFunction<T, R> {
		R apply(T input) throws IOException;
	}
	
	public static final boolean isPowerOfTwo(int n) {
		return (n & (n - 1)) == 0;
	}

	/**
	 * @see http://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
	 */
	public static final int nextPowerOfTwo(int n) {
		n--;
		n |= n >> 1;
		n |= n >> 2;
		n |= n >> 4;
		n |= n >> 8;
		n |= n >> 16;
		n++;
		return n;
	}

	public static void validateBufferParams(byte[] buffer, int offset, int length) {
		Objects.requireNonNull(buffer, "buffer is null");
		if (offset < 0 || offset >= buffer.length)
			throw new IllegalArgumentException("Invalid offset " + offset);
		if (length < 0 || offset + length > buffer.length)
			throw new IllegalArgumentException("Invalid length " + length);
	}

	public static byte[] readFully(InputStream is, int size) throws IOException {
		byte[] res = new byte[size];
		readFully(is, res, 0, size);
		return res;
	}

	public static void readFully(InputStream is, byte[] dst, int offset, int length) throws IOException {
		validateBufferParams(dst, offset, length);
		int read = 0, remaining = length;
		while (remaining > 0) {
			int c = is.read(dst, read, remaining);
			if (c < 0)
				throw new EOFException();
			read += c;
			remaining -= c;
		}
	}

	public static void closeQuietly(Closeable closeable		) {
		if (closeable == null)
			return;
		try {
			closeable.close();
		} catch (IOException e) {
			// shhhh
		}
	}

}
