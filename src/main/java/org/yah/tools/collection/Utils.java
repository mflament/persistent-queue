package org.yah.tools.collection;

import java.util.Objects;

public final class Utils {

	private Utils() {}

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

}
