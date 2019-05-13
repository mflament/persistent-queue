package org.yah.tools.collection;

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
	
}
