package org.yah.tools.queue.impl;

import java.io.IOException;
import java.io.InputStream;

final class CappedInputStream extends InputStream {

	private final InputStream delegate;

	private long remaining;

	public CappedInputStream(InputStream delegate) {
		this.delegate = delegate;
		this.remaining = Integer.MAX_VALUE;
	}

	public void limit(long limit) {
		this.remaining = limit;
	}

	public long remaining() {
		return remaining;
	}

	@Override
	public int read() throws IOException {
		if (remaining > 0) {
			int res = delegate.read();
			if (res == -1)
				return -1;
			remaining--;
			return res;
		}
		return -1;
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		if (remaining == 0)
			return -1;

		int l = (int) Math.min(len, remaining);
		int read = delegate.read(b, off, l);
		remaining -= read;
		return read;
	}

	@Override
	public long skip(long n) throws IOException {
		long min = Math.min(remaining, n);
		long skipped = delegate.skip(min);
		remaining -= skipped;
		return skipped;
	}

	@Override
	public int available() throws IOException {
		return (int) Math.min(remaining, delegate.available());
	}

	@Override
	public void close() throws IOException {
		delegate.close();
	}

	@Override
	public boolean markSupported() {
		return delegate.markSupported();
	}

	@Override
	public synchronized void mark(int readlimit) {
		delegate.mark(readlimit);
	}

	@Override
	public synchronized void reset() throws IOException {
		delegate.reset();
	}

	public void unlimit() {
		remaining = Integer.MAX_VALUE;
	}

}
