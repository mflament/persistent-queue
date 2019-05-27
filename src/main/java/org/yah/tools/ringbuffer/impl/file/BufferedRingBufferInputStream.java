package org.yah.tools.ringbuffer.impl.file;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

import org.yah.tools.ringbuffer.impl.RingBufferInputStream;

public class BufferedRingBufferInputStream extends InputStream {

	private final ByteBuffer buffer;

	private final RingBufferInputStream delegate;

	public BufferedRingBufferInputStream(RingBufferInputStream delegate, int bufferSize) {
		this.delegate = Objects.requireNonNull(delegate, "delegate is null");
		buffer = ByteBuffer.allocate(bufferSize);
		buffer.limit(0);
	}

	@Override
	public int read() throws IOException {
		while (!buffer.hasRemaining()) {
			delegate.awaitInput(s -> s);
			fillBuffer();
		}
		return buffer.get();
	}

	@Override
	public long skip(long n) throws IOException {
		int remaining = buffer.remaining();
		if (remaining > n) {
			buffer.position(buffer.position() + (int) n);
			return n;
		}

		long skipped = delegate.skip(n - remaining);
		resetBuffer();
		return remaining + skipped;
	}

	@Override
	public int read(byte[] target, int offset, int length) throws IOException {
		int remaining = length;
		if (buffer.hasRemaining()) {
			int available = Math.min(remaining, buffer.remaining());
			buffer.get(target, offset, available);
			offset += available;
			remaining -= available;
		}

		if (remaining > buffer.capacity()) {
			// still more than the buffer capacity, do no use buffer for the rest
			offset += delegate.read(target, offset, remaining);
		} else if (remaining > 0) {
			// buffer all that we can and return what we can
			int size = Math.min(fillBuffer(), remaining);
			buffer.get(target, offset, size);
			offset += size;
		}
		return offset;
	}

	private int fillBuffer() throws IOException {
		int size = Math.min(delegate.available(), buffer.capacity());
		int read = delegate.read(buffer.array(), 0, size);
		buffer.rewind().limit(read);
		return read;
	}

	private void resetBuffer() {
		buffer.rewind().limit(0);
	}

}
