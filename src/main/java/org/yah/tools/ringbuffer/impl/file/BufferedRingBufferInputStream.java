package org.yah.tools.ringbuffer.impl.file;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

import org.yah.tools.ringbuffer.impl.RingBufferInputStream;
import org.yah.tools.ringbuffer.impl.RingBufferInputStream.ReadSnapshot;
import org.yah.tools.ringbuffer.impl.exceptions.RingBufferClosedException;
import org.yah.tools.ringbuffer.impl.exceptions.RingBufferConcurrentModificationException;

public class BufferedRingBufferInputStream extends InputStream {

	private final ByteBuffer buffer;

	private final RingBufferInputStream delegate;

	private boolean closed;

	public BufferedRingBufferInputStream(RingBufferInputStream delegate, int bufferSize) {
		this.delegate = Objects.requireNonNull(delegate, "delegate is null");
		buffer = ByteBuffer.allocate(bufferSize);
		buffer.limit(0);
	}

	@Override
	public int read() throws IOException {
		while (!closed && !buffer.hasRemaining()) {
			ReadSnapshot snapshot = delegate.awaitInput(s -> s);
			if (snapshot.available() < 0)
				throw new RingBufferConcurrentModificationException(snapshot.toString());
			fillBuffer();
		}
		
		if (closed)
			throw new RingBufferClosedException();
		
		return buffer.get() & 0xFF;
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
	public int read(byte[] target, final int offset, final int length) throws IOException {
		int remaining = length;
		int read = 0;
		if (buffer.hasRemaining()) {
			int available = Math.min(length, buffer.remaining());
			buffer.get(target, offset, available);
			read += available;
			remaining -= available;
		}

		if (remaining > buffer.capacity()) {
			// still more than the buffer capacity, do no use buffer for the rest
			read += delegate.read(target, offset + read, remaining);
		} else if (remaining > 0) {
			// buffer all that we can and return what we can
			int size = Math.min(fillBuffer(), remaining);
			buffer.get(target, offset + read, size);
			read += size;
		}
		return read;
	}
	
	@Override
	public int available() throws IOException {
		return buffer.remaining() + delegate.available();
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

	@Override
	public void close() throws IOException {
		delegate.close();
		closed = true;
	}
}
