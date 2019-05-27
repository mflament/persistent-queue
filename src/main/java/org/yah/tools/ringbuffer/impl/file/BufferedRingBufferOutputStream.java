package org.yah.tools.ringbuffer.impl.file;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.function.Supplier;

import org.yah.tools.ringbuffer.impl.RingBufferOutputStream;

/**
 * An {@link OutputStream} buffering data to improve file performance.<br/>
 * Delegate to an {@link RingBufferOutputStream} to do the actual loading.
 */
public class BufferedRingBufferOutputStream extends OutputStream {

	private final byte[] singleByte = new byte[1];

	private final Supplier<OutputStream> delegateProvider;

	private final ByteBuffer buffer;

	private OutputStream ringBufferStream;

	public BufferedRingBufferOutputStream(Supplier<OutputStream> delegateProvider, int bufferSize) {
		this.delegateProvider = delegateProvider;
		this.buffer = ByteBuffer.allocate(bufferSize);
	}

	@Override
	public void write(int b) throws IOException {
		singleByte[0] = (byte) b;
		write(singleByte, 0, 1);
	}

	@Override
	public void write(byte[] source, int offset, int length) throws IOException {
		int remaining = length;
		if (buffer.remaining() > 0) {
			int chunk = Math.min(remaining, buffer.remaining());
			buffer.put(source, offset, chunk);
			remaining -= chunk;
			offset += chunk;
		}

		if (remaining > 0) {
			flushBuffer();
			if (remaining >= buffer.remaining())
				doWrite(source, offset, remaining);
			else
				buffer.put(source, offset, remaining);
		}
	}

	@Override
	public void flush() throws IOException {
		flushBuffer();
	}

	@Override
	public void close() throws IOException {
		flushBuffer();
		ringBufferStream.close();
	}

	private void flushBuffer() throws IOException {
		if (buffer.position() > 0) {
			buffer.flip();
			doWrite(buffer.array(), 0, buffer.limit());
			buffer.rewind().limit(buffer.capacity());
		}
	}

	private void doWrite(byte[] source, int offset, int remaining) throws IOException {
		if (ringBufferStream == null)
			ringBufferStream = delegateProvider.get();
		ringBufferStream.write(source, offset, remaining);
	}

}
