package org.yah.tools.ringbuffer.impl;

import java.io.IOException;
import java.io.OutputStream;

import org.yah.tools.ringbuffer.StreamRingBuffer;

/**
 * {@link OutputStream} allowing to append bytes to the ring buffer.<br/>
 * Data are directly written in the underlying {@link LinearBuffer}, but the
 * {@link StreamRingBuffer} state size increment is done when the stream is closed or
 * flushed, making the new data available to the readers.<br/>
 * <p>
 * Can be used with concurrent reader or remove, but not thread safe itself (do
 * not use an instance of RingBufferOutputStream in multiple threads).<br/>
 * A single {@link RingBufferOutputStream} can be used at a time,
 * {@link StreamRingBuffer} will wait for the previous one to be closed in order to
 * give a new one.
 * </p>
 */
public final class RingBufferOutputStream extends OutputStream {

	private final byte[] singleByte = new byte[1];

	protected final AbstractStreamRingBuffer ringBuffer;

	public RingBufferOutputStream(AbstractStreamRingBuffer ringBuffer) {
		this.ringBuffer = ringBuffer;
	}

	@Override
	public void write(int b) throws IOException {
		singleByte[0] = (byte) b;
		write(singleByte, 0, 1);
	}

	@Override
	public void write(byte[] source, int offset, int length) throws IOException {
		if (length == 0)
			return;
		RingBufferUtils.validateBufferParams(source, offset, length);

		RingPosition writePosition = ringBuffer.writePosition(length);
		LinearBuffer linearBuffer = ringBuffer.linearBuffer();
		writePosition.execute(length, (p, l, o) -> linearBuffer.write(p, source, offset + o, l));

		ringBuffer.addPendingWrite(length);
	}

	@Override
	public void flush() throws IOException {
		ringBuffer.flushWriter();
	}

	@Override
	public void close() throws IOException {
		flush();
		ringBuffer.releaseWriter(this);
	}

	@Override
	public String toString() {
		return String.format("RingBufferOutputStream [ringBuffer=%s, pending=%s]", ringBuffer,
				ringBuffer.pendingWrite());
	}

}