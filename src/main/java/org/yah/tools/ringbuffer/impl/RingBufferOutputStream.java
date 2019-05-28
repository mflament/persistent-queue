package org.yah.tools.ringbuffer.impl;

import java.io.IOException;
import java.io.OutputStream;

import org.yah.tools.ringbuffer.RingBuffer;

/**
 * {@link OutputStream} allowing to append bytes to the ring buffer.<br/>
 * Data are directly written in the underlying {@link LinearBuffer}, but the
 * {@link RingBuffer} state size increment is done when the stream is closed or
 * flushed, making the new data available to the readers.<br/>
 * <p>
 * Can be used with concurrent reader or remove, but not thread safe itself (do
 * not use an instance of RingBufferOutputStream in multiple threads).<br/>
 * A single {@link RingBufferOutputStream} can be used at a time,
 * {@link RingBuffer} will wait for the previous one to be closed in order to
 * give a new one.
 * </p>
 */
public final class RingBufferOutputStream extends OutputStream {

	private final byte[] singleByte = new byte[1];

	private final AbstractRingBuffer ringBuffer;

	private int pending;

	public RingBufferOutputStream(AbstractRingBuffer ringBuffer) {
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

		RingBufferState state = ringBuffer.ensureCapacity(pending + length);
		RingPosition writePosition = state.writePosition(pending);
		LinearBuffer linearBuffer = ringBuffer.linearBuffer();
		state.execute(writePosition.position(), length, (p, l, o) -> linearBuffer.write(p, source, offset + o, l));

		pending += length;
	}

	@Override
	public void flush() throws IOException {
		if (pending > 0) {
			ringBuffer.updateState(s -> s.incrementSize(pending));
			pending = 0;
		}
	}

	@Override
	public void close() throws IOException {
		flush();
		ringBuffer.releaseWriter(this);
	}

	@Override
	public String toString() {
		return String.format("RingBufferOutputStream [ringBuffer=%s, pending=%s]", ringBuffer, pending);
	}

}