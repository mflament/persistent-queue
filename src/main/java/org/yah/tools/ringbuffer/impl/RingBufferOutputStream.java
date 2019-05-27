package org.yah.tools.ringbuffer.impl;

import java.io.IOException;
import java.io.OutputStream;

public class RingBufferOutputStream extends OutputStream {

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