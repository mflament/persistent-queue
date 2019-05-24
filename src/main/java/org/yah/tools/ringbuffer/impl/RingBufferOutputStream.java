package org.yah.tools.ringbuffer.impl;

import java.io.IOException;
import java.io.OutputStream;

public class RingBufferOutputStream extends OutputStream {

	private final AbstractRingBuffer ringBuffer;

	public RingBufferOutputStream(AbstractRingBuffer ringBuffer) {
		this.ringBuffer = ringBuffer;
	}

	@Override
	public void write(int b) throws IOException {
		write(new byte[] { (byte) b }, 0, 1);
	}

	@Override
	public void write(byte[] source, int offset, int length) throws IOException {
		if (length == 0)
			return;
		RingBufferUtils.validateBufferParams(source, offset, length);
		ringBuffer.write(source, offset, length);
	}

	@Override
	public void close() throws IOException {
		throw new UnsupportedOperationException();
	}

}