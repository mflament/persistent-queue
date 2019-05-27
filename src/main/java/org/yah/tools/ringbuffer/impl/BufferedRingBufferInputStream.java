package org.yah.tools.ringbuffer.impl;

import java.io.IOException;
import java.nio.ByteBuffer;

public class BufferedRingBufferInputStream extends RingBufferInputStream {

	private final ByteBuffer buffer;

	public BufferedRingBufferInputStream(AbstractRingBuffer ringBuffer, int bufferSize) {
		super(ringBuffer);
		buffer = ByteBuffer.allocate(bufferSize);
		buffer.limit(0);
	}

	@Override
	protected void read(ReadSnapshot snapshot, byte[] target, int offset, int length) throws IOException {
		RingPosition position = snapshot.position;
		int remaining = length;
		if (buffer.hasRemaining()) {
			int available = Math.min(remaining, buffer.remaining());
			buffer.get(target, offset, available);
			offset += available;
			remaining -= available;
			position = position.advance(available);
		}

		if (remaining > buffer.capacity()) {
			// still more than the buffer capacity, do no use buffer for the rest
			snapshot.read(position.position(), target, offset, remaining);
		} else if (remaining > 0) {
			// buffer all that we can
			// bufferedSnapshot.available() is >= length, so size will be >= remaining
			
			int available = snapshot.state.availableToRead(position);
			int size = Math.min(available, buffer.capacity());
			snapshot.read(buffer.array(), 0, size);
			
			buffer.rewind().limit(size);

			buffer.get(target, offset, remaining);
		}
		
		advance(length);
	}

}
