package org.yah.tools.collection.ringbuffer;

import java.io.IOException;
import java.io.OutputStream;

import org.yah.tools.collection.Utils;

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
		Utils.validateBufferParams(source, offset, length);
		synchronized (ringBuffer.writeMonitor) {
			State state = ensureCapacity(length);
			int writePosition = state.writePosition();
			state.execute(writePosition, length, (p, l, o) -> {
				ringBuffer.linearBuffer.write(p, source, offset + o, l);
			});
			
			ringBuffer.updateState(s -> s.incrementSize(length));
		}
	}

	@Override
	public void close() throws IOException {
		throw new UnsupportedOperationException();
	}

	private State ensureCapacity(int additional) throws IOException {
		// work with a state snapshot, it can change in time as follow:
		// - no other writer, so no other capacity change
		// - only concurrent read or remove:
		// - size can only grow
		// - write position will never change
		State state = ringBuffer.getState();
		int capacity = state.capacity();
		int available = state.availableToWrite();
		if (available < additional) {
			int missing = additional - available;
			int newCapacity = Utils.nextPowerOfTwo(capacity + missing);
			if (ringBuffer.inLimit(newCapacity)) {
				LinearBuffer newBuffer = ringBuffer.allocate(newCapacity);
				return ringBuffer.transferTo(newBuffer, state);
			} else {
				return ringBuffer.waitFor(ringBuffer::getState, s -> s.availableToWrite() >= additional);
			}
		}
		return state;
	}

}