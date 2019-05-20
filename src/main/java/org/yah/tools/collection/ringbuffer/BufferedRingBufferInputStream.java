package org.yah.tools.collection.ringbuffer;

import java.io.IOException;

public class BufferedRingBufferInputStream extends RingBufferInputStream {

	private final byte[] buffer;

	private RingPosition bufferPosition;

	private int bufferSize;

	public BufferedRingBufferInputStream(AbstractRingBuffer ringBuffer, int bufferSize) {
		super(ringBuffer);
		buffer = new byte[bufferSize];
		bufferPosition = ringPosition();
	}

	@Override
	protected void read(ReadSnapshot snapshot, byte[] target, int offset, int length) throws IOException {
		BufferedReadSnapshot bufferedSnapshot = (BufferedReadSnapshot) snapshot;
		int remaining = length;
		remaining -= bufferedSnapshot.readFromBuffer(target, offset, length);
		if (remaining == 0) {
			updateRingPosition(p -> p.advance(length));
			return;
		}

		int read = length - remaining;
		RingPosition position = bufferedSnapshot.position.advance(read);
		if (remaining > buffer.length) {
			// still more than the buffer capacity, do no use buffer for the rest
			bufferedSnapshot.read(position.position(), target, offset + read, remaining);
			synchronized (ringBuffer()) {
				// advance the position
				updateRingPosition(p -> p.advance(length));
				// reset the buffer
				bufferPosition = ringPosition();
				bufferSize = 0;
			}
		} else {
			// buffer all we can
			// bufferedSnapshot.available() is >= length, so size >= remaining
			int size = Math.min(bufferedSnapshot.available(), buffer.length);
			// fill the buffer
			bufferedSnapshot.read(position.position(), buffer, 0, size);

			// copy part of the buffer to target
			System.arraycopy(buffer, 0, target, offset + read, remaining);

			synchronized (ringBuffer()) {
				// advance the position
				updateRingPosition(p -> p.advance(length));
				// update the buffer
				bufferPosition = position;
				bufferSize = size;
			}
		}
	}

	@Override
	protected void capacityUpdated(int newCapacity, State fromState) {
		synchronized (ringBuffer()) {
			super.capacityUpdated(newCapacity, fromState);
			bufferPosition = bufferPosition.updateCapacity(newCapacity, fromState);
		}
	}

	@Override
	protected ReadSnapshot newSnapshot() {
		return new BufferedReadSnapshot(this);
	}

	private class BufferedReadSnapshot extends ReadSnapshot {

		private final RingPosition bufferPosition;

		public BufferedReadSnapshot(BufferedRingBufferInputStream is) {
			super(is);
			this.bufferPosition = is.bufferPosition;
		}

		public int readFromBuffer(byte[] target, int offset, int length) {
			int bufferIndex = bufferIndex();
			int available = bufferSize - bufferIndex;
			if (available > 0) {
				int count = Math.min(available, length);
				System.arraycopy(BufferedRingBufferInputStream.this.buffer, bufferIndex, target, offset, count);
				return count;
			}
			return 0;
		}

		public int bufferIndex() {
			synchronized (ringBuffer()) {
				return position.substract(bufferPosition);
			}
		}
	}
}
