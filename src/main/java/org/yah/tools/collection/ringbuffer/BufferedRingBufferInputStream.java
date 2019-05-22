package org.yah.tools.collection.ringbuffer;

import java.io.EOFException;
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
		RingPosition readPosition = bufferedSnapshot.position.advance(read);
		if (remaining > buffer.length) {
			// still more than the buffer capacity, do no use buffer for the rest
			bufferedSnapshot.read(readPosition.position(), target, offset + read, remaining);
			synchronized (ringBuffer()) {
				// advance the position
				updateRingPosition(p -> p.advance(length));
				// reset the buffer
				bufferPosition = ringPosition();
				bufferSize = 0;
			}
		} else {
			// buffer all we can
			// bufferedSnapshot.available() is >= length, so size will be >= remaining
			int available = bufferedSnapshot.state.availableToRead(readPosition);
			int size = Math.min(available, buffer.length);
			// fill the buffer
			try {
				bufferedSnapshot.read(readPosition.position(), buffer, 0, size);
			} catch (EOFException e) {
				System.out.println("size: " + size);
				System.out.println("snapshot: " + snapshot);
				System.out.println("ringBuffer: " + ringBuffer());
				throw e;
			}

			// copy part of the buffer to target
			System.arraycopy(buffer, 0, target, offset + read, remaining);

			synchronized (ringBuffer()) {
				// advance the position
				updateRingPosition(p -> p.advance(length));
				// update the buffer
				bufferPosition = readPosition;
				bufferSize = size;
			}
		}
	}

	@Override
	protected void updateCapacity(int newCapacity, RingBufferState fromState) {
		super.updateCapacity(newCapacity, fromState);
		bufferPosition = bufferPosition.updateCapacity(newCapacity, fromState);
	}

	@Override
	protected ReadSnapshot newSnapshot() {
		AbstractRingBuffer ringBuffer = ringBuffer();
		synchronized (ringBuffer) {
			return new BufferedReadSnapshot(ringBuffer.linearBuffer(), ringBuffer.state(), ringPosition());
		}
	}

	private RingPosition bufferPosition() {
		return bufferPosition;
	}

	private class BufferedReadSnapshot extends ReadSnapshot {

		private final RingPosition bufferPosition;

		public BufferedReadSnapshot(LinearBuffer linearBuffer, RingBufferState state, RingPosition position) {
			super(linearBuffer, state, position);
			this.bufferPosition = bufferPosition();
		}

		public final int readFromBuffer(byte[] target, int offset, int length) {
			int bufferIndex = bufferIndex();
			int available = bufferIndex >= 0 ? bufferSize - bufferIndex : 0;
			if (available > 0) {
				int count = Math.min(available, length);
				System.arraycopy(BufferedRingBufferInputStream.this.buffer, bufferIndex, target, offset, count);
				return count;
			}
			return 0;
		}

		private int bufferIndex() {
			synchronized (ringBuffer()) {
				return position.substract(bufferPosition);
			}
		}

		@Override
		public String toString() {
			return String.format("BufferedReadSnapshot [state=%s, position=%s, bufferPosition=%s]",
					state, position, bufferPosition);
		}

	}

	public static void main(String[] args) {
		RingPosition p = new RingPosition(119072, 44, 131072);
		RingPosition bp = new RingPosition(114984, 44, 131072);
		System.out.println(p.substract(bp));
	}
}
