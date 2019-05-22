package org.yah.tools.collection.ringbuffer;

import java.io.IOException;
import java.io.InputStream;
import java.util.ConcurrentModificationException;
import java.util.function.UnaryOperator;

public class RingBufferInputStream extends InputStream {

	private final byte[] singleByte = new byte[1];

	private final AbstractRingBuffer ringBuffer;

	private RingPosition ringPosition;

	private boolean closed;

	public RingBufferInputStream(AbstractRingBuffer ringBuffer) {
		this.ringBuffer = ringBuffer;
		this.ringPosition = ringBuffer.state().position();
	}

	/**
	 * TODO : add timeout
	 */
	@Override
	public int read() throws IOException {
		ReadSnapshot snapshot = ringBuffer.waitFor(this::newSnapshot, c -> closed || c.available() != 0);
		if (closed)
			throw new RingBufferClosedException();

		if (snapshot.available() < 0)
			throw new ConcurrentModificationException();

		read(snapshot, singleByte, 0, 1);
		return singleByte[0] & 0xFF;
	}

	@Override
	public int read(byte[] target, int offset, int length) throws IOException {
		RingBufferUtils.validateBufferParams(target, offset, length);

		ReadSnapshot snapshot = snapshot();
		int available = snapshot.available();
		if (available < 0)
			throw new ConcurrentModificationException();
		int read = Integer.min(length, available);
		if (read == 0)
			return 0;

		read(snapshot, target, offset, read);
		return read;
	}

	protected void read(ReadSnapshot snapshot, byte[] target, int offset, int length) throws IOException {
		snapshot.read(target, offset, length);
		updateRingPosition(p -> p.advance(length));
	}

	@Override
	public long skip(long n) throws IOException {
		ReadSnapshot snapshot = snapshot();
		int available = snapshot.available();
		if (available < 0)
			throw new ConcurrentModificationException();
		int skipped = Integer.min((int) n, available);
		updateRingPosition(p -> p.advance(skipped));
		return skipped;
	}

	public final RingPosition ringPosition() {
		return ringPosition;
	}

	protected final AbstractRingBuffer ringBuffer() {
		return ringBuffer;
	}

	@Override
	public int available() throws IOException {
		return Math.max(0, snapshot().available());
	}

	@Override
	public void close() throws IOException {
		ringBuffer.removeInputStream(this);
		closed = true;
	}

	@Override
	public String toString() {
		return String.format("RingBufferInputStream[%s]", ringPosition);
	}

	protected void updateCapacity(int newCapacity, RingBufferState fromState) {
		ringPosition = ringPosition.updateCapacity(newCapacity, fromState);
	}

	protected void updateRingPosition(UnaryOperator<RingPosition> operator) {
		synchronized (ringBuffer) {
			this.ringPosition = operator.apply(ringPosition);
		}
	}

	private synchronized ReadSnapshot snapshot() {
		return newSnapshot();
	}

	protected ReadSnapshot newSnapshot() {
		synchronized (ringBuffer) {
			return new ReadSnapshot(ringBuffer.linearBuffer(), ringBuffer.state(), ringPosition);
		}
	}

	protected static class ReadSnapshot {

		protected final LinearBuffer linearBuffer;

		protected final RingBufferState state;

		protected final RingPosition position;

		protected ReadSnapshot(LinearBuffer linearBuffer, RingBufferState state, RingPosition position) {
			this.linearBuffer = linearBuffer;
			this.state = state;
			this.position = position;
		}

		protected void read(byte[] target, int offset, int length) throws IOException {
			state.execute(position.position(), length, (p, l, o) -> linearBuffer.read(p, target, offset + o, l));
		}

		protected void read(int position, byte[] target, int offset, int length) throws IOException {
			state.execute(position, length, (p, l, o) -> linearBuffer.read(p, target, offset + o, l));
		}

		public int available() {
			return state.availableToRead(position);
		}

		@Override
		public String toString() {
			return String.format("ReadSnapshot [state=%s, position=%s]", state, position);
		}

	}
}