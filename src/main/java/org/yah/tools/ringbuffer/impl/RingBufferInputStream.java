package org.yah.tools.ringbuffer.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.ConcurrentModificationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class RingBufferInputStream extends InputStream {

	private final byte[] singleByte = new byte[1];

	protected final AbstractRingBuffer ringBuffer;

	private RingPosition ringPosition;

	private boolean closed;

	public RingBufferInputStream(AbstractRingBuffer ringBuffer) {
		this.ringBuffer = ringBuffer;
		this.ringPosition = ringBuffer.state().position();
	}


	@Override
	public int read() throws IOException {
		ReadSnapshot snapshot = ringBuffer.waitFor(this::snapshot, c -> closed || c.available() != 0);
		if (closed)
			throw new RingBufferClosedException();

		if (snapshot.available() < 0)
			throw new ConcurrentModificationException(snapshot.toString());

		read(snapshot, singleByte, 0, 1);
		return singleByte[0] & 0xFF;
	}

	public int read(long timeout, TimeUnit timeUnit) throws IOException, TimeoutException {
		ReadSnapshot snapshot = ringBuffer.waitFor(this::snapshot, c -> closed || c.available() != 0,
				timeout, timeUnit);
		if (closed)
			throw new RingBufferClosedException();

		if (snapshot == null)
			throw new TimeoutException();

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
		advance(length);
	}

	@Override
	public long skip(long n) throws IOException {
		ReadSnapshot snapshot = snapshot();
		int available = snapshot.available();
		if (available < 0)
			throw new ConcurrentModificationException();
		int skipped = Integer.min((int) n, available);
		advance(skipped);
		return skipped;
	}

	public final RingPosition ringPosition() {
		return ringPosition;
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

	public void updateCapacity(int newCapacity, RingBufferState fromState) {
		ringPosition = ringPosition.updateCapacity(newCapacity, fromState);
	}

	public void shrink(int newCapacity, RingBufferState fromState) {
		ringPosition = ringPosition.shrink(fromState.position(), newCapacity);
	}

	protected final void advance(int length) {
		synchronized (ringBuffer) {
			this.ringPosition = ringPosition.advance(length);
		}
	}

	protected final void setRingPosition(RingPosition position) {
		synchronized (ringBuffer) {
			this.ringPosition = position;
		}
	}

	protected ReadSnapshot snapshot() {
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
			if (state.capacity() != position.capacity())
				throw new IllegalStateException();
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
			return String.format("ReadSnapshot [state=%s, position=%s, available=%d]", state, position, available());
		}

	}
}