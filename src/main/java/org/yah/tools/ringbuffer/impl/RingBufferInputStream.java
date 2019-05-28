package org.yah.tools.ringbuffer.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.yah.tools.ringbuffer.impl.RingBufferUtils.IOFunction;
import org.yah.tools.ringbuffer.impl.exceptions.RingBufferClosedException;
import org.yah.tools.ringbuffer.impl.exceptions.RingBufferConcurrentModificationException;

/**
 * {@link InputStream} used to read from the eldest element of the ring buffer
 * to the newest (FIFO).<br/>
 * This can be used concurrently with any writer threads. But it's not thread
 * safe, so only one thread can use this {@link InputStream} at the same time.
 * 
 */
public final class RingBufferInputStream extends InputStream {

	private final byte[] singleByte = new byte[1];

	private final AbstractRingBuffer ringBuffer;

	private RingPosition ringPosition;

	private boolean closed;

	private ReadSnapshot lastSnapshot;

	public RingBufferInputStream(AbstractRingBuffer ringBuffer) {
		this.ringBuffer = ringBuffer;
		this.ringPosition = ringBuffer.state().position();
	}

	@Override
	public int read() throws IOException {
		return awaitInput(this::readByte);
	}

	@Override
	public long skip(long n) throws IOException {
		ReadSnapshot snapshot = snapshot();
		int available = snapshot.available();
		if (available < 0)
			throw new RingBufferConcurrentModificationException(snapshot.toString());
		int skipped = Integer.min((int) n, available);
		synchronized (ringBuffer) {
			ringPosition = ringPosition.advance(skipped);
		}
		return skipped;
	}

	public RingPosition ringPosition() {
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

	public <T> T awaitInput(IOFunction<ReadSnapshot, T> handler) throws IOException {
		try {
			return this.awaitInput(handler, Long.MAX_VALUE, TimeUnit.MILLISECONDS);
		} catch (TimeoutException e) {
			throw new IllegalStateException("timed out without time out ??", e);
		}
	}

	public <T> T awaitInput(IOFunction<ReadSnapshot, T> handler, long timeout, TimeUnit timeUnit)
			throws TimeoutException, IOException {
		return ringBuffer.waitFor(this::snapshot,
				s -> closed || s.available() != 0, handler,
				timeout, timeUnit);
	}

	@Override
	public int read(byte[] target, int offset, int length) throws IOException {
		RingBufferUtils.validateBufferParams(target, offset, length);
		if (length == 0)
			return 0;

		ReadSnapshot snapshot;
		int read;
		// read without lock, and check afterward if we have read some concurrently
		// modified data
		do {
			snapshot = snapshot();
			// ensure that our current position has not been deleted
			if (snapshot.removed())
				throw new RingBufferConcurrentModificationException(snapshot.toString());

			// get size to read, either requested length, or only what's available
			read = Integer.min(length, snapshot.available());
			if (read == 0)
				break;

			snapshot.read(target, offset, read);
			lastSnapshot = snapshot;
		} while (!advance(snapshot, read));
		return read;
	}

	/**
	 * synchronized with ring buffer, we have a byte to read for sure
	 */
	private int readByte(ReadSnapshot snapshot) throws IOException {
		if (closed)
			throw new RingBufferClosedException();

		if (snapshot.removed())
			throw new RingBufferConcurrentModificationException(snapshot.toString());

		snapshot.read(singleByte, 0, 1);
		ringPosition = ringPosition.advance(1);
		return singleByte[0] & 0xFF;
	}

	private boolean advance(ReadSnapshot snapshot, int length) throws RingBufferConcurrentModificationException {
		synchronized (ringBuffer) {
			// check the current snapshot with the one used to make the copy from the buffer
			// When we where reading from snapshot buffer, the buffer could have been
			// truncated by another thread (remove), or rotated due to a capacity increase
			// (ensureCapacity).
			// In either way, we can not deliver the data that was read
			ReadSnapshot actualSnapshot = snapshot();

			if (actualSnapshot.state.position.after(snapshot.position)) {
				// We have read some data that are now removed, no need to try again
				throw new RingBufferConcurrentModificationException(snapshot.toString());
			}

			if (snapshot.position.position() != actualSnapshot.position.position()) {
				// Our ring position was changed by a concurrent writer who needed some space
				// and triggered a capacity grow
				// We have potentially read some invalid data, try again
				System.out.println("concurrent resize from " + snapshot + " to " + actualSnapshot);
				return false;
			}

			// data that we just read are still valid, advance to next position
			ringPosition = ringPosition.advance(length);
			return true;
		}
	}

	public void updateCapacity(int newCapacity, RingBufferState fromState) {
		ringPosition = ringPosition.updateCapacity(newCapacity, fromState);
	}

	public void shrink(int newCapacity, RingBufferState fromState) {
		ringPosition = ringPosition.shrink(fromState.position(), newCapacity);
	}

	private ReadSnapshot snapshot() {
		synchronized (ringBuffer) {
			return new ReadSnapshot(ringBuffer.linearBuffer(), ringBuffer.state(), ringPosition);
		}
	}

	/**
	 * A state of all the ring buffer properties and the reader ring position
	 * required to do reading asynchronously
	 */
	public static class ReadSnapshot {

		private final LinearBuffer linearBuffer;

		private final RingBufferState state;

		private final RingPosition position;

		private ReadSnapshot(LinearBuffer linearBuffer, RingBufferState state, RingPosition position) {
			this.linearBuffer = linearBuffer;
			this.state = state;
			this.position = position;
			if (state.capacity() != position.capacity())
				throw new IllegalStateException();
		}

		/**
		 * 
		 */
		public boolean removed() {
			return state.position.after(position);
		}

		private void read(byte[] target, int offset, int length) throws IOException {
			state.execute(position.position(), length, (p, l, o) -> linearBuffer.read(p, target, offset + o, l));
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