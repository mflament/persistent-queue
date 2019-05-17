package org.yah.tools.collection.ringbuffer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.BufferOverflowException;
import java.util.Objects;

import org.yah.tools.collection.Utils;
import org.yah.tools.collection.ringbuffer.AbstractRingBuffer.LinearBuffer;

/**
 * Abstract implementation of {@link RingBuffer}<br/>
 */
public abstract class AbstractRingBuffer<T extends LinearBuffer> implements RingBuffer {

	private int limit;

	private final RingPosition startPosition = new RingPosition();

	private int size;

	private T linearBuffer;

	protected AbstractRingBuffer() {}

	protected AbstractRingBuffer(int capacity) {
		if (limit > 0 && capacity > limit)
			throw new IllegalArgumentException("capacity " + capacity + " is greater than limit " + limit);
		this.linearBuffer = allocate(Utils.nextPowerOfTwo(capacity));
	}

	protected final void setLinearBuffer(T linearBuffer) {
		if (!Utils.isPowerOfTwo(linearBuffer.capacity()))
			throw new IllegalArgumentException("LinearBuffer capacity must be a power of 2");
		this.linearBuffer = linearBuffer;
	}

	public final void setLimit(int limit) {
		this.limit = limit > 0 ? Utils.nextPowerOfTwo(limit) : 0;
	}

	protected final void restore(int startPosition, int size) {
		this.startPosition.position = startPosition;
		this.size = size;
	}

	protected final T linearBuffer() {
		return linearBuffer;
	}

	protected final RingPosition startPosition() {
		return startPosition;
	}

	@Override
	public final int size() {
		return size;
	}

	protected final int capacity() {
		return linearBuffer.capacity();
	}

	protected final int wrap(int position) {
		return wrap(position, capacity());
	}

	@Override
	public int read(int offset, byte[] target, int targetOffset, int length) {
		validateBufferParams(target, targetOffset, length);
		if (offset < 0 || offset >= size)
			throw new IllegalArgumentException("Invalid offset " + offset);

		int readPosition = wrap(startPosition + offset);
		int readLength = Math.min(size - offset, length);
		execute(readPosition, readLength, (p, l, o) -> {
			linearBuffer.read(p, target, targetOffset + o, l);
		});
		return readLength;
	}

	@Override
	public void write(byte[] source, int offset, int length) {
		validateBufferParams(source, offset, length);
		if (length == 0)
			return;

		ensureCapacity(length);
		int writePosition = writePosition();
		execute(writePosition, length, (p, l, o) -> {
			linearBuffer.write(p, source, offset + o, l);
		});

		updateSize(size + length);
	}

	@Override
	public synchronized int remove(int count) {
		int removed = Math.min(size, count);
		startPosition.advance(removed, capacity());
		this.size -= removed;
		return removed;
	}

	@Override
	public InputStream reader() {
		return new RingBufferInputStream();
	}

	@Override
	public OutputStream writer() {
		// TODO Auto-generated method stub
		return null;
	}

	private synchronized int writePosition() {
		return startPosition.position + size;
	}

	private void updateSize(int size) {
		synchronized (this) {
			this.size = size;
		}
		onChange(startPosition, size);
	}

	private void ensureCapacity(int additional) {
		// avoid concurrent buffer resize
		int required = size + additional;
		int capacity = capacity();
		if (capacity < required) {
			int newCapacity = Utils.nextPowerOfTwo(required);
			if (limit > 0 && newCapacity > limit)
				throw new BufferOverflowException();

			T newBuffer = allocate(newCapacity);
			int writePosition = writePosition();

			int pos = startPosition.position;
			if (writePosition > capacity) {
				// wrapped, transfer tail of ring from buffer start to buffer end
				writePosition = wrap(writePosition, capacity);
				linearBuffer.copyTo(newBuffer, pos, pos, capacity - pos);
				linearBuffer.copyTo(newBuffer, 0, capacity, writePosition);
			} else {
				linearBuffer.copyTo(newBuffer, pos, pos, writePosition - pos);
			}
			linearBuffer = newBuffer;
		}
	}

	protected void onChange(RingPosition startPosition, int size) {}

	protected final void execute(int position, int length, RingAction action) {
		int capacity = capacity();
		if (length > capacity)
			throw new IllegalArgumentException("Invalid length " + length + ", current capacity " + capacity);

		if (length <= 0)
			return;

		position = wrap(position);
		int endPosition = wrap(position + length);
		if (endPosition <= position) {
			// wrapped
			int tail = capacity - position;
			action.apply(position, tail, 0);
			action.apply(0, endPosition, tail);
		} else {
			action.apply(position, length, 0);
		}
	}

	protected final void validateBufferParams(byte[] buffer, int offset, int length) {
		Objects.requireNonNull(buffer, "buffer is null");
		if (offset < 0 || offset >= buffer.length)
			throw new IllegalArgumentException("Invalid offset " + offset);
		if (length < 0 || offset + length > buffer.length)
			throw new IllegalArgumentException("Invalid length " + length);
	}

	protected abstract T allocate(int capacity);

	protected static int wrap(int position, int capacity) {
		return position & (capacity - 1);
	}

	protected class RingPosition {

		private long cycle;

		private int position;

		public RingPosition() {}

		public RingPosition(long cycle, int position) {
			this.cycle = cycle;
			this.position = position;
		}

		public RingPosition(RingPosition pos) {
			cycle = pos.cycle;
			position = pos.position;
		}

		private void advance(int length, int capacity) {
			int nextPos = position + length;
			if (nextPos > capacity) {
				nextPos = wrap(nextPos, capacity);
				cycle++;
			}
			position = nextPos;
		}
	}

	@FunctionalInterface
	protected interface RingAction {
		void apply(int position, int length, int offset);
	}

	protected interface LinearBuffer {

		int capacity();

		void read(int position, byte[] target, int offset, int length);

		void write(int position, byte[] source, int offset, int length);

		void copyTo(LinearBuffer target, int position, int targetPosition, int length);

	}

	protected final class RingBufferInputStream extends InputStream {

		private byte[] singleByteBuffer = new byte[1];

		private final RingPosition ringPosition;

		public RingBufferInputStream() {
			this.ringPosition = new RingPosition(startPosition);
		}

		@Override
		public int read() throws IOException {
			if (ringPosition.)
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			int available = available();
		
			// TODO Auto-generated method stub
			return super.read(b, off, len);
		}

		@Override
		public long skip(long n) throws IOException {
			// TODO Auto-generated method stub
			return super.skip(n);
		}

		@Override
		public int available() throws IOException {

			// TODO Auto-generated method stub
			return super.available();
		}

	}
}
