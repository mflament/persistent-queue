package org.yah.tools.collection.ringbuffer;

import java.nio.BufferOverflowException;
import java.util.Objects;

import org.yah.tools.collection.Utils;
import org.yah.tools.collection.ringbuffer.AbstractRingBuffer.LinearBuffer;

/**
 * Abstract implementation of {@link RingBuffer}<br/>
 */
public abstract class AbstractRingBuffer<T extends LinearBuffer> implements RingBuffer {

	private int limit;

	private int startPosition;

	private int size;

	private long cycle;

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
		if (startPosition < 0 || startPosition >= size)
			throw new IllegalArgumentException("Invalid start position " + startPosition);
		if (size < 0)
			throw new IllegalArgumentException("Invalid size " + size);
		this.startPosition = startPosition;
		this.size = size;
	}

	protected final T linearBuffer() {
		return linearBuffer;
	}

	protected final int startPosition() {
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

	public synchronized State getState() {
		return new State(startPosition, cycle, size, linearBuffer.capacity());
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

		update(startPosition, size + length);
	}

	@Override
	public int remove(int removed) {
		removed = Math.min(size, removed);
		int newStartPosition = startPosition + removed;
		if (newStartPosition > capacity()) {
			newStartPosition = wrap(newStartPosition);
			cycle++;
		}
		update(newStartPosition, size - removed);
		return removed;
	}

	private synchronized int writePosition() {
		return startPosition + size;
	}

	private void update(int startPosition, int size) {
		synchronized (this) {
			this.startPosition = startPosition;
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

			if (writePosition > capacity) {
				// wrapped, transfer tail of ring from buffer start to buffer end
				writePosition = wrap(writePosition, capacity);
				linearBuffer.copyTo(newBuffer, startPosition, startPosition, capacity - startPosition);
				linearBuffer.copyTo(newBuffer, 0, capacity, writePosition);
			} else {
				linearBuffer.copyTo(newBuffer, startPosition, startPosition, writePosition - startPosition);
			}
			linearBuffer = newBuffer;
		}
	}

	protected void onChange(int startPosition, int size) {}

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

	public static final class State {
		private final int startPosition;
		private final long cycle;
		private final int size;
		private final int capacity;

		private State(int startPosition, long cycle, int size, int capacity) {
			super();
			this.startPosition = startPosition;
			this.cycle = cycle;
			this.size = size;
			this.capacity = capacity;
		}

		public int getStartPosition() {
			return startPosition;
		}

		public long getCycle() {
			return cycle;
		}

		public int getSize() {
			return size;
		}

		public int getCapacity() {
			return capacity;
		}

		@Override
		public String toString() {
			return String.format("State [startPosition=%s, cycle=%s, size=%s, capacity=%s]", startPosition, cycle, size,
					capacity);
		}

	}
}
