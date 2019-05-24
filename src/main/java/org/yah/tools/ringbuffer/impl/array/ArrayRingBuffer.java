package org.yah.tools.ringbuffer.impl.array;

import java.io.IOException;

import org.yah.tools.ringbuffer.RingBuffer;
import org.yah.tools.ringbuffer.impl.AbstractRingBuffer;
import org.yah.tools.ringbuffer.impl.LinearBuffer;
import org.yah.tools.ringbuffer.impl.RingBufferState;
import org.yah.tools.ringbuffer.impl.RingBufferUtils;

/**
 * In memory with byte[] implementation of {@link RingBuffer}
 */
public class ArrayRingBuffer extends AbstractRingBuffer {

	public ArrayRingBuffer(int capacity) throws IOException {
		this(capacity, -1);
	}

	public ArrayRingBuffer(int capacity, int limit) throws IOException {
		super(limit);
		capacity = RingBufferUtils.nextPowerOfTwo(capacity);
		if (limit > 0 && limit < capacity)
			throw new IllegalArgumentException("capacity " + capacity + " is greater than limit " + limit);
		restore(new RingBufferState(capacity), allocate(capacity));
	}

	@Override
	protected ArrayLinearBuffer allocate(int capacity) {
		return new ArrayLinearBuffer(capacity);
	}

	public static class ArrayLinearBuffer implements LinearBuffer {

		private final byte[] buffer;

		public ArrayLinearBuffer(int capacity) {
			this.buffer = new byte[capacity];
		}

		@Override
		public int capacity() {
			return buffer.length;
		}

		@Override
		public void read(int position, byte[] target, int offset, int length) {
			System.arraycopy(buffer, position, target, offset, length);
		}

		@Override
		public void write(int position, byte[] source, int offset, int length) {
			System.arraycopy(source, offset, buffer, position, length);
		}

		@Override
		public void copyTo(LinearBuffer target, int position, int targetPosition, int length) {
			byte[] targetBuffer = ((ArrayLinearBuffer) target).buffer;
			System.arraycopy(buffer, position, targetBuffer, targetPosition, length);
		}

	}

}
