package org.yah.tools.collection.ringbuffer;

import org.yah.tools.collection.ringbuffer.AbstractRingBuffer.LinearBuffer;
import org.yah.tools.collection.ringbuffer.ArrayRingBuffer.ArrayLinearBuffer;

/**
 * In memory with byte[] implementation of {@link RingBuffer}
 */
public class ArrayRingBuffer extends AbstractRingBuffer<ArrayLinearBuffer> {

	public ArrayRingBuffer(int capacity) {
		super(capacity);
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
