package org.yah.tools.collection.ringbuffer;

import org.yah.tools.collection.ringbuffer.ArrayRingBuffer;

public class ArrayRingBufferTest extends AbstractRingBufferTest<ArrayRingBuffer> {

	@Override
	protected ArrayRingBuffer createRingBuffer(int capacity) {
		return new ArrayRingBuffer(capacity);
	}
}
