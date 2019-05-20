package org.yah.tools.collection.ringbuffer.array;

import java.io.IOException;

import org.yah.tools.collection.ringbuffer.AbstractRingBufferTest;
import org.yah.tools.collection.ringbuffer.array.ArrayRingBuffer;

public class ArrayRingBufferTest extends AbstractRingBufferTest<ArrayRingBuffer> {

	@Override
	protected ArrayRingBuffer createRingBuffer(int capacity, int limit) throws IOException {
		return new ArrayRingBuffer(capacity, limit);
	}
}
