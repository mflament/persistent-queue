package org.yah.tools.ringbuffer.impl.array;

import java.io.IOException;

import org.yah.tools.ringbuffer.impl.AbstractRingBufferTest;
import org.yah.tools.ringbuffer.impl.array.ArrayRingBuffer;

public class ArrayRingBufferTest extends AbstractRingBufferTest<ArrayRingBuffer> {

	@Override
	protected ArrayRingBuffer createRingBuffer(int capacity, int limit) throws IOException {
		return new ArrayRingBuffer(capacity, limit);
	}
}
