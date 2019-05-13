package org.yah.tools.collection.ringbuffer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.yah.tools.collection.ringbuffer.AbstractRingBuffer;

public abstract class AbstractRingBufferTest<R extends AbstractRingBuffer<?>> {

	protected static final int CAPACITY = 16;
	protected static final int LIMIT = 64;

	protected R ringBuffer;

	@Before
	public void setUp() throws Exception {
		ringBuffer = createRingBuffer(CAPACITY);
		ringBuffer.setLimit(LIMIT);
	}

	protected abstract R createRingBuffer(int capacity);

	@Test
	public void testWrap() {
		assertEquals(ringBuffer.wrap(0), 0);
		assertEquals(ringBuffer.wrap(1), 1);
		assertEquals(ringBuffer.wrap(CAPACITY), 0);
		assertEquals(ringBuffer.wrap(CAPACITY + 1), 1);
		assertEquals(ringBuffer.wrap(-1), CAPACITY - 1);
	}

	@Test
	public void test() {
		byte[] data = data(CAPACITY / 2);
		ringBuffer.write(data);
		assertEquals(CAPACITY / 2, ringBuffer.size());
		assertEquals(CAPACITY, ringBuffer.capacity());

		byte[] actual = new byte[CAPACITY];
		int read = ringBuffer.read(0, actual);
		assertEquals(CAPACITY / 2, read);
		assertArrayEquals(data, Arrays.copyOfRange(actual, 0, CAPACITY / 2));

		int removed = ringBuffer.remove(CAPACITY / 4);
		assertEquals(CAPACITY / 4, removed);

		int remaining = CAPACITY / 2 - removed;
		assertEquals(remaining, ringBuffer.size());

		read = ringBuffer.read(0, actual);
		assertEquals(remaining, read);
		assertArrayEquals(Arrays.copyOfRange(data, removed, data.length),
				Arrays.copyOfRange(actual, 0, remaining));

		removed = ringBuffer.remove(CAPACITY);
		assertEquals(remaining, removed);
		assertEquals(0, ringBuffer.size());

		data = data(CAPACITY);
		ringBuffer.write(data);
		assertEquals(CAPACITY, ringBuffer.size());

		read = ringBuffer.read(0, actual);
		assertEquals(CAPACITY, read);
		assertArrayEquals(data, actual);

		read = ringBuffer.read(CAPACITY / 2, actual);
		assertEquals(CAPACITY / 2, read);
		assertArrayEquals(Arrays.copyOfRange(data, CAPACITY / 2, data.length), Arrays.copyOfRange(actual, 0, read));

		removed = ringBuffer.remove(CAPACITY / 2);
		assertEquals(CAPACITY / 2, removed);
		read = ringBuffer.read(0, actual);
		assertEquals(CAPACITY / 2, read);
		assertArrayEquals(Arrays.copyOfRange(data, CAPACITY / 2, data.length), Arrays.copyOfRange(actual, 0, read));
	}

	@Test
	public void testEnsureCapacity() {
		assertEquals(CAPACITY, ringBuffer.capacity());
		ringBuffer.write(data(CAPACITY + 1));
		assertEquals(CAPACITY + 1, ringBuffer.size());
		assertTrue(ringBuffer.capacity() > CAPACITY);
	}

	@Test
	public void testEnsureCapacity_wrapped() {
		byte[] data = data(CAPACITY);
		ringBuffer.write(data, 0, CAPACITY / 2);
		ringBuffer.remove(CAPACITY / 2);
		ringBuffer.write(data);

		byte[] actual = new byte[CAPACITY];
		int read = ringBuffer.read(0, actual);
		assertEquals(CAPACITY, read);
		assertArrayEquals(data, actual);
		assertEquals(CAPACITY / 2, ringBuffer.startPosition());

		ringBuffer.write(data);

		assertTrue(ringBuffer.capacity() > CAPACITY);
		assertEquals(CAPACITY * 2, ringBuffer.size());

		actual = new byte[CAPACITY];
		read = ringBuffer.read(0, actual);
		assertEquals(CAPACITY, read);
		assertArrayEquals(data, actual);

		read = ringBuffer.read(CAPACITY, actual);
		assertEquals(CAPACITY, read);
		assertArrayEquals(data, actual);
	}

	protected final byte[] data(int size) {
		return data(size, 0);
	}

	protected final byte[] data(int size, int offset) {
		byte[] res = new byte[size];
		for (int i = 0; i < res.length; i++) {
			res[i] = (byte) (i + offset);
		}
		return res;
	}
}
