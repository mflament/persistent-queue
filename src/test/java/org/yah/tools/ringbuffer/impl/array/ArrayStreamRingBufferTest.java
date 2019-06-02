package org.yah.tools.ringbuffer.impl.array;

import static java.util.Arrays.copyOfRange;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;

import org.junit.Test;
import org.yah.tools.ringbuffer.impl.AbstractStreamRingBufferTest;
import org.yah.tools.ringbuffer.impl.RingBufferInputStream;

public class ArrayStreamRingBufferTest extends AbstractStreamRingBufferTest<ArrayStreamRingBuffer> {

	protected static final int LIMIT = 64;

	@Override
	protected ArrayStreamRingBuffer createRingBuffer(int capacity) throws IOException {
		return new ArrayStreamRingBuffer(capacity, LIMIT, 0);
	}

	@Override
	protected ArrayStreamRingBuffer createFloodBuffer() throws IOException {
		return new ArrayStreamRingBuffer(CAPACITY, 1024 * 1024, Long.MAX_VALUE);
	}

	@Test
	public void testEnsureCapacity() throws IOException {
		assertEquals(CAPACITY, ringBuffer.capacity());
		write(data(CAPACITY + 1));
		assertEquals(CAPACITY + 1, ringBuffer.size());
		assertTrue(ringBuffer.capacity() > CAPACITY);
	}

	@Test
	public void testEnsureCapacity_wrapped() throws IOException {
		byte[] data = data(CAPACITY);

		write(data);

		ringBuffer.remove(CAPACITY / 2);

		write(data);

		assertTrue(ringBuffer.capacity() > CAPACITY);
		assertEquals(CAPACITY + CAPACITY / 2, ringBuffer.size());

		byte[] actual = new byte[CAPACITY];
		try (InputStream is = createReader()) {
			int read = is.read(actual, 0, CAPACITY / 2);
			assertEquals(CAPACITY / 2, read);
			assertArrayEquals(copyOfRange(data, CAPACITY / 2, CAPACITY), copyOfRange(actual, 0, CAPACITY / 2));

			read = is.read(actual);
			assertEquals(CAPACITY, read);
			assertArrayEquals(data, actual);

			read = is.read(actual);
			assertEquals(0, read);
		}
	}

	@Test
	public void test_resize_shift() throws IOException {
		byte[] data = data(CAPACITY);
		write(data, 0, CAPACITY);
		ringBuffer.remove(CAPACITY / 2);

		// wrap
		write(data, 0, CAPACITY / 2);
		ringBuffer.toString();
		RingBufferInputStream is1 = (RingBufferInputStream) createReader();
		is1.skip(CAPACITY / 2);
		assertEquals(0, is1.ringPosition().position());
		assertEquals(1, is1.ringPosition().cycle());

		RingBufferInputStream is2 = (RingBufferInputStream) createReader();
		is2.skip(CAPACITY / 2 + CAPACITY / 4);
		assertEquals(CAPACITY / 4, is2.ringPosition().position());
		assertEquals(1, is1.ringPosition().cycle());

		RingBufferInputStream is3 = (RingBufferInputStream) createReader();
		is3.skip(CAPACITY / 4);
		assertEquals(CAPACITY / 2 + CAPACITY / 4, is3.ringPosition().position());
		assertEquals(0, is3.ringPosition().cycle());

		// resize
		write(data, 0, CAPACITY / 2);
		assertEquals(CAPACITY + CAPACITY / 2, ringBuffer.size());

		assertEquals(CAPACITY, is1.ringPosition().position());
		assertEquals(0, is1.ringPosition().cycle());

		assertEquals(CAPACITY + CAPACITY / 4, is2.ringPosition().position());
		assertEquals(0, is2.ringPosition().cycle());

		assertEquals(CAPACITY / 2 + CAPACITY / 4, is3.ringPosition().position());
		assertEquals(0, is3.ringPosition().cycle());
	}

}
