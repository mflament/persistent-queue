package org.yah.tools.collection.ringbuffer;

import static java.util.Arrays.copyOfRange;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public abstract class AbstractRingBufferTest<R extends AbstractRingBuffer> {

	protected static final int CAPACITY = 16;
	protected static final int LIMIT = 64;

	protected R ringBuffer;

	@Before
	public void setUp() throws Exception {
		ringBuffer = createRingBuffer(CAPACITY, LIMIT);
	}

	@After
	public void close() throws IOException {
		closeBuffer();
	}

	protected void closeBuffer() throws IOException {}

	protected abstract R createRingBuffer(int capacity, int limit) throws IOException;

	@Test
	public void testWrap() {
		RingBufferState state = ringBuffer.state();
		assertEquals(state.wrap(0), 0);
		assertEquals(state.wrap(1), 1);
		assertEquals(state.wrap(CAPACITY), 0);
		assertEquals(state.wrap(CAPACITY + 1), 1);
		assertEquals(state.wrap(-1), CAPACITY - 1);
	}

	@Test
	public void testWrite() throws IOException {
		byte[] data = data(CAPACITY / 2);
		write(data);
		assertEquals(CAPACITY / 2, ringBuffer.size());
		assertEquals(CAPACITY, ringBuffer.capacity());

		data = data(CAPACITY - ringBuffer.size());
		write(data);
		assertEquals(CAPACITY, ringBuffer.size());
		assertEquals(CAPACITY, ringBuffer.capacity());
	}

	@Test
	public void test() throws IOException {
		// fill half of the buffer
		byte[] data = data(CAPACITY / 2);
		write(data);
		assertEquals(CAPACITY / 2, ringBuffer.size());
		assertEquals(CAPACITY, ringBuffer.capacity());

		// read using the whole buffer content
		byte[] actual = new byte[CAPACITY];
		int read = read(actual);
		assertEquals(CAPACITY / 2, read);
		assertArrayEquals(data, copyOfRange(actual, 0, CAPACITY / 2));

		// remove half of the buffer content
		int removed = ringBuffer.remove(CAPACITY / 4);
		assertEquals(CAPACITY / 4, removed);
		int remaining = CAPACITY / 4;
		assertEquals(remaining, ringBuffer.size());

		// read the remaining of the buffer
		read = read(actual);
		assertEquals(remaining, read);
		assertArrayEquals(copyOfRange(data, removed, data.length),
				copyOfRange(actual, 0, remaining));

		removed = ringBuffer.remove(CAPACITY);
		assertEquals(remaining, removed);
		assertEquals(0, ringBuffer.size());

		data = data(CAPACITY);
		write(data);
		assertEquals(CAPACITY, ringBuffer.size());

		// test consecutive read
		try (InputStream is = createReader()) {
			read = is.read(actual, 0, CAPACITY / 2);
			assertEquals(CAPACITY / 2, read);
			assertArrayEquals(copyOfRange(data, 0, read),
					copyOfRange(actual, 0, read));

			read = is.read(actual, 0, CAPACITY / 2);
			assertEquals(CAPACITY / 2, read);
			assertArrayEquals(copyOfRange(data, read, data.length),
					copyOfRange(actual, 0, read));
		}

		removed = ringBuffer.remove(CAPACITY / 2);
		assertEquals(CAPACITY / 2, removed);

		read = read(actual);
		assertEquals(CAPACITY / 2, read);
		assertArrayEquals(copyOfRange(data, read, data.length),
				copyOfRange(actual, 0, read));
	}

	@Test
	public void test_concurrent() throws IOException, InterruptedException {
		List<byte[]> actuals = new ArrayList<>();
		AtomicBoolean stopped = new AtomicBoolean();
		new Thread(() -> {
			try (InputStream is = createReader()) {
				byte[] buffer = new byte[CAPACITY];
				while (!stopped.get()) {
					int read = is.read(buffer);
					if (read > 0) {
						byte[] actual = new byte[read];
						System.arraycopy(buffer, 0, actual, 0, read);
						actuals.add(actual);
					}
				}
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		}).start();

		byte[] data = data(CAPACITY);
		int offset = 0;
		int[] chunkSizes = { 5, 2, 1, 4, 2, 2 };
		for (int i = 0; i < chunkSizes.length; i++) {
			write(data, offset, chunkSizes[i]);
			offset += chunkSizes[i];
			Thread.sleep(20);
		}
		stopped.set(true);
		offset = 0;
		for (int i = 0; i < chunkSizes.length; i++) {
			byte[] actual = actuals.get(i);
			assertArrayEquals(copyOfRange(data, offset, offset + chunkSizes[i]), actual);
			offset += chunkSizes[i];
		}
	}

	@Test // (timeout = 5000)
	public void test_concurrent_blocking() throws IOException, InterruptedException {
		byte[] actuals = new byte[CAPACITY];
		CountDownLatch processingLatch = new CountDownLatch(CAPACITY);
		CountDownLatch closeLatch = new CountDownLatch(1);
		Thread thread = new Thread(() -> {
			try (InputStream is = createReader()) {
				int index = 0;
				while (true) {
					int read = is.read();
					if (index < CAPACITY)
						actuals[index++] = (byte) read;
					processingLatch.countDown();
				}
			} catch (RingBufferClosedException e) {
				closeLatch.countDown();
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		});
		thread.start();

		byte[] data = data(CAPACITY);
		write(data, 0, data.length);

		processingLatch.await();

		ringBuffer.close();
		closeLatch.await();

		assertArrayEquals(data, actuals);
	}

	@Test
	public void test_concurrent_flood() throws IOException, InterruptedException, NoSuchAlgorithmException {
		closeBuffer();

		int messageSize = 36;
		int messageCount = 5000;
		int dataSize = messageSize * messageCount;

		ringBuffer = createRingBuffer(CAPACITY, 1024 * 1024);

		MessageDigest digest = MessageDigest.getInstance("SHA-1");
		byte[] data = data(dataSize);
		byte[] expectedDigest = digest.digest(data);
		digest.reset();

		CountDownLatch closeLatch = new CountDownLatch(1);
		Thread thread = new Thread(() -> {
			try (RingBufferInputStream is = createReader()) {
				int remaining = messageCount;
				while (remaining > 0) {
					byte[] msg = RingBufferUtils.readFully(is, messageSize);
					digest.update(msg);
					remaining--;
					ringBuffer.remove(messageSize);
				}
			} catch (RingBufferClosedException e) {
				// ignore
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				closeLatch.countDown();
			}
		});
		thread.start();

		for (int i = 0; i < messageCount; i++) {
			write(data, i * messageSize, messageSize);
		}

		closeLatch.await();
		ringBuffer.close();

		byte[] actualDigest = digest.digest();
		assertArrayEquals(expectedDigest, actualDigest);
	}

	private RingBufferInputStream createReader() {
		return ringBuffer.reader();
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

	@Test(expected = ConcurrentModificationException.class)
	public void test_concurrent_remove() throws IOException {
		byte[] data = data(CAPACITY);
		write(data, 0, CAPACITY);

		RingBufferInputStream is = (RingBufferInputStream) createReader();
		is.skip(CAPACITY / 4);
		assertEquals(CAPACITY / 4, is.ringPosition().position());

		ringBuffer.remove(CAPACITY / 2);
		is.read();
	}

	@Test
	public void test_resize_shift() throws IOException {
		byte[] data = data(CAPACITY);
		write(data, 0, CAPACITY);
		ringBuffer.remove(CAPACITY / 2);

		// wrap
		write(data, 0, CAPACITY / 2);

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

	protected int read(byte[] target) throws IOException {
		return read(target, 0, target.length);
	}

	protected int read(byte[] target, int offset, int length) throws IOException {
		try (InputStream is = createReader()) {
			return is.read(target, offset, length);
		}
	}

	protected void write(byte[] data) throws IOException {
		ringBuffer.writer().write(data);
	}

	protected void write(byte[] data, int offset, int length) throws IOException {
		ringBuffer.writer().write(data, offset, length);
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
