package org.yah.tools.ringbuffer.impl.file;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import org.junit.Test;
import org.yah.tools.ringbuffer.impl.AbstractStreamRingBufferTest;
import org.yah.tools.ringbuffer.impl.RingBufferState;
import org.yah.tools.ringbuffer.impl.file.FileRingBuffer;

public class FileRingBufferTest extends AbstractStreamRingBufferTest<FileRingBuffer> {

	private File ringBufferFile = new File("target/test/ring-buffers/test-ring-buffer.dat");

	@Override
	public void setup() throws Exception {
		ringBufferFile.getParentFile().mkdirs();
		if (ringBufferFile.exists() && !ringBufferFile.delete())
			throw new IllegalStateException("Unable to delete " + ringBufferFile);
		super.setup();
	}

	@Override
	protected FileRingBuffer createRingBuffer(int capacity) throws IOException {
		return FileRingBuffer.builder(ringBufferFile)
			.withLimit(capacity)
			.withDefaultReaderCache(0)
			.withWriteBufferSize(0)
			.build();
	}

	@Override
	protected FileRingBuffer createFloodBuffer() throws IOException {
		if (ringBufferFile.exists() && !ringBufferFile.delete())
			throw new IllegalStateException("Unable to delete " + ringBufferFile);
		return FileRingBuffer.builder(ringBufferFile)
			.withLimit(1024 * 1024)
			.withDefaultReaderCache(4 * 1024)
			.withWriteBufferSize(4 * 1024)
			.withWriteTimeout(Long.MAX_VALUE)
			.build();
	}

	@Override
	protected void closeBuffer() throws IOException {
		ringBuffer.close();
	}

	private void reloadRingBuffer() throws IOException {
		ringBuffer.close();
		ringBuffer = FileRingBuffer.builder(ringBufferFile).withLimit(CAPACITY).build();
	}

	@Test
	public void test_persistency() throws IOException {
		byte[] data = data(CAPACITY);
		write(data);
		ringBuffer.remove(CAPACITY / 2);

		reloadRingBuffer();
		assertEquals(CAPACITY / 2, ringBuffer.size());
		assertStatePosition(CAPACITY / 2);

		byte[] actual = new byte[CAPACITY / 2];
		int read = read(actual);
		assertEquals(CAPACITY / 2, read);
		assertArrayEquals(Arrays.copyOfRange(data, CAPACITY / 2, CAPACITY), actual);

		ringBuffer.remove(CAPACITY / 2);
		assertEquals(0, ringBuffer.size());
		assertStatePosition(0);
		assertEquals(1, ringBuffer.state().cycle());

		reloadRingBuffer();
		assertEquals(0, ringBuffer.size());
		assertStatePosition(0);
		assertEquals(0, ringBuffer.state().cycle());
	}

	@Test
	public void test_capacity_increase() throws IOException {
		assertEquals(CAPACITY, ringBuffer.limit());

		write(data(CAPACITY));
		ringBuffer.remove(CAPACITY / 2);
		write(data(CAPACITY / 2));

		closeBuffer();
		assertEquals(ringBuffer.headerLength() + CAPACITY, ringBufferFile.length());

		ringBuffer = createRingBuffer(CAPACITY * 2);
		RingBufferState state = ringBuffer.state();
		assertEquals(8, state.position().position());
		assertEquals(CAPACITY, state.size());
		assertEquals(CAPACITY * 2, state.capacity());
		assertEquals(CAPACITY * 2, ringBuffer.limit());

		byte[] actuals = new byte[CAPACITY / 2];
		try (InputStream is = ringBuffer.reader()) {
			int read = is.read(actuals);
			assertEquals(CAPACITY / 2, read);
			assertArrayEquals(data(CAPACITY / 2, CAPACITY / 2), actuals);

			read = is.read(actuals);
			assertEquals(CAPACITY / 2, read);
			assertArrayEquals(data(CAPACITY / 2), actuals);
		}
	}

	@Test
	public void test_shrink() throws IOException {
		assertEquals(CAPACITY, ringBuffer.limit());
		write(data(CAPACITY / 2));

		closeBuffer();
		ringBuffer = createRingBuffer(CAPACITY);
		assertEquals(CAPACITY, ringBuffer.capacity());
		assertEquals(CAPACITY, ringBuffer.limit());

		closeBuffer();
		ringBuffer = createRingBuffer(CAPACITY / 2);
		assertEquals(CAPACITY / 2, ringBuffer.capacity());
		assertEquals(CAPACITY / 2, ringBuffer.limit());
		assertEquals(ringBuffer.headerLength() + CAPACITY / 2, ringBufferFile.length());

		closeBuffer();
		ringBuffer = createRingBuffer(CAPACITY);
		assertEquals(CAPACITY, ringBuffer.capacity());
		assertEquals(CAPACITY, ringBuffer.limit());
		assertEquals(ringBuffer.headerLength() + CAPACITY / 2, ringBufferFile.length());

		write(data(CAPACITY / 4));
		ringBuffer.remove(CAPACITY / 2 + CAPACITY / 4);

		write(data(CAPACITY / 2));
		assertEquals(3 * CAPACITY / 4, ringBuffer.state().position().position());
		assertEquals(0, ringBuffer.state().position().cycle());
		assertEquals(CAPACITY / 2, ringBuffer.size());

		closeBuffer();
		ringBuffer = createRingBuffer(CAPACITY / 2);
		assertEquals(CAPACITY / 2, ringBuffer.capacity());
		assertEquals(CAPACITY / 2, ringBuffer.limit());
		assertEquals(0, ringBuffer.state().position().position());
		assertEquals(0, ringBuffer.state().position().cycle());
		assertEquals(CAPACITY / 2, ringBuffer.size());
		byte[] actuals = new byte[CAPACITY / 2];
		int read = read(actuals);
		assertEquals(CAPACITY / 2, read);
		assertArrayEquals(data(CAPACITY / 2), actuals);
	}

	private void assertStatePosition(int expected) {
		assertEquals(expected, ringBuffer.state().position().position());
	}
}
