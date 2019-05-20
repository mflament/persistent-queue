package org.yah.tools.collection.ringbuffer.file;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;
import org.yah.tools.collection.ringbuffer.AbstractRingBufferTest;
import org.yah.tools.collection.ringbuffer.file.FileRingBuffer;

public class FileRingBufferTest extends AbstractRingBufferTest<FileRingBuffer> {

	private File ringBufferFile = new File("target/test/ring-buffers/test-ring-buffer.dat");

	@Override
	protected FileRingBuffer createRingBuffer(int capacity, int limit) throws IOException {
		ringBufferFile.getParentFile().mkdirs();
		if (ringBufferFile.exists() && !ringBufferFile.delete())
			throw new IllegalStateException("Unable to delete " + ringBufferFile);
		return new FileRingBuffer(ringBufferFile, capacity, limit, FileRingBuffer.DEFAULT_READER_CACHE);
	}

	@Override
	protected void closeBuffer() throws IOException {
		ringBuffer.close();
	}

	private void reloadRingBuffer() throws IOException {
		ringBuffer.close();
		ringBuffer = new FileRingBuffer(ringBufferFile, CAPACITY);
	}

	@Test
	public void test_persistency() throws IOException {
		byte[] data = data(CAPACITY);
		write(data);
		ringBuffer.remove(CAPACITY / 2);

		reloadRingBuffer();
		assertEquals(CAPACITY / 2, ringBuffer.size());
		assertEquals(CAPACITY / 2, ringBuffer.getState().position());

		byte[] actual = new byte[CAPACITY / 2];
		int read = read(actual);
		assertEquals(CAPACITY / 2, read);
		assertArrayEquals(Arrays.copyOfRange(data, CAPACITY / 2, CAPACITY), actual);

		ringBuffer.remove(CAPACITY / 2);
		assertEquals(0, ringBuffer.size());
		assertEquals(0, ringBuffer.getState().position());
		assertEquals(1, ringBuffer.getState().cycle());

		reloadRingBuffer();
		assertEquals(0, ringBuffer.size());
		assertEquals(0, ringBuffer.getState().position());
		assertEquals(0, ringBuffer.getState().cycle());
	}
}
