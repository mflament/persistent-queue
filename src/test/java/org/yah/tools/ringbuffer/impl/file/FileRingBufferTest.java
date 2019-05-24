package org.yah.tools.ringbuffer.impl.file;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;
import org.yah.tools.ringbuffer.impl.AbstractRingBufferTest;
import org.yah.tools.ringbuffer.impl.file.FileRingBuffer;

public class FileRingBufferTest extends AbstractRingBufferTest<FileRingBuffer> {

	private File ringBufferFile = new File("target/test/ring-buffers/test-ring-buffer.dat");

	@Override
	protected FileRingBuffer createRingBuffer(int capacity, int limit) throws IOException {
		ringBufferFile.getParentFile().mkdirs();
		if (ringBufferFile.exists() && !ringBufferFile.delete())
			throw new IllegalStateException("Unable to delete " + ringBufferFile);
		return FileRingBuffer.builder(ringBufferFile).withCapacity(capacity).withLimit(limit).build();
	}

	@Override
	protected void closeBuffer() throws IOException {
		ringBuffer.close();
	}

	private void reloadRingBuffer() throws IOException {
		ringBuffer.close();
		ringBuffer = FileRingBuffer.builder(ringBufferFile).withCapacity(CAPACITY).build();
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

	private void assertStatePosition(int expected) {
		assertEquals(expected, ringBuffer.state().position().position());
	}
}
