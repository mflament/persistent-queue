package org.yah.tools.collection.ringbuffer;

import java.io.File;

import org.junit.After;
import org.yah.tools.collection.ringbuffer.FileRingBuffer;

public class FileRingBufferTest extends AbstractRingBufferTest<FileRingBuffer> {

	@Override
	protected FileRingBuffer createRingBuffer(int capacity) {
		File parent = new File("target/test/ring-buffers");
		parent.mkdirs();
		File file = new File(parent, "test-ring-buffer.dat");
		if (file.exists() && !file.delete())
			throw new IllegalStateException("Unable to delete " + file);
		return new FileRingBuffer(file, capacity);
	}

	@After
	public void close() {
		ringBuffer.close();
	}

}
