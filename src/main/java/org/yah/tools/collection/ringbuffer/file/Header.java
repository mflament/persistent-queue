package org.yah.tools.collection.ringbuffer.file;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;

public class Header {

	private static final int POSTION_OFFSET = 0;
	private static final int SIZE_OFFSET = 1;

	protected static final int LAST_OFFSET = SIZE_OFFSET;

	private final ByteBuffer buffer;

	protected final IntBuffer dataBuffer;

	public Header() {
		buffer = ByteBuffer.allocate(length());
		dataBuffer = buffer.asIntBuffer();
		setStartPosition(0);
		setSize(0);
	}

	public final ByteBuffer getBuffer() {
		return buffer;
	}

	public final int getStartPosition() {
		return dataBuffer.get(POSTION_OFFSET);
	}

	public final void setStartPosition(int startPosition) {
		dataBuffer.put(POSTION_OFFSET, startPosition);
	}

	public final int getSize() {
		return dataBuffer.get(SIZE_OFFSET);
	}

	public final void setSize(int size) {
		dataBuffer.put(SIZE_OFFSET, size);
	}

	public void read(FileChannel fileChannel) throws IOException {
		fileChannel.read(buffer, 0);
		buffer.flip();
	}

	public void write(FileChannel fileChannel) throws IOException {
		fileChannel.write(buffer, 0);
		buffer.flip();
	}

	public int length() {
		return 2 * Integer.BYTES;
	}

	@Override
	public String toString() {
		return String.format("Header [startPosition=%d, size=%d]", getStartPosition(), getSize());
	}

}