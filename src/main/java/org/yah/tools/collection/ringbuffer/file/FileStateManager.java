package org.yah.tools.collection.ringbuffer.file;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;

import org.yah.tools.collection.ringbuffer.RingBufferState;
import org.yah.tools.collection.ringbuffer.StateManager;

public class FileStateManager implements StateManager {

	private final FileChannel fileChannel;

	private final ByteBuffer headerBuffer;

	public FileStateManager(FileChannel fileChannel) {
		this.fileChannel = fileChannel;
		this.headerBuffer = ByteBuffer.allocate(headerLength());
	}

	@Override
	public int headerLength() {
		return 3 * Integer.BYTES;
	}

	@Override
	public RingBufferState read(int capacity) throws IOException {
		int channelSize = (int) fileChannel.size();
		if (channelSize == 0) {
			RingBufferState res = newState(capacity);
			write(res);
			return res;
		} else {
			return readHeader();
		}
	}

	private RingBufferState newState(int capacity) throws IOException {
		return StateManager.super.read(capacity);
	}

	@Override
	public void write(RingBufferState state) throws IOException {
		IntBuffer intBuffer = headerBuffer.asIntBuffer();
		writeHeader(state, intBuffer);
		fileChannel.write(headerBuffer, 0);
		headerBuffer.flip();
	}

	private RingBufferState readHeader() throws IOException {
		headerBuffer.position(0);
		fileChannel.read(headerBuffer, 0);
		headerBuffer.flip();
		return readHeader(headerBuffer.asIntBuffer());
	}

	protected RingBufferState readHeader(IntBuffer intBuffer) throws IOException {
		int pos = intBuffer.get();
		int size = intBuffer.get();
		int capacity = intBuffer.get();
		return new RingBufferState(pos, 0, capacity, size);
	}

	protected void writeHeader(RingBufferState state, IntBuffer intBuffer) {
		intBuffer.put(state.position().position());
		intBuffer.put(state.size());
		intBuffer.put(state.position().capacity());
	}

}
