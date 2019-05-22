package org.yah.tools.collection.ringbuffer.file;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.yah.tools.collection.ringbuffer.AbstractRingBuffer;
import org.yah.tools.collection.ringbuffer.BufferedRingBufferInputStream;
import org.yah.tools.collection.ringbuffer.LinearBuffer;
import org.yah.tools.collection.ringbuffer.RingBufferInputStream;
import org.yah.tools.collection.ringbuffer.RingBufferState;
import org.yah.tools.collection.ringbuffer.RingBufferUtils;

public class FileRingBuffer extends AbstractRingBuffer {

	public static final int DEFAULT_CAPACITY = 128 * 1024;

	public static final int DEFAULT_READER_CACHE = 4 * 1024;

	private final int defaultReaderCache;

	private final FileChannel fileChannel;

	private final ByteBuffer headerBuffer;

	public FileRingBuffer(File file) throws IOException {
		this(file, DEFAULT_CAPACITY, -1, DEFAULT_READER_CACHE);
	}

	public FileRingBuffer(File file, int capacity) throws IOException {
		this(file, capacity, -1, DEFAULT_READER_CACHE);
	}

	public FileRingBuffer(File file, int capacity, int limit, int defaultReaderCache) throws IOException {
		super(limit);
		this.fileChannel = openChannel(file.toPath());
		this.defaultReaderCache = defaultReaderCache;
		this.headerBuffer = ByteBuffer.allocate(headerLength());
		RingBufferState state = readHeader(capacity);
		LinearBuffer linearBuffer = new FileLinearBuffer(state.capacity());
		// TODO : shrink to min(state.size, capacity) if capacity < state.capacity
		restore(state, linearBuffer);
	}

	@Override
	protected RingBufferInputStream createInputStream() {
		if (defaultReaderCache > 0)
			return new BufferedRingBufferInputStream(this, defaultReaderCache);
		return super.createInputStream();
	}

	public final BufferedRingBufferInputStream reader(int bufferSize) {
		BufferedRingBufferInputStream is = new BufferedRingBufferInputStream(this, bufferSize);
		addInputStream(is);
		return is;
	}

	@Override
	protected final void onStateChange(RingBufferState state) throws IOException {
		writeHeader(state);
	}

	@Override
	public void close() throws IOException {
		super.close();
		fileChannel.close();
	}

	@Override
	protected LinearBuffer allocate(int capacity) throws IOException {
		return new FileLinearBuffer(capacity);
	}

	private FileChannel openChannel(Path path) throws IOException {
		FileChannel channel = null;
		try {
			return FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE,
					StandardOpenOption.SYNC);
		} catch (IOException e) {
			RingBufferUtils.closeQuietly(channel);
			throw e;
		}
	}

	private RingBufferState readHeader(int capacity) throws IOException {
		int channelSize = (int) fileChannel.size();
		if (channelSize == 0) {
			RingBufferState res = newState(capacity);
			writeHeader(res);
			return res;
		} else {
			return readHeader();
		}
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

	private void writeHeader(RingBufferState state) throws IOException {
		IntBuffer intBuffer = headerBuffer.asIntBuffer();
		writeHeader(state, intBuffer);
		fileChannel.write(headerBuffer, 0);
		headerBuffer.flip();
	}

	protected void writeHeader(RingBufferState state, IntBuffer intBuffer) throws IOException {
		intBuffer.put(state.position().position());
		intBuffer.put(state.size());
		intBuffer.put(state.position().capacity());
	}

//	@Override
//	protected void write(byte[] source, int offset, int length) throws IOException {
//		super.write(source, offset, length);
//		fileChannel.force(true);
//	}

	protected int headerLength() {
		return Integer.BYTES * 3;
	}

	public class FileLinearBuffer implements LinearBuffer {

		private int capacity;

		public FileLinearBuffer(int capacity) {
			this.capacity = RingBufferUtils.nextPowerOfTwo(capacity);
		}

		@Override
		public int capacity() {
			return capacity;
		}

		@Override
		public void read(int position, byte[] target, int offset, int length) throws IOException {
			ByteBuffer dst = ByteBuffer.wrap(target, offset, length);
			int read = 0;
			while (dst.hasRemaining()) {
				int last = fileChannel.read(dst, headerLength() + position + read);
				if (last < 0)
					throw new EOFException();
				read += last;
			}
		}

		@Override
		public void write(int position, byte[] source, int offset, int length) throws IOException {
			ByteBuffer src = ByteBuffer.wrap(source, offset, length);
			int write = 0;
			while (src.hasRemaining()) {
				write += fileChannel.write(src, headerLength() + position + write);
			}
		}

		@Override
		public void copyTo(LinearBuffer target, int position, int targetPosition, int length) throws IOException {
			if (position == targetPosition)
				return;

			fileChannel.position(headerLength() + targetPosition);
			fileChannel.transferTo(headerLength() + position, length, fileChannel);
		}

	}
}
