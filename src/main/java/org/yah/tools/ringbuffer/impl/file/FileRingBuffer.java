package org.yah.tools.ringbuffer.impl.file;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.yah.tools.ringbuffer.impl.AbstractRingBuffer;
import org.yah.tools.ringbuffer.impl.BufferedRingBufferInputStream;
import org.yah.tools.ringbuffer.impl.LinearBuffer;
import org.yah.tools.ringbuffer.impl.RingBufferInputStream;
import org.yah.tools.ringbuffer.impl.RingBufferState;
import org.yah.tools.ringbuffer.impl.RingBufferUtils;

public class FileRingBuffer extends AbstractRingBuffer {

	public enum SyncMode {
		NONE,
		SYNC,
		FORCE;
	}

	public static final int DEFAULT_CAPACITY = 128 * 1024;

	public static final int DEFAULT_READER_CACHE = 4 * 1024;

	private final int defaultReaderCache;

	private final FileChannel fileChannel;

	private final SyncMode syncMode;

	private final ByteBuffer headerBuffer;

	protected FileRingBuffer(Builder builder) throws IOException {
		super(builder.limit);
		this.defaultReaderCache = builder.readerCacheSize;
		this.syncMode = builder.syncMode;
		this.fileChannel = openChannel(builder.file.toPath());
		headerBuffer = ByteBuffer.allocate(headerLength());

		RingBufferState state = readState(builder.capacity);
		// TODO : shrink to min(state.size, capacity) if capacity < state.capacity
		LinearBuffer linearBuffer = new FileLinearBuffer(state.capacity());
		restore(state, linearBuffer);
	}

	protected RingBufferState readState(int capacity) throws IOException {
		int channelSize = (int) fileChannel.size();
		if (channelSize == 0) {
			RingBufferState res = newState(capacity);
			writeState(res);
			return res;
		} else {
			return readState();
		}
	}

	protected RingBufferState newState(int requestedCapacity) {
		return new RingBufferState(requestedCapacity);
	}

	protected RingBufferState readState() throws IOException {
		headerBuffer.position(0);
		fileChannel.read(headerBuffer, 0);
		headerBuffer.flip();
		return readHeader(headerBuffer.asIntBuffer());
	}

	@Override
	protected void writeState(RingBufferState state) throws IOException {
		IntBuffer intBuffer = headerBuffer.asIntBuffer();
		writeHeader(state, intBuffer);
		fileChannel.write(headerBuffer, 0);
		headerBuffer.flip();
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

	@Override
	protected RingBufferInputStream createInputStream() {
		if (defaultReaderCache > 0)
			return new BufferedRingBufferInputStream(this, defaultReaderCache);
		return super.createInputStream();
	}

	public BufferedRingBufferInputStream reader(int bufferSize) {
		BufferedRingBufferInputStream is = new BufferedRingBufferInputStream(this, bufferSize);
		addInputStream(is);
		return is;
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
			List<StandardOpenOption> options = new ArrayList<>(Arrays.asList(StandardOpenOption.CREATE,
					StandardOpenOption.READ, StandardOpenOption.WRITE));
			if (syncMode == SyncMode.SYNC)
				options.add(StandardOpenOption.SYNC);
			return FileChannel.open(path, options.toArray(new StandardOpenOption[options.size()]));
		} catch (IOException e) {
			RingBufferUtils.closeQuietly(channel);
			throw e;
		}
	}

	@Override
	protected void write(byte[] source, int offset, int length) throws IOException {
		super.write(source, offset, length);
		if (syncMode == SyncMode.FORCE)
			fileChannel.force(true);
	}

	protected int headerLength() {
		return 3 * Integer.BYTES;
	}

	public class FileLinearBuffer implements LinearBuffer {

		private final int headerLength;

		private int capacity;

		public FileLinearBuffer(int capacity) {
			this.capacity = RingBufferUtils.nextPowerOfTwo(capacity);
			this.headerLength = headerLength();
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
				int last = fileChannel.read(dst, headerLength + position + read);
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
				write += fileChannel.write(src, headerLength + position + write);
			}
		}

		@Override
		public void copyTo(LinearBuffer target, int position, int targetPosition, int length) throws IOException {
			if (position == targetPosition)
				return;

			fileChannel.position(headerLength + targetPosition);
			fileChannel.transferTo(headerLength + position, length, fileChannel);
		}
	}

	public static Builder builder(File file) {
		return new Builder(file);
	}

	public static Builder builder() {
		return new Builder();
	}

	public static class Builder {

		private File file = new File("queue.dat");

		private int capacity = 128 * 1024;

		private int limit = 5 * 1024 * 1024;

		private int readerCacheSize = 8 * 1024;

		protected SyncMode syncMode = SyncMode.NONE;

		protected Builder() {}

		public Builder(File file) {
			this.file = file;
		}

		public Builder withFile(File file) {
			this.file = file;
			return this;
		}

		public Builder withCapacity(int capacity) {
			this.capacity = capacity;
			return this;
		}

		public Builder withLimit(int limit) {
			this.limit = limit;
			return this;
		}

		public Builder withDefaultReaderCache(int defaultReaderCache) {
			this.readerCacheSize = defaultReaderCache;
			return this;
		}

		public Builder withSyncMode(SyncMode syncMode) {
			this.syncMode = syncMode;
			return this;
		}

		public FileRingBuffer build() throws IOException {
			return new FileRingBuffer(this);
		}
	}
}
