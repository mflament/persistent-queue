package org.yah.tools.ringbuffer.impl.file;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.yah.tools.ringbuffer.impl.AbstractRingBuffer;
import org.yah.tools.ringbuffer.impl.BufferedRingBufferInputStream;
import org.yah.tools.ringbuffer.impl.LinearBuffer;
import org.yah.tools.ringbuffer.impl.RingBufferState;
import org.yah.tools.ringbuffer.impl.RingBufferUtils;

public class FileRingBuffer extends AbstractRingBuffer {

	private static final int HEADER_LENGTH = 3 * Integer.BYTES;

	public enum SyncMode {
		NONE,
		SYNC,
		FORCE;
	}

	public static final int DEFAULT_CAPACITY = 128 * 1024;

	public static final int DEFAULT_READER_CACHE = 4 * 1024;

	private final int defaultReaderCache;

	private final int defaultWriteBufferSize;

	private final int requestedCapacity;

	private final Path file;

	private final SyncMode syncMode;

	private final ByteBuffer headerBuffer;

	private FileChannel fileChannel;

	protected FileRingBuffer(Builder builder) throws IOException {
		super(builder.limit);
		this.defaultReaderCache = builder.readerCacheSize;
		this.defaultWriteBufferSize = builder.defaultWriteBufferSize;
		this.syncMode = builder.syncMode;
		this.requestedCapacity = builder.capacity;
		this.file = builder.file.toPath();
		this.fileChannel = openChannel();
		headerBuffer = ByteBuffer.allocate(headerLength());
		RingBufferState state = readState();
		LinearBuffer linearBuffer = new FileLinearBuffer(state.capacity());
		restore(state, linearBuffer);
	}

	public void shrink() throws IOException {
		updateState(this::shrink);
	}

	protected RingBufferState readState() throws IOException {
		int channelSize = (int) fileChannel.size();
		if (channelSize == 0) {
			RingBufferState res = newState(requestedCapacity);
			writeState(res);
			return res;
		} else {
			headerBuffer.position(0);
			fileChannel.read(headerBuffer, 0);
			headerBuffer.flip();
			return readHeader(headerBuffer.asIntBuffer());
		}
	}

	protected RingBufferState newState(int requestedCapacity) {
		return new RingBufferState(requestedCapacity);
	}

	@Override
	public OutputStream writer() {
		return writer(defaultWriteBufferSize);
	}

	public OutputStream writer(int bufferSize) {
		return new BufferedRingBufferOutputStream(super::writer, bufferSize);
	}

	@Override
	protected void writeState(RingBufferState state) throws IOException {
		IntBuffer intBuffer = headerBuffer.asIntBuffer();
		writeHeader(state, intBuffer);
		fileChannel.write(headerBuffer, 0);
		if (syncMode == SyncMode.FORCE)
			fileChannel.force(false);
		headerBuffer.flip();
	}

	protected int headerLength() {
		return HEADER_LENGTH;
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
	public InputStream reader() {
		return reader(defaultReaderCache);
	}

	public synchronized InputStream reader(int bufferSize) {
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

	private FileChannel openChannel() throws IOException {
		FileChannel channel = null;
		try {
			List<StandardOpenOption> options = new ArrayList<>(Arrays.asList(StandardOpenOption.CREATE,
					StandardOpenOption.READ, StandardOpenOption.WRITE));
			if (syncMode == SyncMode.SYNC) {
				options.add(StandardOpenOption.DSYNC);
			}
			return FileChannel.open(file, options.toArray(new StandardOpenOption[options.size()]));
		} catch (IOException e) {
			RingBufferUtils.closeQuietly(channel);
			throw e;
		}
	}

	private RingBufferState shrink(RingBufferState state) throws IOException {
		int required = Math.max(requestedCapacity, RingBufferUtils.nextPowerOfTwo(state.size()));
		if (required < state.capacity()) {
			int position = state.position().position();
			if (state.size() > 0 && position > 0)
				rewindBuffer(state);
			fileChannel.truncate(headerLength() + required);
			forEachInputStreams(is -> is.shrink(required, state));
			return state.shrink(required);
		}
		return state;
	}

	private void rewindBuffer(RingBufferState state) throws IOException {
		int position = state.position().position();
		Path tempFile = file.getParent().resolve(file.getFileName() + ".tmp");
		try (FileChannel tempFileChannel = FileChannel.open(tempFile,
				StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
				StandardOpenOption.READ, StandardOpenOption.WRITE)) {

			int headerLength = headerLength();
			fileChannel.transferTo(0, headerLength, tempFileChannel);
			if (state.wrapped()) {
				fileChannel.transferTo(position + headerLength, state.capacity() - position, tempFileChannel);
				fileChannel.transferTo(headerLength, state.writePosition(), tempFileChannel);
			} else {
				fileChannel.transferTo(position + headerLength, state.size(), tempFileChannel);
			}
		}

		fileChannel.close();
		Path backupFile = Files.move(file, file.getParent().resolve(file.getFileName() + ".bak"),
				StandardCopyOption.REPLACE_EXISTING);
		Files.move(tempFile, file);
		fileChannel = openChannel();
		Files.delete(backupFile);
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

			int headerLength = headerLength();
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

		private int defaultWriteBufferSize = 4 * 1024;

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

		public Builder withDefaultWriteBufferSize(int defaultWriteBufferSize) {
			this.defaultWriteBufferSize = defaultWriteBufferSize;
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
