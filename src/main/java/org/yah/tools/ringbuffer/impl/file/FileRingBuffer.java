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

import org.yah.tools.ringbuffer.impl.AbstractStreamRingBuffer;
import org.yah.tools.ringbuffer.impl.LinearBuffer;
import org.yah.tools.ringbuffer.impl.RingBufferState;
import org.yah.tools.ringbuffer.impl.RingBufferUtils;

public class FileRingBuffer extends AbstractStreamRingBuffer {

	private static final int HEADER_LENGTH = 3 * Integer.BYTES;

	public enum SyncMode {
		NONE,
		SYNC,
		FORCE;
	}

	public static final int DEFAULT_CAPACITY = 128 * 1024;

	public static final int DEFAULT_READER_CACHE = 4 * 1024;

	private final int readerCache;

	private final int writeBufferSize;

	private final int requestedLimit;

	private final Path file;

	private final SyncMode syncMode;

	private final ByteBuffer headerBuffer;

	private FileChannel fileChannel;

	protected FileRingBuffer(Builder builder) throws IOException {
		super(builder.limit, builder.writeTimeout);
		this.readerCache = builder.readerCacheSize;
		this.writeBufferSize = builder.writeBufferSize;
		this.syncMode = builder.syncMode;
		this.requestedLimit = RingBufferUtils.nextPowerOfTwo(builder.limit);
		this.file = builder.file.toPath();
		this.fileChannel = openChannel();
		headerBuffer = ByteBuffer.allocate(headerLength());

		RingBufferState state = readState();
		// retrieve disk space if possible
		state = shrink(state);

		if (requestedLimit > state.capacity()) {
			// capacity increased, transfer the end of ring buffer from start of file to end
			// of file to keep buffer continuous
			transferWrappedTail(state);
			state = state.withCapacity(requestedLimit);
		}
		restore(state, new FileLinearBuffer());
	}

	private RingBufferState readState() throws IOException {
		int channelSize = (int) fileChannel.size();
		if (channelSize == 0) {
			RingBufferState res = newState(requestedLimit);
			writeState(res);
			return res;
		} else {
			headerBuffer.position(0);
			fileChannel.read(headerBuffer, 0);
			headerBuffer.flip();
			return readHeader(headerBuffer.asIntBuffer());
		}
	}

	protected RingBufferState newState(int capacity) {
		return new RingBufferState(capacity);
	}

	@Override
	public OutputStream writer() throws IOException {
		if (writeBufferSize > 0)
			return new BufferedRingBufferOutputStream(() -> super.createWriter(), writeBufferSize);
		return super.createWriter();
	}

	@Override
	public InputStream reader() throws IOException {
		if (readerCache > 0)
			return new BufferedRingBufferInputStream(super.createReader(), readerCache);
		return super.createReader();
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
	public void close() throws IOException {
		super.close();
		fileChannel.close();
	}

	@Override
	protected LinearBuffer allocate(int capacity) throws IOException {
		throw new UnsupportedOperationException();
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

	/**
	 * Resize the current buffer to the closest capacity matching it's size, not
	 * lower to requested capacity.<br/>
	 * The linear buffer is transfered to be continuous from 0 to size
	 */
	private RingBufferState shrink(RingBufferState state) throws IOException {
		int required = Math.max(requestedLimit, RingBufferUtils.nextPowerOfTwo(state.size()));
		if (required < state.capacity()) {
			int position = state.position().position();
			if (state.size() > 0 && position > 0) {
				// we have some data to copy
				rewindBuffer(state);
			}
			fileChannel.truncate(headerLength() + required);
			return state.shrink(required);
		}
		return state;
	}

	private void transferWrappedTail(RingBufferState state) throws IOException {
		if (state.wrapped()) {
			int headerLength = headerLength();
			long size = fileChannel.size();
			int expectedSize = headerLength + state.capacity();
			if (size != expectedSize) {
				throw new IllegalStateException("Unexpected FileChannel size " + size + ", expecting " + expectedSize);
			}
			fileChannel.position(size);
			int remaining = state.writePosition();
			int transferred = 0;
			while (remaining > 0) {
				int t = (int) fileChannel.transferTo(headerLength + transferred, remaining, fileChannel);
				remaining -= t;
				transferred += t;
			}
		}
	}

	/**
	 * Transfer the linear buffer content to the start of the linear buffer.<br/>
	 * Use a temporary file to avoid losing data if something goes wrong.
	 */
	private void rewindBuffer(RingBufferState state) throws IOException {
		int position = state.position().position();
		Path tempFile = file.getParent().resolve(file.getFileName() + ".tmp");
		try (FileChannel tempFileChannel = FileChannel.open(tempFile,
				StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
				StandardOpenOption.READ, StandardOpenOption.WRITE)) {

			int headerLength = headerLength();
			// copy the header
			fileChannel.transferTo(0, headerLength, tempFileChannel);
			if (state.wrapped()) {
				// copy tail of linear buffer (start of ring buffer) to start of new linear
				// buffer
				fileChannel.transferTo(position + headerLength, state.capacity() - position, tempFileChannel);
				// copy start of linear buffer (end of ring buffer) after start of ring buffer
				fileChannel.transferTo(headerLength, state.writePosition(), tempFileChannel);
			} else {
				// copy linear buffer to start of new linear buffer
				fileChannel.transferTo(position + headerLength, state.size(), tempFileChannel);
			}
		}

		// close current file channel to delete move it
		fileChannel.close();

		// keep a back, simple rename is fast
		Path backupFile = Files.move(file, file.getParent().resolve(file.getFileName() + ".bak"),
				StandardCopyOption.REPLACE_EXISTING);

		// rename the new created file to buffer file name
		Files.move(tempFile, file);

		// reopen the file channel
		fileChannel = openChannel();

		// all good, delete the backup file
		Files.delete(backupFile);
	}

	public class FileLinearBuffer implements LinearBuffer {

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
			throw new UnsupportedOperationException();
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

		private int limit = 8 * 1024 * 1024;

		private int readerCacheSize = 8 * 1024;

		private int writeBufferSize = 4 * 1024;

		protected SyncMode syncMode = SyncMode.NONE;

		private long writeTimeout = 0;

		protected Builder() {}

		public Builder(File file) {
			this.file = file;
		}

		public Builder withFile(File file) {
			this.file = file;
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

		public Builder withWriteBufferSize(int writeBufferSize) {
			this.writeBufferSize = writeBufferSize;
			return this;
		}

		public Builder withSyncMode(SyncMode syncMode) {
			this.syncMode = syncMode;
			return this;
		}

		public Builder withWriteTimeout(long writeTimeout) {
			this.writeTimeout = writeTimeout;
			return this;
		}

		public FileRingBuffer build() throws IOException {
			return new FileRingBuffer(this);
		}
	}
}
