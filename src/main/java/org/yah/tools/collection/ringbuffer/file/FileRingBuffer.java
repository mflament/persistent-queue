package org.yah.tools.collection.ringbuffer.file;

import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.yah.tools.collection.ringbuffer.AbstractRingBuffer;
import org.yah.tools.collection.ringbuffer.BufferedRingBufferInputStream;
import org.yah.tools.collection.ringbuffer.LinearBuffer;
import org.yah.tools.collection.ringbuffer.RingBufferInputStream;
import org.yah.tools.collection.ringbuffer.RingBufferUtils;
import org.yah.tools.collection.ringbuffer.State;

public class FileRingBuffer extends AbstractRingBuffer {

	public static final int DEFAULT_CAPACITY = 128 * 1024;

	public static final int DEFAULT_READER_CACHE = 4 * 1024;

	private final int defaultReaderCache;

	private final FileChannel fileChannel;

	private int capacity;

	private final Header header;

	public FileRingBuffer(File file) throws IOException {
		this(file, DEFAULT_CAPACITY, -1, DEFAULT_READER_CACHE);
	}

	public FileRingBuffer(File file, int capacity) throws IOException {
		this(file, capacity, -1, DEFAULT_READER_CACHE);
	}

	public FileRingBuffer(File file, int capacity, int limit, int defaultReaderCache) throws IOException {
		super(capacity, limit);
		this.fileChannel = openChannel(file.toPath());
		this.capacity = RingBufferUtils.nextPowerOfTwo(capacity);
		this.defaultReaderCache = defaultReaderCache;

		this.header = readHeader();
		FileLinearBuffer buffer = new FileLinearBuffer();
		restoreBuffer(buffer, header.getStartPosition(), header.getSize());
	}

	protected Header newHeader() {
		return new Header();
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
	protected void onChange(State state) throws IOException {
		updateHeader(header, state);
	}

	protected void updateHeader(Header header, State state) throws IOException {
		header.setStartPosition(state.position());
		header.setSize(state.size());
		header.write(fileChannel);
	}

	@Override
	public void close() throws IOException {
		super.close();
		fileChannel.close();
	}

	@Override
	protected LinearBuffer allocate(int capacity) throws IOException {
		this.capacity = capacity;
		return linearBuffer;
	}

	private int headerLength() {
		return header.length();
	}

	private FileChannel openChannel(Path path) throws IOException {
		FileChannel channel = null;
		try {
			return FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.SYNC,
					StandardOpenOption.READ, StandardOpenOption.WRITE);
		} catch (IOException e) {
			closeQuietly(channel);
			throw e;
		}
	}

	private void closeQuietly(Closeable closeable) {
		if (closeable != null) {
			try {
				closeable.close();
			} catch (IOException e2) {}
		}
	}

	private Header readHeader() throws IOException {
		int channelSize = (int) fileChannel.size();
		Header hdr = newHeader();
		if (channelSize == 0) {
			hdr.write(fileChannel);
		} else {
			hdr.read(fileChannel);
			this.capacity = channelSize - hdr.length();
		}
		return hdr;
	}

	public class FileLinearBuffer implements LinearBuffer {

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
				if (last < 0) {
					System.out.println(String.format(
							"position: %d, file position: %d, target.length: %d, offset: %d, length: %d, read: %d, dst: %s, file.length: %d",
							position, headerLength() + position + read, target.length, offset, length, read, dst,
							fileChannel.size()));
					throw new EOFException();
				}
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
			if (target != this)
				throw new IllegalStateException("different target");

			if (position == targetPosition)
				return;

			fileChannel.position(headerLength() + targetPosition);
			fileChannel.transferTo(headerLength() + position, length, fileChannel);
		}

	}
}
