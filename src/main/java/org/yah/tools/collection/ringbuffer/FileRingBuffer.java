package org.yah.tools.collection.ringbuffer;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

import org.yah.tools.collection.ringbuffer.AbstractRingBuffer.LinearBuffer;
import org.yah.tools.collection.ringbuffer.FileRingBuffer.FileLinearBuffer;

public class FileRingBuffer extends AbstractRingBuffer<FileLinearBuffer> implements AutoCloseable {

	private static final int VERSION = 1;

	private static final int HEADER_LENGTH = 4 + 4 + 4;

	private static final int DEFAULT_CAPACITY = 128 * 1024;

	public FileRingBuffer(File file) {
		this(file, DEFAULT_CAPACITY);
	}

	public FileRingBuffer(File file, int capacity) {
		setLinearBuffer(new FileLinearBuffer(file, capacity));
	}

	@Override
	protected void onChange(int startPosition, int size) {
		linearBuffer().updateHeader(startPosition, size);
	}

	@Override
	public void close() {
		try {
			linearBuffer().close();
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	@Override
	protected FileLinearBuffer allocate(int capacity) {
		return linearBuffer().allocate(capacity);
	}

	public static class Header {

		private final ByteBuffer buffer = ByteBuffer.allocate(HEADER_LENGTH);

		public Header() {
			setVersion(VERSION);
		}

		public int getVersion() {
			return buffer.getInt(0);
		}

		public void setVersion(int version) {
			buffer.putInt(0, version);
		}

		public int getStartPosition() {
			return buffer.getInt(1);
		}

		public void setStartPosition(int startPosition) {
			buffer.putInt(1, startPosition);
		}

		public int getSize() {
			return buffer.getInt(2);
		}

		public void setSize(int size) {
			buffer.putInt(2, size);
		}

		public void read(RandomAccessFile file) throws IOException {
			file.seek(0);
			file.readFully(buffer.array());
		}

		public void write(RandomAccessFile file) throws IOException {
			file.seek(0);
			file.write(buffer.array());
		}

	}

	public class FileLinearBuffer implements LinearBuffer, Closeable {

		private final RandomAccessFile file;

		private final Header header = new Header();

		public FileLinearBuffer(File file, int capacity) {
			RandomAccessFile raf = null;
			try {
				raf = new RandomAccessFile(file, "rwd");
				if (raf.length() == 0) {
					header.write(raf);
					raf.setLength(HEADER_LENGTH + capacity);
				} else {
					header.read(raf);
					if (header.getVersion() != VERSION) {
						throw new IllegalArgumentException("Unsupported version " + header.getVersion() + " in "
								+ file);
					}
					restore(header.getStartPosition(), header.getSize());
				}
			} catch (IOException e) {
				if (raf != null) {
					try {
						raf.close();
					} catch (IOException e2) {}
				}
				throw new UncheckedIOException(e);
			}
			this.file = raf;
		}

		public FileLinearBuffer allocate(int capacity) {
			try {
				file.setLength(capacity + HEADER_LENGTH);
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
			return this;
		}

		@Override
		public void close() throws IOException {
			file.close();
		}

		@Override
		public int capacity() {
			try {
				return (int) file.length() - HEADER_LENGTH;
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		}

		@Override
		public void read(int position, byte[] target, int offset, int length) {
			try {
				file.seek(HEADER_LENGTH + position);
				file.readFully(target, offset, length);
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		}

		@Override
		public void write(int position, byte[] source, int offset, int length) {
			try {
				file.seek(HEADER_LENGTH + position);
				file.write(source, offset, length);
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		}

		@Override
		public void copyTo(LinearBuffer target, int position, int targetPosition, int length) {
			RandomAccessFile targetFile = ((FileLinearBuffer) target).file;
			byte[] buffer = new byte[length];
			read(position, buffer, 0, buffer.length);

			try {
				targetFile.seek(HEADER_LENGTH + targetPosition);
				targetFile.write(buffer, 0, buffer.length);
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		}

		private void updateHeader(int startPosition, int size) {
			header.setStartPosition(startPosition);
			header.setSize(size);
			try {
				header.write(file);
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		}

	}
}
