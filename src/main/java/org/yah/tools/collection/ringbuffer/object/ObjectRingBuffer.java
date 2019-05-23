package org.yah.tools.collection.ringbuffer.object;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.nio.IntBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

import org.yah.tools.collection.ringbuffer.RingBufferInputStream;
import org.yah.tools.collection.ringbuffer.RingBufferState;
import org.yah.tools.collection.ringbuffer.file.FileRingBuffer;
import org.yah.tools.collection.ringbuffer.file.FileRingBuffer.Builder;
import org.yah.tools.collection.ringbuffer.object.converters.SerializableConverter;

public final class ObjectRingBuffer<E> implements Iterable<E> {

	private final FileRingBuffer fileBuffer;

	private final ObjectConverter<E> converter;

	private final InputStream elementInputStream;

	private final CappedInputStream cappedInputStream;

	private final int writeBufferSize;

	private int lastElementSize;

	private ObjectRingBuffer(Builder<E> builder)
			throws IOException {
		this.fileBuffer = builder.fileBuffer;
		this.converter = builder.converter;
		this.writeBufferSize = builder.writeBufferSize;
		this.elementInputStream = fileBuffer.reader();
		this.cappedInputStream = new CappedInputStream(elementInputStream);
	}

	public int elementsCount() {
		return state().elements();
	}

	@Override
	public ObjectRingBufferState state() {
		return (ObjectRingBufferState) super.state();
	}

	@Override
	protected RingBufferState newState(int capacity) {
		return new ObjectRingBufferState(0, 0, capacity, 0, 0);
	}

	@Override
	protected RingBufferState readHeader(IntBuffer intBuffer) throws IOException {
		RingBufferState rbs = super.readHeader(intBuffer);
		int elements = intBuffer.get();
		return new ObjectRingBufferState(rbs, elements);
	}

	@Override
	protected void writeHeader(RingBufferState state, IntBuffer intBuffer) throws IOException {
		super.write(state, intBuffer);
		intBuffer.put(((ObjectRingBufferState) state).elements());
	}

	@Override
	protected int headerLength() {
		return super.headerLength() + Integer.BYTES;
	}

	@Override
	public void close() throws IOException {
		elementInputStream.close();
		super.close();
	}

	/**
	 * remove the last polled element if any, and read the next element, blocking if
	 * empty
	 * 
	 * @throws IOException
	 */
	public E poll() throws IOException {
		commit();
		lastElementSize = readInt(elementInputStream);
		return readElement(cappedInputStream, lastElementSize);
	}

	public void commit() throws IOException {
		if (lastElementSize > 0) {
			remove(Integer.BYTES + lastElementSize);
			lastElementSize = 0;
		}
	}

	@Override
	public Iterator<E> iterator() {
		return new QueueIterator();
	}

	public void write(Collection<E> elements) throws IOException {
		try (WriteBuffer writeBuffer = new WriteBuffer(writeBufferSize)) {
			elements.forEach(writeBuffer::write);
			write(writeBuffer.buffer(), 0, writeBuffer.size(),
					s -> incrementSize(s, writeBuffer.size(), elements.size()));
		} catch (UncheckedIOException e) {
			throw e.getCause();
		}
	}

	private E readElement(CappedInputStream is, int elementSize) throws IOException {
		is.limit(elementSize);
		E element = converter.read(is);
		if (is.remaining() > 0)
			throw new IOException("remaining element data " + is.remaining());
		is.unlimit();
		return element;
	}

	private RingBufferState incrementSize(RingBufferState state, int length, int count) {
		return ((ObjectRingBufferState) state()).incrementSize(length, count);
	}

	private int readInt(InputStream is) throws IOException {
		int res = is.read() << 24;
		res |= is.read() << 16;
		res |= is.read() << 8;
		res |= is.read();
		return res;
	}

	public static <E extends Serializable> Builder<E> builder() {
		return new Builder<>(SerializableConverter.instance());
	}

	public static <E> Builder<E> builder(ObjectConverter<E> elementConverter) {
		return new Builder<>(elementConverter);
	}

	private class WriteBuffer extends ByteArrayOutputStream {

		private int elementCount;

		public WriteBuffer(int size) {
			super(size);
		}

		public byte[] buffer() {
			return buf;
		}

		public final int write(E element) {
			skip(Integer.BYTES);
			int position = size();
			try {
				converter.write(element, this);
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
			int elementSize = size() - position;
			writeInt(position - Integer.BYTES, elementSize);
			elementCount++;
			return elementSize;
		}

		private void writeInt(int index, int i) {
			buf[index] = (byte) (i >> 24);
			buf[index + 1] = (byte) (i >> 16);
			buf[index + 2] = (byte) (i >> 8);
			buf[index + 3] = (byte) i;
		}

		private void skip(int n) {
			for (int i = 0; i < n; i++)
				write(0);
		}

	}

	private final class QueueIterator implements Iterator<E>, Closeable {

		private final CappedInputStream is;

		public QueueIterator() {
			RingBufferInputStream reader = reader();
			is = new CappedInputStream(reader);
		}

		@Override
		public void close() throws IOException {
			is.close();
		}

		@Override
		public boolean hasNext() {
			try {
				return is.available() > 0;
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		}

		@Override
		public E next() {
			if (!hasNext())
				throw new NoSuchElementException();
			try {
				int size = readInt(is);
				return readElement(is, size);
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		}
	}

	public static final class Builder<E> extends FileRingBuffer.Builder {

		public FileRingBuffer fileBuffer;

		private final ObjectConverter<E> converter;

		private int writeBufferSize = 8 * 1024;

		private Builder(ObjectConverter<E> converter) {
			this.converter = Objects.requireNonNull(converter, "converter is null");
		}

		@Override
		public Builder<E> withFile(File file) {
			// TODO Auto-generated method stub
			return super.withFile(file);
		}

		@Override
		public Builder<E> withCapacity(int capacity) {
			// TODO Auto-generated method stub
			return super.withCapacity(capacity);
		}

		@Override
		public Builder<E> withLimit(int limit) {
			// TODO Auto-generated method stub
			return super.withLimit(limit);
		}

		@Override
		public Builder<E> withDefaultReaderCache(int defaultReaderCache) {
			// TODO Auto-generated method stub
			return super.withDefaultReaderCache(defaultReaderCache);
		}

		@Override
		public Builder<E> sync() {
			super.sync();
			return this;
		}

		public Builder<E> withWriteBufferSize(int writeBufferSize) {
			this.writeBufferSize = writeBufferSize;
			return this;
		}

		@Override
		public ObjectRingBuffer<E> build() throws IOException {
			return new ObjectRingBuffer<>(this);
		}
	}
}
