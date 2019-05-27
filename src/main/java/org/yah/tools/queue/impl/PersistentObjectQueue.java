package org.yah.tools.queue.impl;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

import org.yah.tools.queue.ObjectQueue;
import org.yah.tools.queue.impl.converters.SerializableConverter;
import org.yah.tools.ringbuffer.impl.RingBufferInputStream;
import org.yah.tools.ringbuffer.impl.RingBufferState;
import org.yah.tools.ringbuffer.impl.RingPosition;
import org.yah.tools.ringbuffer.impl.file.FileRingBuffer;
import org.yah.tools.ringbuffer.impl.file.FileRingBuffer.SyncMode;

public final class PersistentObjectQueue<E> implements ObjectQueue<E> {

	private final ObjectFileRingBuffer fileBuffer;

	private final ObjectConverter<E> converter;

	private final InputStream elementInputStream;

	private final CappedInputStream cappedInputStream;

	private final int writeBufferSize;

	private final boolean autoCommit;

	private int lastElementSize;

	private PersistentObjectQueue(Builder<E> builder)
			throws IOException {
		this.fileBuffer = new ObjectFileRingBuffer(builder.fileBufferBuilder);
		this.converter = builder.converter;
		this.writeBufferSize = builder.writeBufferSize;
		this.elementInputStream = fileBuffer.reader();
		this.cappedInputStream = new CappedInputStream(elementInputStream);
		this.autoCommit = builder.autoCommit;
	}

	@Override
	public int elementsCount() {
		return state().elements();
	}

	@Override
	public void close() throws IOException {
		elementInputStream.close();
		fileBuffer.close();
	}

	/**
	 * remove the last polled element if any, and read the next element, blocking if
	 * empty
	 * 
	 * @throws IOException
	 */
	@Override
	public E poll() throws IOException {
		if (autoCommit)
			commit();
		lastElementSize = readInt(elementInputStream);
		return readElement(cappedInputStream, lastElementSize);
	}

	@Override
	public void commit() throws IOException {
		if (lastElementSize > 0) {
			fileBuffer.remove(Integer.BYTES + lastElementSize, 1);
			lastElementSize = 0;
		}
	}

	@Override
	public void transferTo(ObjectQueue<E> target, int length) throws IOException {
		if (!(target instanceof PersistentObjectQueue))
			throw new IllegalArgumentException("Invalid target type " + target.getClass().getName());

		PersistentObjectQueue<E> persistentTarget = (PersistentObjectQueue<E>) target;
		Collection<E> elements = new ArrayList<>(length);
		int bufferSize;
		try (QueueIterator iterator = new QueueIterator()) {
			while (iterator.hasNext() && elements.size() < length) {
				elements.add(iterator.next());
			}
			bufferSize = iterator.bufferSize();
		}
		persistentTarget.offer(elements);

		fileBuffer.remove(bufferSize, elements.size());
	}

	@Override
	public QueueCursor<E> cursor() {
		return new QueueIterator();
	}

	@Override
	public void offer(Iterator<E> elements) throws IOException {
		try (WriteBuffer<E> writeBuffer = new WriteBuffer<>(converter, writeBufferSize)) {
			elements.forEach(writeBuffer::write);
			fileBuffer.write(writeBuffer);
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

	public void shrink() throws IOException {
		fileBuffer.shrink();
	}

	private ObjectRingBufferState state() {
		return (ObjectRingBufferState) fileBuffer.state();
	}

	@Override
	public String toString() {
		return String.format("PersistentObjectQueue [fileBuffer=%s, elementInputStream=%s]", fileBuffer,
				elementInputStream);
	}

	public static <E extends Serializable> Builder<E> builder() {
		return new Builder<>(SerializableConverter.instance());
	}

	public static <E> Builder<E> builder(ObjectConverter<E> elementConverter) {
		return new Builder<>(elementConverter);
	}

	private static int readInt(InputStream is) throws IOException {
		int res = safeRead(is) << 24;
		res |= safeRead(is) << 16;
		res |= safeRead(is) << 8;
		res |= safeRead(is);
		return res;
	}

	private static int safeRead(InputStream is) throws IOException {
		int res = is.read();
		if (res == -1)
			throw new EOFException();
		return res;
	}

	private static class WriteBuffer<E> extends ByteArrayOutputStream {

		private final ObjectConverter<E> converter;

		private int elementCount;

		public WriteBuffer(ObjectConverter<E> converter, int size) {
			super(size);
			this.converter = converter;
		}

		public byte[] buffer() {
			return buf;
		}

		public final int write(E element) throws IOException {
			skip(Integer.BYTES);
			int position = size();
			converter.write(element, this);
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

		@Override
		public synchronized void reset() {
			super.reset();
			elementCount = 0;
		}

	}

	private final class QueueIterator implements QueueCursor<E> {

		private final CappedInputStream is;

		private int elementsSize;

		private int elementsCount;

		public QueueIterator() {
			RingBufferInputStream reader = fileBuffer.reader();
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
				elementsSize += size;
				elementsCount++;
				return readElement(is, size);
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		}

		public int bufferSize() {
			return elementsSize + elementsCount * Integer.BYTES;
		}
	}

	private static class ObjectRingBufferState extends RingBufferState {

		private final int elements;

		public ObjectRingBufferState(int position, long cycle, int capacity, int size, int elements) {
			super(position, cycle, capacity, size);
			this.elements = elements;
		}

		public ObjectRingBufferState(RingPosition position, int size, int elements) {
			super(position, size);
			this.elements = elements;
		}

		public ObjectRingBufferState(RingBufferState rbs, int elements) {
			super(rbs);
			this.elements = elements;
		}

		public int elements() {
			return elements;
		}

		@Override
		protected RingBufferState newState(int position, long cycle, int capacity, int size) {
			return new ObjectRingBufferState(position, cycle, capacity, size, elements);
		}

		protected final RingBufferState incrementSize(int length, int count) {
			return new ObjectRingBufferState(position,
					size() + length,
					elements + count);
		}

		public RingBufferState remove(int length, int count) {
			return new ObjectRingBufferState(remove(length),
					elements - count);
		}

	}

	private static class ObjectFileRingBuffer extends FileRingBuffer {

		private static final int HEADER_LENGTH = 4 * Integer.BYTES;

		public ObjectFileRingBuffer(Builder builder) throws IOException {
			super(builder);
		}

		public void write(WriteBuffer<?> writeBuffer) throws IOException {
			super.write(writeBuffer.buffer(), 0, writeBuffer.size(),
					s -> incrementSize(s, writeBuffer.size(), writeBuffer.elementCount));
		}

		public synchronized int remove(int length, int count) throws IOException {
			int removable = Math.min(size(), length);
			if (removable < length)
				throw new IOException("Missing bytes to remove: requested " + length + ", removable: " + removable);
			if (removable > 0)
				updateState(s -> remove(s, removable, count));
			return removable;
		}

		private RingBufferState remove(RingBufferState state, int length, int count) {
			ObjectRingBufferState orbs = (ObjectRingBufferState) state;
			return orbs.remove(length, count);
		}

		private RingBufferState incrementSize(RingBufferState state, int length, int count) {
			ObjectRingBufferState orbs = (ObjectRingBufferState) state;
			return orbs.incrementSize(length, count);
		}

		@Override
		protected RingBufferState newState(int capacity) {
			return new ObjectRingBufferState(0, 0, capacity, 0, 0);
		}

		@Override
		protected RingBufferState readHeader(IntBuffer intBuffer) throws IOException {
			int pos = intBuffer.get();
			int size = intBuffer.get();
			int capacity = intBuffer.get();
			int elements = intBuffer.get();
			return new ObjectRingBufferState(new RingPosition(pos, 0, capacity), size, elements);
		}

		@Override
		protected void writeHeader(RingBufferState state, IntBuffer intBuffer) {
			super.writeHeader(state, intBuffer);
			ObjectRingBufferState os = (ObjectRingBufferState) state;
			intBuffer.put(os.elements);
		}

		@Override
		protected int headerLength() {
			return HEADER_LENGTH;
		}

	}

	public static final class Builder<E> {

		private final ObjectConverter<E> converter;

		private int writeBufferSize = 8 * 1024;

		private FileRingBuffer.Builder fileBufferBuilder = FileRingBuffer.builder();

		private boolean autoCommit;

		private Builder(ObjectConverter<E> converter) {
			this.converter = Objects.requireNonNull(converter, "converter is null");
		}

		public Builder<E> withFile(File file) {
			fileBufferBuilder.withFile(file);
			return this;
		}

		public Builder<E> withCapacity(int capacity) {
			fileBufferBuilder.withCapacity(capacity);
			return this;
		}

		public Builder<E> withLimit(int limit) {
			fileBufferBuilder.withLimit(limit);
			return this;
		}

		public Builder<E> withDefaultReaderCache(int defaultReaderCache) {
			fileBufferBuilder.withDefaultReaderCache(defaultReaderCache);
			return this;
		}

		public Builder<E> withSyncMode(SyncMode syncMode) {
			fileBufferBuilder.withSyncMode(syncMode);
			return this;
		}

		public Builder<E> withWriteBufferSize(int writeBufferSize) {
			this.writeBufferSize = writeBufferSize;
			return this;
		}

		public Builder<E> autoCommit() {
			this.autoCommit = true;
			return this;
		}

		public PersistentObjectQueue<E> build() throws IOException {
			return new PersistentObjectQueue<>(this);
		}
	}

}
