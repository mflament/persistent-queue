package org.yah.tools.queue.impl;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.Objects;

import org.yah.tools.queue.ObjectQueue;
import org.yah.tools.queue.PollableObjectQueue;
import org.yah.tools.queue.impl.converters.StringObjectConverter;
import org.yah.tools.ringbuffer.impl.RingBufferState;
import org.yah.tools.ringbuffer.impl.RingBufferUtils;
import org.yah.tools.ringbuffer.impl.RingPosition;
import org.yah.tools.ringbuffer.impl.exceptions.RingBufferClosedException;
import org.yah.tools.ringbuffer.impl.file.FileRingBuffer;
import org.yah.tools.ringbuffer.impl.file.FileRingBuffer.SyncMode;

public final class PersistentObjectQueue<E> implements PollableObjectQueue<E> {

	private final ObjectFileRingBuffer<E> fileBuffer;

	private final ObjectConverter<E> converter;

	private final InputStream elementInputStream;

	private final CappedInputStream cappedInputStream;

	private SizedObject<E> lastElement;

	private boolean interrupted;

	private PersistentObjectQueue(Builder<E> builder)
			throws IOException {
		this.converter = builder.converter;
		this.fileBuffer = new ObjectFileRingBuffer<>(builder.fileBufferBuilder, converter);
		this.elementInputStream = fileBuffer.reader();
		this.cappedInputStream = new CappedInputStream(elementInputStream);
	}

	@Override
	public int size() {
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
	public E poll() throws IOException, InterruptedException {
		if (lastElement != null)
			return lastElement.getElement();

		try {
			int size = readInt(elementInputStream);
			E element = readElement(size);
			lastElement = new SizedObject<>(element, size);
			return element;
		} catch (InterruptedIOException | RingBufferClosedException e) {
			if (interrupted)
				throw new InterruptedException();
			throw e;
		}
	}

	@Override
	public void commit() throws IOException {
		if (lastElement != null) {
			fileBuffer.remove(Integer.BYTES + lastElement.getSize(), 1);
			lastElement = null;
		}
	}

	@Override
	public void clear() throws IOException {
		fileBuffer.remove(fileBuffer.size());
		lastElement = null;
	}

	@Override
	public void interrupt() {
		interrupted = true;
		RingBufferUtils.closeQuietly(elementInputStream);
	}

	void transferTo(ObjectQueue<E> target, int length) throws IOException {
		if (!(target instanceof PersistentObjectQueue))
			throw new IllegalArgumentException("Invalid target type " + target.getClass().getName());

		int currentLength = fileBuffer.state().elements();
		if (currentLength < length)
			throw new IllegalArgumentException("Buffer elements count " + currentLength + " is less that requested length " + length);
		
		PersistentObjectQueue<E> persistentTarget = (PersistentObjectQueue<E>) target;
		Collection<E> elements = new ArrayList<>(length);
		int totalSize = 0;
		while (elements.size() < length) {
			int size = readInt(elementInputStream);
			elements.add(readElement(size));
			totalSize += size + Integer.BYTES;
		}
		persistentTarget.offer(elements);
		fileBuffer.remove(totalSize, elements.size());
		lastElement = null;
	}

	@Override
	public QueueCursor<E> cursor() throws IOException {
		return new QueueIterator();
	}

	@Override
	public void offer(Collection<E> elements) throws IOException {
		try (OutputStream outputStream = fileBuffer.writer()) {
			for (E element : elements) {
				fileBuffer.writeElement(element, outputStream);
			}
		}
	}

	private E readElement(int elementSize) throws IOException {
		return readElement(cappedInputStream, elementSize);
	}

	private E readElement(CappedInputStream is, int elementSize) throws IOException {
		is.limit(elementSize);
		E element = converter.read(is);
		if (is.remaining() > 0)
			throw new IOException("remaining element data " + is.remaining());
		is.unlimit();
		return element;
	}

	private ObjectRingBufferState state() {
		return (ObjectRingBufferState) fileBuffer.state();
	}

	@Override
	public String toString() {
		return String.format("PersistentObjectQueue [fileBuffer=%s, elementInputStream=%s]", fileBuffer,
				elementInputStream);
	}

	public static Builder<String> builder() {
		return new Builder<>(StringObjectConverter.INSTANCE);
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

	private final class QueueIterator implements QueueCursor<E> {

		private final CappedInputStream is;

		private int elementsSize;

		private int elementsCount;

		public QueueIterator() throws IOException {
			is = new CappedInputStream(fileBuffer.reader());
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

		public RingBufferState remove(int length, int count) {
			return new ObjectRingBufferState(remove(length), elements - count);
		}

		public RingBufferState add(int length, int count) {
			return new ObjectRingBufferState(incrementSize(length), elements + count);
		}

	}

	static class ObjectFileRingBuffer<E> extends FileRingBuffer {

		private static final int HEADER_LENGTH = 4 * Integer.BYTES;

		private final ElementBuffer<E> elementBuffer;

		private int pendingElements;

		public ObjectFileRingBuffer(Builder builder, ObjectConverter<E> converter) throws IOException {
			super(builder);
			this.elementBuffer = new ElementBuffer<>(converter);
		}

		@Override
		public ObjectRingBufferState state() {
			return (ObjectRingBufferState) super.state();
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

		public void writeElement(E element, OutputStream outputStream) throws IOException {
			elementBuffer.write(element, outputStream);
			pendingElements++;
		}

		@Override
		protected void flushWriter() throws IOException {
			if (pendingElements > 0) {
				updateState(s -> ((ObjectRingBufferState) s).add(pendingWrite(), pendingElements));
				pendingElements = 0;
				pendingWrite = 0;
			}
		}

		@Override
		protected int headerLength() {
			return HEADER_LENGTH;
		}

	}

	private static class ElementBuffer<E> extends ByteArrayOutputStream {

		private final ObjectConverter<E> converter;

		public ElementBuffer(ObjectConverter<E> converter) {
			super(1024);
			this.converter = converter;
		}

		public final void write(E element, OutputStream target) throws IOException {
			converter.write(element, this);
			int elementSize = size();
			writeInt(elementSize, target);
			target.write(buf, 0, elementSize);
			reset();
		}

		private void writeInt(int i, OutputStream os) throws IOException {
			os.write((i >> 24) & 0xFF);
			os.write((byte) (i >> 16) & 0xFF);
			os.write((byte) (i >> 8) & 0xFF);
			os.write((byte) i & 0xFF);
		}
	}

	public static final class Builder<E> {

		private final ObjectConverter<E> converter;

		private FileRingBuffer.Builder fileBufferBuilder = FileRingBuffer.builder();

		private Builder(ObjectConverter<E> converter) {
			this.converter = Objects.requireNonNull(converter, "converter is null");
		}

		public Builder<E> withFile(File file) {
			fileBufferBuilder = fileBufferBuilder.withFile(file);
			return this;
		}

		public Builder<E> withLimit(int limit) {
			fileBufferBuilder = fileBufferBuilder.withLimit(limit);
			return this;
		}

		public Builder<E> withDefaultReaderCache(int defaultReaderCache) {
			fileBufferBuilder = fileBufferBuilder.withDefaultReaderCache(defaultReaderCache);
			return this;
		}

		public Builder<E> withSyncMode(SyncMode syncMode) {
			fileBufferBuilder = fileBufferBuilder.withSyncMode(syncMode);
			return this;
		}

		public Builder<E> withWriteBufferSize(int writeBufferSize) {
			fileBufferBuilder = fileBufferBuilder.withWriteBufferSize(writeBufferSize);
			return this;
		}

		public Builder<E> withWriteTimeout(long writeTimeout) {
			fileBufferBuilder = fileBufferBuilder.withWriteTimeout(writeTimeout);
			return this;
		}

		public PersistentObjectQueue<E> build() throws IOException {
			return new PersistentObjectQueue<>(this);
		}
	}

}
