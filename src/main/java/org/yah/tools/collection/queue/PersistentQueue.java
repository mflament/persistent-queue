/**
 * 
 */
package org.yah.tools.collection.queue;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Consumer;

import org.yah.tools.collection.ringbuffer.State;
import org.yah.tools.collection.ringbuffer.file.FileRingBuffer;
import org.yah.tools.collection.ringbuffer.file.Header;

/**
 * @author Yah
 * @created 2019/05/10
 */
public class PersistentQueue<E> implements Closeable {

	private static final class PersistentQueueRingBuffer extends FileRingBuffer {

		public PersistentQueueRingBuffer(File file, int capacity, int limit, int defaultReaderCache)
				throws IOException {
			super(file, capacity, limit, defaultReaderCache);
		}

		public PersistentQueueRingBuffer(File file, int capacity) throws IOException {
			super(file, capacity);
		}

		public PersistentQueueRingBuffer(File file) throws IOException {
			super(file);
		}

		@Override
		protected Header newHeader() {
			return new PersitentQueueHeader();
		}

		@Override
		protected void updateHeader(Header header, State state) throws IOException {
			((PersitentQueueHeader)header).setElementCount(();
			super.updateHeader(header, state);
		}

		public void writeElements(byte[] source, int offset, int length, int elementCount) throws IOException {
			writer().write(b);
		}

	}

	private static final int DEFAULT_WRITE_BIFFER_SIZE = 1024;

	private final File file;

	private final PersistentQueueRingBuffer ringBufer;

	private final ElementReader<E> elementReader;

	private final InputStream liveStream;

	public PersistentQueue(ElementReader<E> elementReader, File file) throws IOException {
		this.elementReader = Objects.requireNonNull(elementReader, "elementReader is null");
		this.file = Objects.requireNonNull(file, "file is null");
		this.ringBufer = new PersistentQueueRingBuffer(file);
		liveStream = ringBufer.reader();
	}

	@Override
	public void close() throws IOException {
		ringBufer.close();
	}

	public Iterator<E> iterator() {
		return new QueueIterator();
	}

	public void offer(Collection<E> elements) {
		if (elements.isEmpty())
			return;

		WriteBuffer<E> writeBuffer = newWriteBuffer();
		elements.forEach(writeBuffer::writeElement);
		try {
			ringBufer.writeElements(writeBuffer.buffer(), 0, writeBuffer.size(), elements.size());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		OutputStream ringWriter = ringBufer.writer();
		synchronized (ringWriter) {
			writeBuffer.flush(ringWriter);
		}
	}

	public void consume(Consumer<E> consumer) throws InterruptedException {
		SizedElement<E> head = readElement(liveStream);
		consumer.accept(head.element);
		try {
			ringBufer.remove(Integer.BYTES + head.size);
		} catch (IOException ex) {
			throw new UncheckedIOException(ex);
		}
	}

	public int size() {
		return ringBufer.size();
	}

	private WriteBuffer<E> newWriteBuffer() {
		return newWriteBuffer(DEFAULT_WRITE_BIFFER_SIZE);
	}

	private WriteBuffer<E> newWriteBuffer(int size) {
		return new WriteBuffer<>(elementReader, size);
	}

	private SizedElement<E> readElement(InputStream is) {
		try {
			int size = readInt(is);
			E element = elementReader.read(is);
			return new SizedElement<E>(element, size);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	private int readInt(InputStream is) throws IOException {
		int res = is.read() << 24;
		res |= is.read() << 16;
		res |= is.read() << 8;
		res |= is.read();
		return res;
	}

	private static class WriteBuffer<E> extends ByteArrayOutputStream {

		private final ElementReader<E> elementReader;

		private int elementCount;

		public WriteBuffer(ElementReader<E> elementReader, int size) {
			super(size);
			this.elementReader = elementReader;
		}

		public byte[] buffer() {
			return buf;
		}

		@Override
		public synchronized void reset() {
			super.reset();
			elementCount = 0;
		}

		public void flush(OutputStream writer) {
			try {
				writer.write(buf, 0, size());
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
			reset();
		}

		public final int writeElement(E element) {
			skip(Integer.BYTES);
			int position = size();
			try {
				elementReader.write(element, this);
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

	private final static class SizedElement<E> {
		private final E element;
		private final int size;

		private SizedElement(E element, int size) {
			super();
			this.element = element;
			this.size = size;
		}
	}

	private final class QueueIterator implements Iterator<E> {

		private final InputStream is;

		public QueueIterator() {
			is = ringBufer.reader();
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
			return readElement(is).element;
		}
	}

}
