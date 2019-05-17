/**
 * 
 */
package org.yah.tools.collection.queue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.BufferUnderflowException;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.Consumer;

import org.yah.tools.collection.ringbuffer.AbstractRingBuffer.State;
import org.yah.tools.collection.ringbuffer.FileRingBuffer;

/**
 * @author Yah
 * @created 2019/05/10
 */
public class PersistentQueue<E> implements SimpleQueue<E> {

	private static final int BUFFERS_INITIAL_CAPACITY = 4 * 1024;

	private final FileRingBuffer ringBufer;

	private final ElementReader<E> elementReader;

	private SizedElement<E> head;

	private final ReadBuffer readBuffer;

	private final DirectByteArrayOutputStream writeBuffer;

	public PersistentQueue(ElementReader<E> elementReader, File file) {
		this.elementReader = elementReader;
		ringBufer = new FileRingBuffer(file);
		readBuffer = new ReadBuffer(BUFFERS_INITIAL_CAPACITY);
		writeBuffer = new DirectByteArrayOutputStream(BUFFERS_INITIAL_CAPACITY);
		head = readBuffer.readElement(0);
	}

	@Override
	public Iterator<E> iterator() {
		return new PersistentQueueIterator();
	}

	@Override
	public void offer(Collection<E> elements) {
		if (elements.isEmpty())
			return;
		synchronized (writeBuffer) {
			writeBuffer.reset();
			Iterator<E> iterator = elements.iterator();
			E newHead = iterator.next();
			int newHeadSize = writeBuffer.writeElement(newHead);
			iterator.forEachRemaining(writeBuffer::writeElement);
			ringBufer.write(writeBuffer.getBuffer(), 0, writeBuffer.size());
			setHead(new SizedElement<>(newHead, newHeadSize));
		}
	}

	@Override
	public void consume(Consumer<E> consumer) throws InterruptedException {
		SizedElement<E> e = peekHead();
		consumer.accept(e.element);
		ringBufer.remove(Integer.SIZE + e.size);
		nextHead();
	}

	private synchronized SizedElement<E> peekHead() throws InterruptedException {
		while (head == null) {
			wait();
		}
		return head;
	}

	private synchronized void setHead(SizedElement<E> newHead) {
		if (head == null) {
			head = newHead;
			notifyAll();
		}
	}

	private synchronized void nextHead() {
		if (ringBufer.size() > 0) {
			head = readBuffer.readElement(0);
		} else {
			head = null;
		}
	}

	@Override
	public int size() {
		return ringBufer.size();
	}

	private class ReadBuffer extends InputStream {

		private byte[] buffer;

		private int readPosition;

		private int size;

		public ReadBuffer(int capacity) {
			buffer = new byte[capacity];
		}

		@Override
		public int read() throws IOException {
			if (readPosition < size)
				return buffer[readPosition++];
			return -1;
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			int remaining = size - readPosition;
			if (remaining == 0)
				return 0;
			len = Math.min(remaining, len);
			System.arraycopy(buffer, readPosition, b, off, len);
			readPosition += len;
			return len;
		}

		private void fill(int position, int length) {
			int remaining = ringBufer.size() - position;
			if (remaining < 0)
				throw new BufferUnderflowException();
			if (buffer.length < length)
				buffer = new byte[(int) (length * 1.5f)];
			size = ringBufer.read(position, buffer, 0, length);
			readPosition = 0;
		}

		public SizedElement<E> readElement(int position) {
			if (ringBufer.size() == 0)
				return null;

			try {
				int size = readInt(position);
				fill(position + Integer.SIZE, size);
				return new SizedElement<>(elementReader.read(this), size);
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		}

		private int readInt(int position) throws IOException {
			fill(position, Integer.SIZE);
			return Byte.toUnsignedInt(buffer[0]) << 24 |
					Byte.toUnsignedInt(buffer[1]) << 16 |
					Byte.toUnsignedInt(buffer[2]) << 8 |
					Byte.toUnsignedInt(buffer[3]);
		}
	}

	private class DirectByteArrayOutputStream extends ByteArrayOutputStream {

		public DirectByteArrayOutputStream() {
			super();
		}

		public DirectByteArrayOutputStream(int size) {
			super(size);
		}

		public final byte[] getBuffer() {
			return buf;
		}

		public final int writeElement(E element) {
			skip(Integer.SIZE);
			int position = size();
			try {
				elementReader.write(element, this);
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
			int elementSize = size() - position;
			writeInt(position - Integer.SIZE, elementSize);
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

	private final class PersistentQueueIterator implements Iterator<E> {

		private final ReadBuffer readBuffer;

		private State ringState;

		private int position;

		public PersistentQueueIterator() {
			readBuffer = new ReadBuffer(BUFFERS_INITIAL_CAPACITY);
			ringState = ringBufer.getState();
		}

		@Override
		public boolean hasNext() {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public E next() {
			// TODO Auto-generated method stub
			return null;
		}

	}
}
