package org.yah.tools.collection.queue;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

import org.yah.tools.collection.ringbuffer.RingBuffer;

public class SimpleQueueImpl<E> implements SimpleQueue<E> {
	
	public interface ElementReader<E> {

		E read(byte[] buffer, int offset, int length);

		void write(E element, byte[] buffer, int offset);

	}

	private final RingBuffer ringBufer;

	private final BlockingQueue<E> elementBuffer;

	public SimpleQueueImpl(RingBuffer ringBuffer) {
		this.ringBufer = Objects.requireNonNull(ringBuffer, "ringBuffer is null");
		this.elementBuffer = new LinkedBlockingQueue<>();
		
	}

	@Override
	public Iterator<E> iterator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void offer(Collection<E> elements) {
		synchronized (writerMonitor) {
			
		}
	}

	@Override
	public void consume(Consumer<E> consumer) {
		// TODO Auto-generated method stub

	}

	@Override
	public int size() {
		// TODO Auto-generated method stub
		return 0;
	}

}
