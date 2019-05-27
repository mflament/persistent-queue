package org.yah.tools.queue;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import org.yah.tools.queue.impl.QueueCursor;

public interface ObjectQueue<E> extends Closeable {

	/**
	 * number of elements in buffer
	 */
	int elementsCount();

	/**
	 * remove the last polled element if any, and read the next element, blocking if
	 * empty
	 * 
	 * @throws IOException
	 */
	E poll() throws IOException;

	/**
	 * Remove the first element
	 */
	void commit() throws IOException;

	/**
	 * Write the element to the end of the queue.
	 */
	void offer(Iterator<E> elements) throws IOException;

	default void offer(Collection<E> elements) throws IOException {
		offer(elements.iterator());
	}

	default void offer(E element) throws IOException {
		offer(Collections.singleton(element).iterator());
	}

	void transferTo(ObjectQueue<E> target, int length) throws IOException;

	QueueCursor<E> cursor();
}