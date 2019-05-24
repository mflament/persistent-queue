package org.yah.tools.queue;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;

public interface ObjectQueue<E> extends Iterable<E>, Closeable {

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
	void write(Collection<E> elements) throws IOException;

}