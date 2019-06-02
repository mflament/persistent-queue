package org.yah.tools.queue;

import java.io.IOException;

public interface PollableObjectQueue<E> extends ObjectQueue<E> {

	/**
	 * Read the last uncommited poller element, or read the next element, blocking
	 * if necessary
	 * 
	 * @throws IOException
	 * @throws {@link InterruptedException}
	 */
	E poll() throws IOException, InterruptedException;

	/**
	 * Remove the last polled element
	 */
	void commit() throws IOException;

	/**
	 * remove all elements
	 * 
	 * @throws IOException
	 */
	void clear() throws IOException;

	/**
	 * Interrupt poller threads.
	 */
	void interrupt();

}
