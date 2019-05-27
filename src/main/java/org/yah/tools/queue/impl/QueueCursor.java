package org.yah.tools.queue.impl;

import java.io.Closeable;

public interface QueueCursor<E> extends Closeable {
	
	boolean hasNext();

	E next();
}
