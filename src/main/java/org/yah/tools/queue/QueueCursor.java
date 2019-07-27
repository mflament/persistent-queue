package org.yah.tools.queue;

import java.io.Closeable;

public interface QueueCursor<E> extends Closeable {
	
	boolean hasNext();

	E next();
}
