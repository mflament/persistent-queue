package org.yah.tools.queue;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Consumer;

public interface ObjectQueue<E> extends Closeable {

	/**
	 * number of elements in buffer
	 */
	int size();

	default boolean isEmpty() {
		return size() == 0;
	}

	void offer(Collection<E> elements) throws IOException;

	default void offer(E element) throws IOException {
		offer(Collections.singleton(element));
	}

	QueueCursor<E> cursor() throws IOException;

	default void forEach(Consumer<E> consumer) throws IOException {
		try (QueueCursor<E> cursor = cursor()) {
			while (cursor.hasNext()) {
				consumer.accept(cursor.next());
			}
		}
	}
}