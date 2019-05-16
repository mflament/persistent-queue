package org.yah.tools.collection.queue;

import java.util.Collection;
import java.util.Collections;
import java.util.function.Consumer;

public interface SimpleQueue<E> extends Iterable<E> {

	void offer(Collection<E> elements);

	default void offer(E element) {
		offer(Collections.singleton(element));
	}

	void consume(Consumer<E> consumer) throws InterruptedException;

	int size();

}
