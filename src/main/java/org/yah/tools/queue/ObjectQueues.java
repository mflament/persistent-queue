package org.yah.tools.queue;

import java.io.IOException;
import java.util.List;

public interface ObjectQueues<E> extends ObjectQueue<E> {

	String name();

	List<PollableObjectQueue<E>> partitions();

	void resize(int newSize) throws IOException;

	default void interrupt() {
		partitions().forEach(PollableObjectQueue::interrupt);
	}

}
