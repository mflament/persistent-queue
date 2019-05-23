package org.yah.tools.collection.ringbuffer;

import java.io.IOException;

public interface StateManager {

	default int headerLength() {
		return 0;
	}

	default RingBufferState read(int requestedCapacity) throws IOException {
		return new RingBufferState(0, 0, requestedCapacity, 0);
	}

	default void write(RingBufferState state) throws IOException {
		// no op
	}

}
