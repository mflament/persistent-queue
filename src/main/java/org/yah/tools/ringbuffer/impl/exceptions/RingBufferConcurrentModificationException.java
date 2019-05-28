package org.yah.tools.ringbuffer.impl.exceptions;

import java.io.IOException;

public class RingBufferConcurrentModificationException extends IOException {

	public RingBufferConcurrentModificationException(String message) {
		super(message);
	}

}
