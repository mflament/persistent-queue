package org.yah.tools.ringbuffer.impl.exceptions;

import java.io.IOException;

public class RingBufferUnderflowException extends IOException {

	public RingBufferUnderflowException(int requested) {
		super(String.format("Requested %d bytes, no bytes available", requested));
	}

}
