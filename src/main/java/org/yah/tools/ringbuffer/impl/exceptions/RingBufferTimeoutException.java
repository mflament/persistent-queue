package org.yah.tools.ringbuffer.impl.exceptions;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class RingBufferTimeoutException extends IOException {

	public RingBufferTimeoutException(String message) {
		super(message);
	}

	public RingBufferTimeoutException(TimeoutException cause) {
		super(cause);
	}

	@Override
	public synchronized TimeoutException getCause() {
		return (TimeoutException) super.getCause();
	}
}
