package org.yah.tools.ringbuffer.impl.exceptions;

import java.io.IOException;

public class RingBufferInterruptedException extends IOException {

	public RingBufferInterruptedException(InterruptedException cause) {
		super(cause);
	}

	@Override
	public synchronized InterruptedException getCause() {
		return (InterruptedException) super.getCause();
	}
}
