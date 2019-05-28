package org.yah.tools.ringbuffer.impl.exceptions;

public class WaitForWriterInterruptedException extends RuntimeException {

	public WaitForWriterInterruptedException(InterruptedException cause) {
		super(cause);
	}

	@Override
	public synchronized InterruptedException getCause() {
		return (InterruptedException) super.getCause();
	}
}
