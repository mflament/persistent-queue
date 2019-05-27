package org.yah.tools.ringbuffer;

public class UncheckedInterruptedException extends RuntimeException {

	public UncheckedInterruptedException(InterruptedException cause) {
		super(cause);
	}

	@Override
	public synchronized InterruptedException getCause() {
		return (InterruptedException) super.getCause();
	}

}
