package org.yah.tools.ringbuffer.impl;

import java.io.IOException;

public class RingBufferOverflowException extends IOException {

	public RingBufferOverflowException(int requested, int remaining) {
		super(String.format("Requested %d bytes, only %d available", requested, remaining));
	}

}
