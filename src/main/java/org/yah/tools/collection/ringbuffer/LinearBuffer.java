package org.yah.tools.collection.ringbuffer;

import java.io.IOException;

public interface LinearBuffer {

	int capacity();

	void read(int position, byte[] target, int offset, int length) throws IOException;

	void write(int position, byte[] source, int offset, int length) throws IOException;

	void copyTo(LinearBuffer target, int position, int targetPosition, int length) throws IOException;

}