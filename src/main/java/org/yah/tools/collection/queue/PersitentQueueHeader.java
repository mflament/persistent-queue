package org.yah.tools.collection.queue;

import org.yah.tools.collection.ringbuffer.file.Header;

public class PersitentQueueHeader extends Header {

	private static final int ELEMENT_COUNT_OFFSET = LAST_OFFSET + 1;

	public PersitentQueueHeader() {
		super();
		setElementCount(0);
	}

	public final int getElementCount() {
		return dataBuffer.get(ELEMENT_COUNT_OFFSET);
	}

	public final void setElementCount(int count) {
		dataBuffer.put(ELEMENT_COUNT_OFFSET, count);
	}

	@Override
	public int length() {
		return super.length() + Integer.BYTES;
	}

	@Override
	public String toString() {
		return String.format("PersitentQueueHeader [elementCount=%s, startPosition=%s, size=%s]",
				getElementCount(), getStartPosition(), getSize());
	}

}
