package org.yah.tools.collection.ringbuffer.object;

import org.yah.tools.collection.ringbuffer.RingBufferState;
import org.yah.tools.collection.ringbuffer.RingPosition;

final class ObjectRingBufferState extends RingBufferState {

	private final int elements;

	public ObjectRingBufferState(int position, int cycle, int capacity, int size, int elements) {
		super(position, cycle, capacity, size);
		this.elements = elements;
	}

	public ObjectRingBufferState(RingPosition position, int size, int elements) {
		super(position, size);
		this.elements = elements;
	}

	public ObjectRingBufferState(RingBufferState rbs, int elements) {
		super(rbs);
		this.elements = elements;
	}

	public int elements() {
		return elements;
	}

	@Override
	protected RingBufferState newState(int position, int cycle, int capacity, int size) {
		return new ObjectRingBufferState(position, cycle, capacity, size, elements);
	}

	@Override
	protected RingBufferState remove(int length) {
		return new ObjectRingBufferState(super.remove(length), elements - 1);
	}

	protected final RingBufferState incrementSize(int length, int count) {
		return new ObjectRingBufferState(position,
				size() + length,
				elements + count);
	}

}
