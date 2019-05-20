package org.yah.tools.collection.ringbuffer;

public final class State extends RingPosition {

	private final int size;

	State(int capacity) {
		this(0, 0, capacity, 0);
	}

	State(int position, int cycle, int capacity, int size) {
		super(position, cycle, capacity);
		this.size = size;
	}

	protected State remove(int length) {
		int nextSize = size - length;
		int nextPos = wrap(position + length);
		int nextCycle = cycle;
		if (nextPos < position)
			nextCycle++;
		return new State(nextPos, nextCycle, capacity, nextSize);
	}

	public int size() {
		return size;
	}

	public int writePosition() {
		return wrap(position + size);
	}

	protected State incrementSize(int length) {
		return new State(position, cycle, capacity, size + length);
	}

	protected int availableToRead(RingPosition from) {
		int distance = from.substract(this);
		if (distance < 0)
			return -1;
		return size - distance;
	}

	public int availableToWrite() {
		return capacity - size;
	}

	protected boolean wrapped() {
		return position + size > capacity;
	}

	@Override
	protected State updateCapacity(int newCapacity, State fromState) {
		int offset = 0;
		if (fromState.wrapped() && position < fromState.writePosition()) {
			offset = fromState.capacity;
		}
		int cycleOffset = offset > 0 ? -1 : 0;
		return new State(position + offset, cycle + cycleOffset, newCapacity, size);
	}

	@Override
	public String toString() {
		return String.format("State [position=%s, size=%s, cycle=%s, capacity=%s]", position, size, cycle, capacity);
	}

}