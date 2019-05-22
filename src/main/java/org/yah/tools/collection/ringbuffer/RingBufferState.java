package org.yah.tools.collection.ringbuffer;

import java.io.IOException;

import org.yah.tools.collection.ringbuffer.RingPosition.RingAction;

public class RingBufferState {

	protected final RingPosition position;

	protected final int size;

	public RingBufferState(int position, long cycle, int capacity, int size) {
		this(new RingPosition(position, cycle, capacity), size);
	}

	public RingBufferState(RingPosition position, int size) {
		this.position = position;
		this.size = size;
	}

	public RingBufferState(RingBufferState from) {
		this.position = from.position;
		this.size = from.size;
	}

	protected RingBufferState remove(int length) {
		RingPosition nextPos = position.advance(length);
		int nextSize = size - length;
		return new RingBufferState(nextPos, nextSize);
	}

	public int size() {
		return size;
	}

	public RingPosition position() {
		return position;
	}

	public int writePosition() {
		return position.wrap(position.position() + size);
	}

	public RingBufferState incrementSize(int length) {
		return new RingBufferState(position, size + length);
	}

	public int availableToRead(RingPosition from) {
		int distance = from.substract(position);
		if (distance < 0)
			return -1;
		return size - distance;
	}

	public boolean wrapped() {
		return position.position() + size > position.capacity();
	}

	public RingBufferState updateCapacity(int newCapacity, RingBufferState fromState) {
		RingPosition newPos = position.updateCapacity(newCapacity, fromState);
		return new RingBufferState(newPos, size);
	}

	public RingBufferState withCapacity(int newCapacity) {
		return new RingBufferState(position.withCapacity(newCapacity), size);
	}

	@Override
	public String toString() {
		return String.format("RingBufferState [position=%s, size=%s]", position, size);
	}

	protected RingBufferState newState(int position, int cycle, int capacity, int size) {
		return new RingBufferState(position, cycle, capacity, size);
	}

	public int capacity() {
		return position.capacity();
	}

	public long cycle() {
		return position.cycle();
	}

	public int wrap(int position) {
		return this.position.wrap(position);
	}

	public void execute(int position, int length, RingAction action) throws IOException {
		this.position.execute(position, length, action);
	}

}