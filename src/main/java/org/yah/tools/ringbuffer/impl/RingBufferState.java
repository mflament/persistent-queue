package org.yah.tools.ringbuffer.impl;

import java.io.IOException;

import org.yah.tools.ringbuffer.impl.RingPosition.RingAction;

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

	public RingBufferState(int capacity) {
		this(0, 0, capacity, 0);
	}

	protected RingBufferState remove(int length) {
		RingPosition nextPos = position.advance(length);
		int nextSize = size - length;
		return newState(nextPos, nextSize);
	}

	public int size() {
		return size;
	}

	public RingPosition position() {
		return position;
	}

	public int writePosition() {
		return wrap(position.position() + size);
	}

	public RingPosition writePosition(int offset) {
		return position.advance(size + offset);
	}

	public RingBufferState incrementSize(int length) {
		return newState(position, size + length);
	}

	public int availableToRead(RingPosition from) {
		int distance = from.substract(position);
		if (distance < 0)
			return -1;
		return Math.max(0, size - distance);
	}

	public boolean wrapped() {
		return position.position() + size > position.capacity();
	}

	public RingBufferState updateCapacity(int newCapacity, RingBufferState fromState) {
		RingPosition newPos = position.updateCapacity(newCapacity, fromState);
		return newState(newPos, size);
	}

	public RingBufferState withCapacity(int newCapacity) {
		return newState(position.withCapacity(newCapacity), size);
	}

	@Override
	public String toString() {
		return String.format("RingBufferState [position=%s, size=%s]", position, size);
	}

	protected final RingBufferState newState(RingPosition ringPosition, int size) {
		return newState(ringPosition.position(), ringPosition.cycle(), ringPosition.capacity(), size);
	}

	protected RingBufferState newState(int position, long cycle, int capacity, int size) {
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

	public RingBufferState shrink(int newCapacity) {
		return newState(new RingPosition(0, cycle(), newCapacity), size);
	}

	public boolean isEmpty() {
		return size == 0;
	}

}