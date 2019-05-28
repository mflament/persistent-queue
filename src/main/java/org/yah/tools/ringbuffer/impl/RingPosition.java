package org.yah.tools.ringbuffer.impl;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RingPosition {

	private static final Logger LOGGER = LoggerFactory.getLogger(RingPosition.class);

	@FunctionalInterface
	public interface RingAction {
		void apply(int position, int length, int offset) throws IOException;
	}

	public static int wrap(int position, int capacity) {
		return position & (capacity - 1);
	}

	private final int position;

	private final long cycle;

	private final int capacity;

	public RingPosition(RingPosition from) {
		this(from.position, from.cycle, from.capacity);
	}

	public RingPosition(int position, long cycle, int capacity) {
		this.position = position;
		this.cycle = cycle;
		this.capacity = capacity;
	}

	public int position() {
		return position;
	}

	public long cycle() {
		return cycle;
	}

	public int capacity() {
		return capacity;
	}

	public int wrap(int position) {
		return wrap(position, capacity);
	}

	public int substract(RingPosition other) {
		if (other.cycle == cycle) {
			return position - other.position;
		} else if (other.cycle > cycle) {
			// my position to end of buffer
			int behind = capacity - position;

			// and full cycles until other
			// assume capacity was constant if more than one cycle
			long fullCycles = other.cycle - cycle - 1;
			if (fullCycles > 1 && capacity != other.capacity) {
				LOGGER.warn("{} cycles between {} and {}, distance will be wrong");
			}
			behind += fullCycles * capacity;

			// distance to other from buffer start
			behind += other.position;
			return -behind;
		} else { // other.cycle < cycle
			// other distance to his buffer end
			int before = other.capacity - other.position;

			// and full cycles until me
			long fullCycles = cycle - other.cycle - 1;
			if (fullCycles > 1 && capacity != other.capacity) {
				LOGGER.warn("{} cycles between {} and {}, distance will be wrong");
			}
			before += fullCycles * capacity;

			// my position from buffer start
			before += position;
			return before;
		}
	}

	public boolean after(RingPosition other) {
		return cycle > other.cycle || (cycle == other.cycle && position > other.position);
	}

	@Override
	public String toString() {
		return String.format("RingPosition [position=%s, cycle=%s, capacity=%s]", position, cycle, capacity);
	}

	public RingPosition advance(int offset) {
		if (Math.abs(offset) > capacity)
			throw new IllegalArgumentException("offset can not exceed capacity (" + capacity + ")");
		if (offset == 0)
			return this;
		int nextPos = wrap(position + offset);
		long nextCycle = cycle;
		if (offset < 0 && nextPos >= position) {
			nextCycle--;
		} else if (offset > 0 && nextPos <= position) {
			nextCycle++;
		}
		return new RingPosition(nextPos, nextCycle, capacity);
	}

	public RingPosition withCapacity(int newCapacity) {
		return new RingPosition(position, cycle, newCapacity);
	}

	public RingPosition updateCapacity(int newCapacity, RingBufferState fromState) {
		int offset = 0;
		long nextCycle = cycle;
		if (fromState.wrapped() && position < fromState.writePosition()) {
			offset = fromState.capacity();
			nextCycle--;
		}
		return new RingPosition(position + offset, nextCycle, newCapacity);
	}

	public RingPosition shrink(RingPosition from, int newCapacity) {
		int nextPos = substract(from);
		long nextCycle = from.cycle;
		if (nextPos < 0)
			nextCycle--;
		return new RingPosition(nextPos, nextCycle, newCapacity);
	}

	public void execute(int position, int length, RingAction action) throws IOException {
		if (length > capacity)
			throw new IllegalArgumentException("Invalid length " + length + ", current capacity " + capacity);
		if (length < 0)
			throw new IllegalArgumentException("Invalid length " + length + ", must be >= 0");
		if (length == 0)
			return;

		int endPosition = wrap(position + length);
		if (endPosition <= position) {
			// wrapped
			int tail = capacity - position;
			action.apply(position, tail, 0);
			action.apply(0, endPosition, tail);
		} else {
			action.apply(position, length, 0);
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + capacity;
		result = prime * result + (int) (cycle ^ (cycle >>> 32));
		result = prime * result + position;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		RingPosition other = (RingPosition) obj;
		return capacity == other.capacity &&
				cycle == other.cycle &&
				position == other.position;
	}

}
