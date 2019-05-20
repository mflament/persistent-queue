package org.yah.tools.collection.ringbuffer;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RingPosition {

	private static final Logger LOGGER = LoggerFactory.getLogger(RingPosition.class);

	@FunctionalInterface
	protected interface RingAction {
		void apply(int position, int length, int offset) throws IOException;
	}

	public static int wrap(int position, int capacity) {
		return position & (capacity - 1);
	}

	protected final int position;

	protected final int cycle;

	protected final int capacity;

	public RingPosition(RingPosition from) {
		this(from.position, from.cycle, from.capacity);
	}

	public RingPosition(int position, int cycle, int capacity) {
		this.position = position;
		this.cycle = cycle;
		this.capacity = capacity;
	}

	public final int position() {
		return position;
	}

	public final int cycle() {
		return cycle;
	}

	public final int capacity() {
		return capacity;
	}

	public final int wrap(int position) {
		return wrap(position, capacity);
	}

	public final int substract(RingPosition other) {
		if (other.cycle == cycle) {
			return position - other.position;
		} else if (other.cycle > cycle) {
			// my position to end of buffer
			int behind = capacity - position;

			// and full cycles until other
			// assume capacity was constant if more than one cycle
			int fullCycles = other.cycle - cycle - 1;
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
			int fullCycles = cycle - other.cycle - 1;
			if (fullCycles > 1 && capacity != other.capacity) {
				LOGGER.warn("{} cycles between {} and {}, distance will be wrong");
			}
			before += fullCycles * capacity;

			// my position from buffer start
			before += position;
			return before;
		}
	}

	protected final boolean after(RingPosition other) {
		return cycle > other.cycle || (cycle == other.cycle && position > other.position);
	}

	@Override
	public String toString() {
		return String.format("RingPosition [position=%s, cycle=%s, capacity=%s]", position, cycle, capacity);
	}

	protected RingPosition advance(int length) {
		int nextPos = wrap(position + length);
		int nextCycle = cycle;
		if (nextPos < position)
			nextCycle++;
		return new RingPosition(nextPos, nextCycle, capacity);
	}

	protected RingPosition updateCapacity(int newCapacity, State fromState) {
		int offset = 0;
		if (fromState.wrapped() && position < fromState.writePosition()) {
			offset = fromState.capacity;
		}
		int cycleOffset = offset > 0 ? -1 : 0;
		return new RingPosition(position + offset, cycle + cycleOffset, newCapacity);
	}

	protected void execute(int position, int length, RingAction action) throws IOException {
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
}
