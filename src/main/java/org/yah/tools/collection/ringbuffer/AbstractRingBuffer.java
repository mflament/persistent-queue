package org.yah.tools.collection.ringbuffer;

import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import org.yah.tools.collection.Utils;

/**
 * Abstract implementation of {@link RingBuffer}<br/>
 */
public abstract class AbstractRingBuffer implements RingBuffer, Closeable {

	private final int limit;

	private State state;

	protected LinearBuffer linearBuffer;

	private final List<RingBufferInputStream> inputStreams = new ArrayList<>();

	protected final Object writeMonitor = new Object();

	protected AbstractRingBuffer(int capacity, int limit) {
		this.limit = limit;
		this.state = new State(capacity);
	}

	protected abstract LinearBuffer allocate(int capacity) throws IOException;

	protected void onChange(State state) throws IOException {}

	@Override
	public final int size() {
		return state.size();
	}

	public final State getState() {
		return state;
	}

	public final int getLimit() {
		return limit;
	}

	@Override
	public final RingBufferInputStream reader() {
		RingBufferInputStream is = createInputStream();
		addInputStream(is);
		return is;
	}

	protected final synchronized void addInputStream(RingBufferInputStream is) {
		inputStreams.add(is);
	}

	@Override
	public final synchronized RingBufferOutputStream writer() {
		return createOutputStream();
	}

	protected RingBufferInputStream createInputStream() {
		return new RingBufferInputStream(this);
	}

	protected RingBufferOutputStream createOutputStream() {
		return new RingBufferOutputStream(this);
	}

	@Override
	public synchronized int remove(int length) throws IOException {
		int removed = Math.min(state.size(), length);
		if (removed > 0)
			updateState(s -> s.remove(removed));
		return removed;
	}

	protected final synchronized void removeInputStream(RingBufferInputStream ringBufferInputStream) {
		inputStreams.remove(ringBufferInputStream);
	}

	@Override
	public synchronized void close() throws IOException {
		RingBufferInputStream[] streams = inputStreams.toArray(new RingBufferInputStream[inputStreams.size()]);
		for (RingBufferInputStream stream : streams) {
			stream.close();
		}
		// wake up blocked readers
		notifyAll();
	}

	protected final void createBuffer(int capacity) throws IOException {
		if (limit > 0 && limit < capacity)
			throw new IllegalArgumentException("capacity " + capacity + " is greater than limit " + limit);
		this.linearBuffer = allocate(Utils.nextPowerOfTwo(capacity));
	}

	protected final void restoreBuffer(LinearBuffer buffer, int startPosition, int size) {
		this.linearBuffer = buffer;
		this.state = new State(startPosition, 0, this.linearBuffer.capacity(), size);
	}

	protected final int capacity() {
		return state.capacity();
	}

	protected final boolean inLimit(int newCapacity) {
		return limit <= 0 || newCapacity <= limit;
	}

	protected final synchronized State updateState(UnaryOperator<State> operator) throws IOException {
		state = operator.apply(state);
		onChange(state);
		notifyAll();
		return state;
	}

	@Override
	public String toString() {
		return String.format("%s [state=%s]", getClass().getSimpleName(), state);
	}

	protected State transferTo(LinearBuffer target, State fromState) throws IOException {
		int startPosition = fromState.position();
		int writePosition = fromState.writePosition();
		if (fromState.wrapped()) {
			// wrapped, copy end of buffer to other buffer at same position
			linearBuffer.copyTo(target, startPosition, startPosition, fromState.capacity() - startPosition);
			// since wrapped, transfer tail of ring from actual buffer start to new buffer
			// end
			linearBuffer.copyTo(target, 0, fromState.capacity(), writePosition);
		} else {
			// direct copy, same position
			linearBuffer.copyTo(target, startPosition, startPosition, fromState.size());
		}

		return updateState(s -> updateBuffer(s, target, fromState));
	}

	private State updateBuffer(State currentState, LinearBuffer target, State fromState) {
		int newCapacity = target.capacity();
		State newState = currentState.updateCapacity(newCapacity, fromState);
		inputStreams.forEach(is -> is.capacityUpdated(newCapacity, fromState));
		linearBuffer = target;
		return newState;
	}

	protected final synchronized <C> C waitFor(Supplier<C> contextSupplier,
			Predicate<C> contextPredicate)
			throws InterruptedIOException {
		C last = contextSupplier.get();
		while (!contextPredicate.test(last)) {
			try {
				wait();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new InterruptedIOException();
			}
			last = contextSupplier.get();
		}
		return last;
	}

	protected final synchronized <V> Optional<V> convertIf(Predicate<State> predicate, Function<State, V> converter) {
		State state = getState();
		if (predicate.test(state))
			return Optional.of(converter.apply(state));
		return Optional.empty();
	}

}
