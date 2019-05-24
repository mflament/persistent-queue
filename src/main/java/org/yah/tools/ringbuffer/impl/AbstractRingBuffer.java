package org.yah.tools.ringbuffer.impl;

import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import org.yah.tools.ringbuffer.RingBuffer;

/**
 * Abstract implementation of {@link RingBuffer}<br/>
 */
public abstract class AbstractRingBuffer implements RingBuffer, Closeable {

	private final int limit;

	private RingBufferState state;

	private LinearBuffer linearBuffer;

	private final List<RingBufferInputStream> inputStreams = new ArrayList<>();

	private final Object writeMonitor = new Object();

	protected AbstractRingBuffer(int limit) {
		this.limit = limit;
	}

	public RingBufferState state() {
		return state;
	}

	@Override
	public final int size() {
		return state.size();
	}

	public final int limit() {
		return limit;
	}

	@Override
	public final RingBufferInputStream reader() {
		RingBufferInputStream is = createInputStream();
		addInputStream(is);
		return is;
	}

	@Override
	public final synchronized RingBufferOutputStream writer() {
		return createOutputStream();
	}

	@Override
	public synchronized int remove(int length) throws IOException {
		int removed = Math.min(size(), length);
		if (removed > 0)
			updateState(s -> s.remove(removed));
		return removed;
	}

	protected void write(byte[] source, int offset, int length) throws IOException {
		write(source, offset, length, s -> s.incrementSize(length));
	}

	protected final void write(byte[] source, int offset, int length, UnaryOperator<RingBufferState> stateUpdater)
			throws IOException {
		synchronized (writeMonitor) {
			RingBufferState state = ensureCapacity(length);
			int writePosition = state.writePosition();
			state.execute(writePosition, length, (p, l, o) -> linearBuffer.write(p, source, offset + o, l));
			updateState(stateUpdater);
		}
	}

	protected RingBufferInputStream createInputStream() {
		return new RingBufferInputStream(this);
	}

	protected RingBufferOutputStream createOutputStream() {
		return new RingBufferOutputStream(this);
	}

	protected final synchronized void addInputStream(RingBufferInputStream is) {
		inputStreams.add(is);
	}

	protected final synchronized void removeInputStream(RingBufferInputStream is) {
		inputStreams.remove(is);
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

	protected final int capacity() {
		return state.capacity();
	}

	protected final boolean inLimit(int newCapacity) {
		return limit <= 0 || newCapacity <= limit;
	}

	protected final void restore(RingBufferState state, LinearBuffer linearBuffer) {
		this.state = state;
		this.linearBuffer = linearBuffer;
	}

	protected final synchronized RingBufferState updateState(UnaryOperator<RingBufferState> operator)
			throws IOException {
		state = operator.apply(state);
		writeState(state);
		notifyAll();
		return state;
	}

	protected abstract LinearBuffer allocate(int capacity) throws IOException;

	protected void writeState(RingBufferState state) throws IOException {}

	protected LinearBuffer linearBuffer() {
		return linearBuffer;
	}

	private RingBufferState ensureCapacity(int additional) throws IOException {
		// work with a state snapshot, it can change in time as follow:
		// - no other writer, so no other capacity change
		// - only concurrent read or remove:
		// - size can only grow
		// - write position will never change
		RingBufferState state = state();
		int available = availableToWrite(state);
		if (available < additional) {
			int missing = additional - available;
			int newCapacity = RingBufferUtils.nextPowerOfTwo(state.capacity() + missing);
			if (inLimit(newCapacity)) {
				LinearBuffer newBuffer = allocate(newCapacity);
				return transferTo(newBuffer, state);
			}
			return waitFor(this::state, s -> availableToWrite(s) >= additional);
		}
		return state;
	}

	private int availableToWrite(RingBufferState s) {
		if (limit > 0) {
			// capacity can be over current limit for persistent buffer that have been
			// reconfigured
			return Math.min(limit, s.capacity()) - s.size();
		}
		return s.capacity() - s.size();
	}

	@Override
	public String toString() {
		return String.format("%s [state=%s]", getClass().getSimpleName(), state);
	}

	protected RingBufferState transferTo(LinearBuffer target, RingBufferState fromState) throws IOException {
		int startPosition = fromState.position().position();
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

	private RingBufferState updateBuffer(RingBufferState currentState, LinearBuffer target, RingBufferState fromState) {
		int newCapacity = target.capacity();
		RingBufferState newState = currentState.updateCapacity(newCapacity, fromState);
		inputStreams.forEach(is -> is.updateCapacity(newCapacity, fromState));
		linearBuffer = target;
		return newState;
	}

	protected final synchronized <C> C waitFor(Supplier<C> contextSupplier,
			Predicate<C> contextPredicate)
			throws InterruptedIOException {
		return waitFor(contextSupplier, contextPredicate, 0, TimeUnit.MILLISECONDS);
	}

	protected final synchronized <C> C waitFor(Supplier<C> contextSupplier,
			Predicate<C> contextPredicate, long timeout, TimeUnit timeUnit)
			throws InterruptedIOException {
		C last = contextSupplier.get();
		long remaining = timeUnit.toMillis(timeout);
		long timeLimit = remaining > 0 ? System.currentTimeMillis() + remaining : 0;
		while (!contextPredicate.test(last) && remaining > 0) {
			try {
				wait(remaining);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new InterruptedIOException();
			}
			last = contextSupplier.get();
			remaining = timeLimit - System.currentTimeMillis();
		}
		return contextPredicate.test(last) ? last : null;
	}

	protected final synchronized <V> Optional<V> convertIf(Predicate<RingBufferState> predicate,
			Function<RingBufferState, V> converter) {
		RingBufferState state = state();
		if (predicate.test(state))
			return Optional.of(converter.apply(state));
		return Optional.empty();
	}

}