package org.yah.tools.ringbuffer.impl;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.yah.tools.ringbuffer.RingBuffer;
import org.yah.tools.ringbuffer.UncheckedInterruptedException;

/**
 * Abstract implementation of {@link RingBuffer}<br/>
 */
public abstract class AbstractRingBuffer implements RingBuffer, Closeable {

	public interface StateOperator {
		RingBufferState udpateState(RingBufferState state) throws IOException;
	}

	private final int limit;

	private volatile RingBufferState state;

	private LinearBuffer linearBuffer;

	private final List<RingBufferInputStream> inputStreams = new ArrayList<>();

	private final Object writerMonitor = new Object();

	private RingBufferOutputStream outputStream;

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
	public synchronized InputStream reader() {
		RingBufferInputStream is = new RingBufferInputStream(this);
		inputStreams.add(is);
		return is;
	}

	@Override
	public OutputStream writer() {
		synchronized (writerMonitor) {
			while (outputStream != null) {
				try {
					writerMonitor.wait();
				} catch (InterruptedException e) {
					throw new UncheckedInterruptedException(e);
				}
			}
			outputStream = new RingBufferOutputStream(this);
			return outputStream;
		}
	}

	protected void releaseWriter(RingBufferOutputStream outputStream) {
		synchronized (writerMonitor) {
			if (this.outputStream != outputStream) {
				throw new IllegalStateException(outputStream + " does is not that current writer, "
						+ this.outputStream);
			}
			this.outputStream = null;
			writerMonitor.notify();
		}
	}

	@Override
	public synchronized int remove(int length) throws IOException {
		int removed = Math.min(size(), length);
		if (removed > 0)
			updateState(s -> s.remove(removed));
		return removed;
	}

	protected final void addInputStream(BufferedRingBufferInputStream is) {
		inputStreams.add(is);
	}

	protected final synchronized void removeInputStream(RingBufferInputStream is) {
		inputStreams.remove(is);
	}

	protected final synchronized void forEachInputStreams(Consumer<RingBufferInputStream> consumer) {
		inputStreams.forEach(consumer);
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

	protected final synchronized RingBufferState updateState(StateOperator operator)
			throws IOException {
		state = operator.udpateState(state);
		writeState(state);
		notifyAll();
		return state;
	}

	protected abstract LinearBuffer allocate(int capacity) throws IOException;

	protected void writeState(RingBufferState state) throws IOException {}

	protected LinearBuffer linearBuffer() {
		return linearBuffer;
	}

	protected RingBufferState ensureCapacity(int additional) throws IOException {
		// work with a state snapshot, it can change in time as follow:
		// - no other writer, so no other capacity change
		// - only concurrent read or remove:
		// - size can only grow
		// - write position will never change
		RingBufferState fromState = state();
		RingBufferState state = fromState;
		int available = availableToWrite(fromState);
		if (available < additional) {
			int missing = additional - available;
			int newCapacity = RingBufferUtils.nextPowerOfTwo(fromState.capacity() + missing);
			if (inLimit(newCapacity)) {
				// increase capacity
				LinearBuffer newBuffer = allocate(newCapacity);
				transferTo(newBuffer, fromState);

				synchronized (this) {
					state = updateState(s -> s.updateCapacity(newCapacity, fromState));
					linearBuffer = newBuffer;
					inputStreams.forEach(is -> is.updateCapacity(newCapacity, fromState));
				}
			} else {
				throw new RingBufferOverflowException(newCapacity, limit);
				// return waitFor(this::state, s -> availableToWrite(s) >= additional);
			}
		}
		return state;
	}

	private int availableToWrite(RingBufferState state) {
		if (limit > 0) {
			// capacity can be over current limit for persistent buffer that have been
			// reconfigured
			return Math.min(limit, state.capacity()) - state.size();
		}
		return state.capacity() - state.size();
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(getClass().getSimpleName());
		sb.append(" [capacity=").append(capacity());
		if (limit > 0)
			sb.append(", limit=").append(limit);
		sb.append("]").append(System.lineSeparator());
		int width = 80;
		sb.append('|');
		RingBufferState state = state();
		if (state.isEmpty()) {
			for (int i = 0; i < width; i++)
				sb.append('-');
		} else {
			float factor = width / (float) state.capacity();
			int readPos = (int) (state.position().position() * factor);
			int writePos = (int) (state.writePosition() * factor) - 1;
			if (writePos < 0)
				writePos = width;
			int i = 0;
			if (state.wrapped()) {
				for (; i < writePos; i++)
					sb.append('#');
				sb.append('>');
				i++;
				for (; i < readPos; i++)
					sb.append('-');
				for (; i < width; i++)
					sb.append('#');
			} else {
				for (; i < readPos; i++)
					sb.append('-');
				for (; i < writePos; i++)
					sb.append('#');
				sb.append('>');
				i++;
				for (; i < width; i++)
					sb.append('-');
			}
		}
		sb.append('|');
		return sb.toString();
	}

	private void transferTo(LinearBuffer target, RingBufferState fromState) throws IOException {
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
		long remaining = timeout > 0 ? timeUnit.toMillis(timeout) : Long.MAX_VALUE;
		long timeLimit = remaining > 0 ? System.currentTimeMillis() + remaining : Long.MAX_VALUE;
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

}
