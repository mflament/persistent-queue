package org.yah.tools.queue.impl;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class CappedIterator<T> implements Iterator<T> {

	private final Iterator<T> delegate;

	private int limit;

	private int count;

	public CappedIterator(Iterator<T> delegate) {
		this(delegate, Integer.MAX_VALUE);
	}

	public CappedIterator(Iterator<T> delegate, int limit) {
		this.delegate = delegate;
		this.limit = limit;
	}

	@Override
	public boolean hasNext() {
		return count < limit && delegate.hasNext();
	}

	@Override
	public T next() {
		if (!hasNext())
			throw new NoSuchElementException();
		count++;
		return delegate.next();
	}
	
	public void reset(int limit) {
		this.limit = limit;
		count = 0;
	}
}