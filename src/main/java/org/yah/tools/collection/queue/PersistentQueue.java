/**
 * 
 */
package org.yah.tools.collection.queue;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import org.yah.tools.collection.ringbuffer.RingBuffer;

/**
 * @author Yah
 * @created 2019/05/10
 */
public class PersistentQueue<E> implements BlockingQueue<E> {

	public interface ElementReader<E> {

		E read(byte[] buffer, int offset, int length) throws IOException;

		void write(E element, byte[] buffer, int offset) throws IOException;

	}

	private final RingBuffer ringBufer;

	private final Queue<E> elementBuffer;

	public PersistentQueue(RingBuffer ringBufer) {
		this.ringBufer = Objects.requireNonNull(ringBufer, "ringBufer is null");
		elementBuffer = new ConcurrentLinkedQueue<>();
	}

	@Override
	public E remove() {
		E res = poll();
		if (res == null)
			throw new NoSuchElementException();
		return res;
	}

	@Override
	public E poll() {
		return elementBuffer.poll();
	}

	@Override
	public E element() {
		E res = peek();
		if (res == null)
			throw new NoSuchElementException();
		return res;
	}

	@Override
	public E peek() {
		return elementBuffer.peek();
	}

	@Override
	public int size() {
		return ringBufer.size();
	}

	@Override
	public boolean isEmpty() {
		return ringBufer.size() == 0;
	}

	@Override
	public Iterator<E> iterator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object[] toArray() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> T[] toArray(T[] a) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean addAll(Collection<? extends E> c) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void clear() {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean add(E e) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean offer(E e) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void put(E e) throws InterruptedException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public E take() throws InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public E poll(long timeout, TimeUnit unit) throws InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int remainingCapacity() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean remove(Object o) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean contains(Object o) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int drainTo(Collection<? super E> c) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int drainTo(Collection<? super E> c, int maxElements) {
		// TODO Auto-generated method stub
		return 0;
	}

}
