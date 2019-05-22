package org.yah.tools.collection.ringbuffer.object;

final class SizedObject<E> {
	
	private final E element;
	
	private final int size;

	public SizedObject(E element, int size) {
		super();
		this.element = element;
		this.size = size;
	}

	public E getElement() {
		return element;
	}

	public int getSize() {
		return size;
	}

}