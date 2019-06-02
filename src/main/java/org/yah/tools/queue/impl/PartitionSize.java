package org.yah.tools.queue.impl;

import org.yah.tools.queue.impl.PersistentObjectQueues.Partition;

public final class PartitionSize<E> {
	
	private final Partition<E> partition;
	
	private final int size;

	public PartitionSize(Partition<E> partition, int size) {
		super();
		this.partition = partition;
		this.size = size;
	}

	public Partition<E> getPartition() {
		return partition;
	}

	public int getSize() {
		return size;
	}

	@Override
	public String toString() {
		return String.format("%s=%d", partition, size);
	}

}