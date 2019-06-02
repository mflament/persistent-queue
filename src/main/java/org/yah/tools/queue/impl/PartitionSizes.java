package org.yah.tools.queue.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class PartitionSizes<E> {

	private final int total;

	private final List<PartitionSize<E>> partitions;

	public PartitionSizes(List<PartitionSize<E>> partitions) {
		this.total = partitions.stream().mapToInt(PartitionSize::getSize).sum();
		this.partitions = new ArrayList<>(partitions);
	}

	public int getTotal() {
		return total;
	}

	public PartitionSize<E> get(int index) {
		return partitions.get(index);
	}

	public int partitionsCount() {
		return partitions.size();
	}

	public List<PartitionSize<E>> partitions() {
		return Collections.unmodifiableList(partitions);
	}

	@Override
	public String toString() {
		return String.format("%s(%s)", total, partitions);
	}

}