package org.yah.tools.queue.impl;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yah.tools.queue.ObjectQueues;
import org.yah.tools.queue.PollableObjectQueue;
import org.yah.tools.ringbuffer.impl.RingBufferUtils;

public class PersistentObjectQueues<E> implements ObjectQueues<E> {

	public interface PersistentObjectQueueFactory<E> {
		PersistentObjectQueue<E> create(File file) throws IOException;
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(PersistentObjectQueues.class);

	private static final Pattern PARTITION_FILE_NAME_PATTERN = Pattern.compile("partition-(\\d+).dat");

	private final File directory;

	private final PersistentObjectQueueFactory<E> queueFactory;

	private List<Partition<E>> partitions = new ArrayList<>();

	public PersistentObjectQueues(File directory, ObjectConverter<E> elementConverter) throws IOException {
		this(directory, PersistentObjectQueue.builder(elementConverter));
	}

	public PersistentObjectQueues(File directory, PersistentObjectQueue.Builder<E> queueBuilder) throws IOException {
		this(directory, f -> queueBuilder.withFile(f).build());
	}

	public PersistentObjectQueues(File directory, PersistentObjectQueueFactory<E> queueFactory) throws IOException {
		this.directory = Objects.requireNonNull(directory, "directory is null");
		this.queueFactory = Objects.requireNonNull(queueFactory, "queueFactory is null");
		if (!directory.exists() && !directory.mkdirs())
			throw new IllegalArgumentException("Unable to create storage directory " + directory);
		if (!directory.isDirectory())
			throw new IllegalArgumentException(directory + " is not a directory");

		loadPartitions();
	}

	@Override
	public String name() {
		return directory.getName();
	}

	public File getDirectory() {
		return directory;
	}

	@Override
	public int size() {
		return partitions.stream().mapToInt(Partition::size).sum();
	}

	@Override
	public List<PollableObjectQueue<E>> partitions() {
		return new ArrayList<>(partitions);
	}

	@Override
	public void resize(int newSize) throws IOException {
		List<Partition<E>> sources = new ArrayList<>(partitions);
		List<Partition<E>> newQueues = new ArrayList<>(newSize);
		List<Partition<E>> queuesToDelete = new ArrayList<>();

		final int totalSize = size();

		// keep current queues
		int count = Math.min(partitions.size(), newSize);
		for (int i = 0; i < count; i++) {
			newQueues.add(partitions.get(i));
		}

		// create new queues
		for (int i = count; i < newSize; i++) {
			newQueues.add(createPartition(createFile(newQueues.size())));
		}

		// tag queues to delete
		for (int i = newSize; i < partitions.size(); i++) {
			queuesToDelete.add(partitions.get(i));
		}

		partitions = newQueues;

		try {
			int averageSize = (int) Math.ceil(totalSize / (float) newSize);
			for (Partition<E> partition : sources) {
				int targetSize;
				if (partitions.contains(partition)) {
					targetSize = averageSize;
				} else {
					targetSize = 0;
				}
				if (partition.size() > targetSize) {
					transfer(partition, averageSize, partition.size() - targetSize);
				}
			}

			if (!queuesToDelete.stream().allMatch(Partition::isEmpty)) {
				throw new IllegalStateException("some queues to delete are not empty");
			}
		} finally {
			// close queues to delete
			queuesToDelete.forEach(RingBufferUtils::closeQuietly);
		}

		queuesToDelete.forEach(Partition::delete);
	}

	@Override
	public QueueCursor<E> cursor() {
		return new QueuesCursor();
	}

	@Override
	public void offer(Collection<E> elements) throws IOException {
		PartitionSizes<E> sizes = sizes();
		List<PartitionSize<E>> partitionsSizes = new ArrayList<>(sizes.partitions());
		Collections.sort(partitionsSizes, Comparator.comparing(PartitionSize::getSize));

		int targetSize = sizes.getTotal() + elements.size();
		int targetAverage = (int) Math.ceil(targetSize / (float) sizes.partitionsCount());

		int remaining = elements.size();
		Iterator<E> iterator = elements.iterator();
		for (PartitionSize<E> partitionSize : partitionsSizes) {
			int available = targetAverage - partitionSize.getSize();
			available = Math.max(0, Math.min(remaining, available));
			if (available > 0) {
				Collection<E> chunk = collect(iterator, available);
				partitionSize.getPartition().offer(chunk);
				remaining -= available;
				if (remaining == 0)
					break;
			}
		}
		if (remaining > 0)
			throw new IllegalStateException("Damn, we have missed some one");
	}

	private Collection<E> collect(Iterator<E> iterator, int available) {
		Collection<E> res = new ArrayList<>(available);
		while (iterator.hasNext() && res.size() < available)
			res.add(iterator.next());
		return res;
	}

	@Override
	public void close() {
		partitions.forEach(RingBufferUtils::closeQuietly);
	}

	public PartitionSizes<E> sizes() {
		List<PartitionSize<E>> queues = partitions.stream()
			.map(p -> new PartitionSize<E>(p, p.size()))
			.collect(Collectors.toList());
		return new PartitionSizes<E>(queues);
	}

	private void loadPartitions() throws IOException {
		File[] files = directory.listFiles((d, n) -> match(n));
		for (int i = 0; i < files.length; i++) {
			partitions.add(createPartition(files[i]));
		}
		Collections.sort(partitions);
	}

	private Partition<E> createPartition(File file) throws IOException {
		PersistentObjectQueue<E> queue = queueFactory.create(file);
		return new Partition<>(file, queue);
	}

	private File createFile(int index) {
		return new File(directory, "partition-" + index + ".dat");
	}

	@Override
	public String toString() {
		return String.format("PersistentObjectQueues [directory=%s, queueFactory=%s, partitions=%s]", directory,
				queueFactory, partitions);
	}

	private void transfer(Partition<E> source, int requestedSize, int amount) throws IOException {
		if (amount == 0)
			return;

		int remaining = amount;
		for (Partition<E> target : partitions) {
			if (target == source)
				continue;

			int available = requestedSize - target.size();
			if (available > 0) {
				int transferSize = Math.min(available, remaining);
				source.transferTo(target, transferSize);
				remaining -= transferSize;
				if (remaining == 0)
					return;
			}
		}
		throw new IllegalStateException("not enough space on tagets");
	}

	public static int getQueueIndex(File queueFile) {
		Matcher matcher = PARTITION_FILE_NAME_PATTERN.matcher(queueFile.getName());
		if (matcher.matches())
			return Integer.parseInt(matcher.group(1));
		throw new IllegalArgumentException("queue file name " + queueFile.getName() + " does not match "
				+ PARTITION_FILE_NAME_PATTERN);
	}

	public static boolean match(String name) {
		return PARTITION_FILE_NAME_PATTERN.matcher(name).matches();
	}

	public static class Partition<E> implements Comparable<Partition<E>>, PollableObjectQueue<E> {

		private final int index;

		private final File file;

		private final PersistentObjectQueue<E> queue;

		public Partition(File file, PersistentObjectQueue<E> queue) {
			this.file = file;
			this.index = getQueueIndex(file);
			this.queue = queue;
		}

		private void transferTo(Partition<E> target, int length) throws IOException {
			queue.transferTo(target.queue, length);
		}

		@Override
		public void close() throws IOException {
			queue.close();
		}

		@Override
		public int size() {
			return queue.size();
		}

		public void delete() {
			if (!file.delete())
				LOGGER.error("Unable to delete {}", file);
		}

		@Override
		public int compareTo(Partition<E> o) {
			return Integer.compare(index, o.index);
		}

		@Override
		public void offer(E element) throws IOException {
			queue.offer(element);
		}

		@Override
		public void offer(Collection<E> elements) throws IOException {
			queue.offer(elements);
		}

		@Override
		public QueueCursor<E> cursor() throws IOException {
			return queue.cursor();
		}

		@Override
		public E poll() throws IOException, InterruptedException {
			return queue.poll();
		}

		@Override
		public void commit() throws IOException {
			queue.commit();
		}

		@Override
		public void clear() throws IOException {
			queue.clear();
		}

		@Override
		public void interrupt() {
			queue.interrupt();
		}

		@Override
		public String toString() {
			return String.format("Partition [index=%s, queue=%s]", index, queue);
		}

	}

	private class QueuesCursor implements QueueCursor<E> {

		private Iterator<Partition<E>> partitionsIterator;

		private QueueCursor<E> partitionCursor;

		private E next;

		public QueuesCursor() {
			partitionsIterator = partitions.iterator();
			next = fetchNext();
		}

		@Override
		public boolean hasNext() {
			return next != null;
		}

		@Override
		public E next() {
			if (!hasNext())
				throw new NoSuchElementException();
			E res = next;
			next = fetchNext();
			return res;
		}

		@Override
		public void close() {
			if (partitionCursor != null) {
				RingBufferUtils.closeQuietly(partitionCursor);
				partitionCursor = null;
			}
		}

		private E fetchNext() {
			if (partitionCursor != null && partitionCursor.hasNext())
				return partitionCursor.next();
			partitionCursor = nextCursor();
			return partitionCursor != null ? partitionCursor.next() : null;
		}

		private QueueCursor<E> nextCursor() {
			close();
			while (partitionsIterator.hasNext()) {
				Partition<E> partition = partitionsIterator.next();
				QueueCursor<E> cursor;
				try {
					cursor = partition.cursor();
				} catch (IOException e) {
					throw new UncheckedIOException(e);
				}

				if (cursor.hasNext())
					return cursor;
				RingBufferUtils.closeQuietly(cursor);
			}
			return null;
		}

	}

	public static List<File> listPartitions(File directory) {
		if (!directory.exists())
			return Collections.emptyList();
		return Arrays.asList(directory.listFiles(f -> match(f.getName())));
	}

}
