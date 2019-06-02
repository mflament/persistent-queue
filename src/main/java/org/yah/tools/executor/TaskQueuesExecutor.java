package org.yah.tools.executor;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yah.tools.queue.ObjectQueues;
import org.yah.tools.queue.PollableObjectQueue;

public class TaskQueuesExecutor<T> {

	private static final Logger LOGGER = LoggerFactory.getLogger(TaskQueuesExecutor.class);

	public enum Status {
		STARTING,
		RUNNING,
		STOPPING,
		STOPPED;
	}

	private final ObjectQueues<T> queues;

	private final List<Poller> pollers;

	private final Consumer<T> taskHandler;

	private int concurrency;

	private Phaser phaser;

	private AtomicReference<Status> status = new AtomicReference<>(Status.STOPPED);

	public TaskQueuesExecutor(ObjectQueues<T> queues,
			int concurrency,
			Consumer<T> taskHandler)
			throws IOException {
		if (concurrency <= 0)
			throw new IllegalArgumentException("concurency must be > 0");

		this.queues = Objects.requireNonNull(queues, "queues is null");
		this.taskHandler = Objects.requireNonNull(taskHandler, "taskHandler is null");
		pollers = new ArrayList<>(concurrency);
		setConcurrency(concurrency);
	}

	public int getConcurrency() {
		return concurrency;
	}

	public synchronized void setConcurrency(int concurrency) throws IOException {
		if (concurrency == this.concurrency)
			return;

		boolean stopped = stop();

		queues.resize(concurrency);

		String name = queues.name();
		int i = 0;
		for (PollableObjectQueue<T> partition : queues.partitions()) {
			if (i >= pollers.size()) {
				Poller poller = new Poller(name.toLowerCase() + "-thread-" + i, partition);
				pollers.add(poller);
			}
			i++;
		}

		while (pollers.size() > concurrency) {
			pollers.remove(pollers.size() - 1);
		}
		this.concurrency = concurrency;

		if (stopped)
			start();
	}

	public String name() {
		return queues.name();
	}

	public boolean start() throws IOException {
		if (!status.compareAndSet(Status.STOPPED, Status.STARTING))
			return false;

		phaser = new Phaser();
		phaser.bulkRegister(1 + concurrency);
		pollers.stream().map(p -> new Thread(p, p.name)).forEach(Thread::start);

		phaser.arriveAndAwaitAdvance();
		status.set(Status.RUNNING);

		return true;
	}

	public boolean stop() throws IOException {
		if (!status.compareAndSet(Status.RUNNING, Status.STOPPING))
			return false;

		queues.interrupt();
		phaser.arriveAndAwaitAdvance();
		status.set(Status.STOPPED);
		return true;
	}

	public void shutdown() throws IOException {
		stop();
		queues.close();
	}

	public Status getStatus() {
		return status.get();
	}

	public synchronized void submit(T task) throws IOException {
		queues.offer(task);
	}

	public synchronized void submit(Collection<T> tasks) throws IOException {
		queues.offer(tasks);
	}

	public TaskPollerStats statistics() {
		TaskPollerStats[] stats = pollersStatistics();
		int processed = 0;
		int remaining = 0;
		long maxProcessingTime = Long.MIN_VALUE;
		Duration maxDuration = null;
		for (int i = 0; i < stats.length; i++) {
			processed += stats[i].getProcessed();
			maxProcessingTime = Math.max(stats[i].getProcessingTime(), maxProcessingTime);
			remaining += stats[i].getRemaining();
			Duration pollerDuration = stats[i].getRemainingDuration();
			if (maxDuration == null || maxDuration.compareTo(pollerDuration) < 0)
				maxDuration = pollerDuration;
		}
		return new TaskPollerStats(processed, maxProcessingTime, remaining, maxDuration);
	}

	public TaskPollerStats[] pollersStatistics() {
		TaskPollerStats[] res = new TaskPollerStats[pollers.size()];
		int index = 0;
		for (Poller poller : pollers)
			res[index++] = poller.stats();
		return res;
	}

	private boolean isStopRequested() {
		return status.get() == Status.STOPPING;
	}

	private class Poller implements Runnable {

		private final PollableObjectQueue<T> queue;

		private final String name;

		private int processedCount;

		private long processingTime;

		public Poller(String name, PollableObjectQueue<T> queue) {
			this.name = name;
			this.queue = queue;
		}

		@Override
		public String toString() {
			return name;
		}

		@Override
		public void run() {
			phaser.arriveAndAwaitAdvance();
			try {
				messageLoop();
			} catch (RuntimeException e) {
				LOGGER.error("Unhandled error in message loop", e);
			} finally {
				LOGGER.debug("{} stopped", name);
				phaser.arrive();
			}
		}

		private void messageLoop() {
			while (!isStopRequested()) {
				T task;
				try {
					task = queue.poll();
				} catch (InterruptedException e) {
					return;
				} catch (IOException e) {
					LOGGER.error("Error polling message", e);
					return;
				}

				long startTime = System.currentTimeMillis();
				try {
					taskHandler.accept(task);
				} catch (RuntimeException e) {
					LOGGER.error("Error executing task {}", e);
					return;
				}

				try {
					commit(System.currentTimeMillis() - startTime);
				} catch (IOException e) {
					LOGGER.error("Error removing persistent message, there will be duplicate", e);
					return;
				}
			}
		}

		private synchronized void commit(long elapsed) throws IOException {
			queue.commit();
			processedCount++;
			processingTime += elapsed;
		}

		private synchronized TaskPollerStats stats() {
			return new TaskPollerStats(processedCount, processingTime, queue.size());
		}
	}

}
