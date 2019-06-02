package org.yah.tools.executor;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.IntFunction;

import org.junit.Test;
import org.yah.tools.queue.ObjectQueues;
import org.yah.tools.queue.impl.PersistentObjectQueue;
import org.yah.tools.queue.impl.PersistentObjectQueues;
import org.yah.tools.queue.impl.converters.SerializableObjectConverter;

import com.fasterxml.jackson.annotation.JsonCreator;

public class TaskQueuesExecutorTest {

	public static final class TestTask implements Serializable {
		private final int id;

		public TestTask(int id) {
			this.id = id;
		}
	}

	public static final class FloodTask implements Serializable {

		private static final long serialVersionUID = 1L;

		private final String id;

		private final long time;

		private final boolean exit;

		@JsonCreator
		public FloodTask(String id, long time, boolean exit) {
			this.id = id;
			this.time = time;
			this.exit = exit;
		}

		public String getId() {
			return id;
		}

		public long getTime() {
			return time;
		}

		public boolean isExit() {
			return exit;
		}

		@Override
		public String toString() {
			return String.format("TestTask [id=%s, time=%s, exit=%s]", id, time, exit);
		}

	}

	private static class TaskGenerator<E extends Serializable> implements Iterator<E> {

		private final int total;

		private final IntFunction<E> taskFactory;

		private int remaining;

		public TaskGenerator(int total, IntFunction<E> taskFactory) {
			this.total = total;
			this.taskFactory = taskFactory;
			this.remaining = total;
		}

		@Override
		public boolean hasNext() {
			return remaining > 0;
		}

		@Override
		public E next() {
			if (!hasNext())
				throw new NoSuchElementException();
			int index = total - remaining;
			E task = taskFactory.apply(index);
			remaining--;
			return task;
		}
	}

	@Test
	public void test() throws IOException, InterruptedException {
		File dir = new File("target/testQueue");

		PersistentObjectQueues.listPartitions(dir).forEach(File::delete);

		SerializableObjectConverter<TestTask> converter = SerializableObjectConverter.instance();
		ObjectQueues<TestTask> queues = new PersistentObjectQueues<>(dir, PersistentObjectQueue.builder(converter));

		int taskCount = 1000;
		TaskGenerator<TestTask> taskGenerator = new TaskGenerator<>(taskCount, TestTask::new);
		BitSet bs = new BitSet(taskCount);
		CountDownLatch cdl = new CountDownLatch(taskCount);
		TaskQueuesExecutor<TestTask> executor = new TaskQueuesExecutor<>(queues, 4, t -> {
			synchronized (bs) {
				bs.set(t.id);
				cdl.countDown();
			}
		});
		executor.start();

		while (taskGenerator.hasNext()) {
			TestTask task = taskGenerator.next();
			executor.submit(task);
		}
		cdl.await();
		executor.shutdown();
		assertEquals(taskCount, bs.cardinality());
	}

	public static void main(String[] args) throws IOException, InterruptedException {
		File dir = new File("target/testQueue");
		SerializableObjectConverter<FloodTask> converter = SerializableObjectConverter.instance();
		ObjectQueues<FloodTask> queues = new PersistentObjectQueues<>(dir, PersistentObjectQueue.builder(converter));

		int taskCount = queues.size();
		final TaskGenerator<FloodTask> taskGenerator;
		if (taskCount == 0) {
			taskCount = 500_000;
			taskGenerator = new TaskGenerator<>(taskCount, i -> new FloodTask("flood task-" + i, 1, false));
		} else {
			taskGenerator = null;
		}

		CountDownLatch cdl = new CountDownLatch(taskCount);
		TaskQueuesExecutor<FloodTask> executor = new TaskQueuesExecutor<>(queues,
				8,
				taskExecutor(cdl));
		executor.start();

		if (taskGenerator != null) {
			new Thread(() -> generate_tasks(taskGenerator, executor)).start();
		}

		System.out.println(executor.statistics());
		while (!cdl.await(1, TimeUnit.SECONDS)) {
			System.out.println(executor.statistics());
		}

		System.out.println("shuting down");
		executor.shutdown();
		System.out.println("shutdown complete");
	}

	private static <T extends Serializable> void generate_tasks(TaskGenerator<T> generator, TaskQueuesExecutor<T> executor) {
		System.out.println("submiting " + generator.total + " tasks");
		Random random = new Random();
		long start = System.currentTimeMillis();
		while (generator.hasNext()) {
			int chunkSize = 1 + random.nextInt(99);
			Collection<T> tasks = new ArrayList<>(chunkSize);
			while (tasks.size() < chunkSize && generator.hasNext()) {
				tasks.add(generator.next());
			}
			try {
				executor.submit(tasks);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		long elapsed = System.currentTimeMillis() - start;
		System.out.println("submited " + generator.total + " tasks in " + elapsed + "ms");
	}

	private static Consumer<FloodTask> taskExecutor(CountDownLatch countDownLatch) {
		return task -> {
			try {
				Thread.sleep(task.getTime());
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			if (task.exit)
				System.exit(0);
			countDownLatch.countDown();
		};
	}

}
