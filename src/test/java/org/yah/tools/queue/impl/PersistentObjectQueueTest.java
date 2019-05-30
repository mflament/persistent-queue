package org.yah.tools.queue.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;
import org.yah.tools.queue.ObjectQueue;

public class PersistentObjectQueueTest {

	private PersistentObjectQueue<String> createQueue(boolean delete) throws IOException {
		File file = new File("target/test/ring-buffers/object-buffer.dat");
		if (!file.getParentFile().exists() && !file.getParentFile().mkdirs())
			throw new IOException("Unable to create directory " + file.getParentFile());
		if (delete && file.exists())
			file.delete();
		return PersistentObjectQueue.<String>builder().withFile(file).build();
	}

	private PersistentObjectQueue<String> newQueue() throws IOException {
		return createQueue(true);
	}

	private ObjectQueue<String> loadQueue() throws IOException {
		return createQueue(false);
	}

	@Test
	public void test_write() throws IOException {
		try (ObjectQueue<String> buffer = newQueue()) {
			assertEquals(0, buffer.elementsCount());
			buffer.offer(Collections.singleton("value"));
			assertEquals(1, buffer.elementsCount());
			String actual = buffer.poll();
			assertEquals("value", actual);
		}
	}

	@Test
	public void test_write_persistency() throws IOException {
		try (ObjectQueue<String> buffer = newQueue()) {
			buffer.offer(Collections.singleton("value"));
			assertEquals(1, buffer.elementsCount());
		}

		try (ObjectQueue<String> buffer = loadQueue()) {
			assertEquals(1, buffer.elementsCount());
			String actual = buffer.poll();
			assertEquals("value", actual);
		}
	}

	@Test
	public void test_poll_autocommit() throws IOException {
		try (ObjectQueue<String> buffer = newQueue()) {
			buffer.offer(Arrays.asList("value1", "value2"));
			assertEquals(2, buffer.elementsCount());

			String actual = buffer.poll();
			assertEquals("value1", actual);
			assertEquals(2, buffer.elementsCount());

			actual = buffer.poll();
			assertEquals("value2", actual);
			assertEquals(1, buffer.elementsCount());

			buffer.commit();
			assertEquals(0, buffer.elementsCount());
		}
	}

	@Test
	public void test_iterator() throws IOException {
		try (PersistentObjectQueue<String> buffer = newQueue()) {
			buffer.offer(Arrays.asList("value1", "value2", "value3"));
			try (QueueCursor<String> cursor = buffer.cursor()) {
				assertTrue(cursor.hasNext());
				assertEquals("value1", cursor.next());

				assertTrue(cursor.hasNext());
				assertEquals("value2", cursor.next());

				assertTrue(cursor.hasNext());
				assertEquals("value3", cursor.next());

				assertFalse(cursor.hasNext());
			}
		}

	}

}
