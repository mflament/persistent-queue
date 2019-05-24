package org.yah.tools.queue.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import org.junit.Test;
import org.yah.tools.queue.ObjectQueue;
import org.yah.tools.queue.impl.PersistentObjectQueue;

public class PersistentObjectQueueTest {

	private PersistentObjectQueue<String> createBuffer(boolean delete) throws IOException {
		File file = new File("target/test/ring-buffers/object-buffer.dat");
		if (!file.getParentFile().exists() && !file.getParentFile().mkdirs())
			throw new IOException("Unable to create directory " + file.getParentFile());
		if (delete && file.exists())
			file.delete();
		return PersistentObjectQueue.<String>builder()
			.withFile(file)
			.build();
	}

	private PersistentObjectQueue<String> newBuffer() throws IOException {
		return createBuffer(true);
	}

	private ObjectQueue<String> loadBuffer() throws IOException {
		return createBuffer(false);
	}

	@Test
	public void test_write() throws IOException {
		try (ObjectQueue<String> buffer = newBuffer()) {
			assertEquals(0, buffer.elementsCount());
			buffer.write(Collections.singleton("value"));
			assertEquals(1, buffer.elementsCount());
			String actual = buffer.poll();
			assertEquals("value", actual);
		}
	}

	@Test
	public void test_write_persistency() throws IOException {
		try (ObjectQueue<String> buffer = newBuffer()) {
			buffer.write(Collections.singleton("value"));
			assertEquals(1, buffer.elementsCount());
		}

		try (ObjectQueue<String> buffer = loadBuffer()) {
			assertEquals(1, buffer.elementsCount());
			String actual = buffer.poll();
			assertEquals("value", actual);
		}
	}

	@Test
	public void test_poll() throws IOException {
		try (ObjectQueue<String> buffer = newBuffer()) {
			buffer.write(Arrays.asList("value1", "value2"));
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
		try (PersistentObjectQueue<String> buffer = newBuffer()) {
			buffer.write(Arrays.asList("value1", "value2", "value3"));
			Iterator<String> iterator = buffer.iterator();
			assertTrue(iterator.hasNext());
			assertEquals("value1", iterator.next());
			
			assertTrue(iterator.hasNext());
			assertEquals("value2", iterator.next());
			
			assertTrue(iterator.hasNext());
			assertEquals("value3", iterator.next());			

			assertFalse(iterator.hasNext());
		}

	}

}
