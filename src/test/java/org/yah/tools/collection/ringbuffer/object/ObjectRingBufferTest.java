package org.yah.tools.collection.ringbuffer.object;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import org.junit.Test;

public class ObjectRingBufferTest {

	private ObjectRingBuffer<String> createBuffer(boolean delete) throws IOException {
		File file = new File("target/test/ring-buffers/object-buffer.dat");
		if (delete && file.exists())
			file.delete();
		return ObjectRingBuffer.<String>builder()
			.withFile(file)
			.build();
	}

	private ObjectRingBuffer<String> newBuffer() throws IOException {
		return createBuffer(true);
	}

	private ObjectRingBuffer<String> loadBuffer() throws IOException {
		return createBuffer(false);
	}

	@Test
	public void test_write() throws IOException {
		try (ObjectRingBuffer<String> buffer = newBuffer()) {
			assertEquals(0, buffer.elementsCount());
			buffer.write(Collections.singleton("value"));
			assertEquals(1, buffer.elementsCount());
			String actual = buffer.poll();
			assertEquals("value", actual);
		}
	}

	@Test
	public void test_write_persistency() throws IOException {
		try (ObjectRingBuffer<String> buffer = newBuffer()) {
			buffer.write(Collections.singleton("value"));
			assertEquals(1, buffer.elementsCount());
		}

		try (ObjectRingBuffer<String> buffer = loadBuffer()) {
			assertEquals(1, buffer.elementsCount());
			String actual = buffer.poll();
			assertEquals("value", actual);
		}
	}

	@Test
	public void test_poll() throws IOException {
		try (ObjectRingBuffer<String> buffer = newBuffer()) {
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
		try (ObjectRingBuffer<String> buffer = newBuffer()) {
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