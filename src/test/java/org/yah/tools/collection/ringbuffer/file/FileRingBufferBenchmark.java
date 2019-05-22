package org.yah.tools.collection.ringbuffer.file;

import static org.junit.Assert.assertArrayEquals;

import java.io.File;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.CountDownLatch;

import org.yah.tools.collection.ringbuffer.RingBufferClosedException;
import org.yah.tools.collection.ringbuffer.RingBufferInputStream;
import org.yah.tools.collection.ringbuffer.RingBufferUtils;

public class FileRingBufferBenchmark {

	protected static final byte[] data(int size) {
		byte[] res = new byte[size];
		for (int i = 0; i < res.length; i++) {
			res[i] = (byte) i;
		}
		return res;
	}

	private FileRingBuffer newEmptyRingBuffer(int capacity, int limit, int readerCache) throws IOException {
		File file = new File("target/test/ring-buffers/bench-buffer.dat");
		if (file.exists())
			file.delete();
		return new FileRingBuffer(file, capacity, limit, readerCache);
	}

	private void benchmark(FileRingBuffer ringBuffer, int dataSize, int messageSize) throws IOException,
			InterruptedException, NoSuchAlgorithmException {
		byte[] message = data(messageSize);

		final MessageDigest readerDigest = MessageDigest.getInstance("SHA-1");
		int messageCount = dataSize / messageSize;
		int totalSize = messageCount * messageSize;
		CountDownLatch closeLatch = new CountDownLatch(1);
		StringBuilder logBuilder = new StringBuilder();
		Thread thread = new Thread(() -> {
			long timeToRead = 0, timeToRemove = 0;
			try (RingBufferInputStream is = ringBuffer.reader()) {
				int remaining = messageCount;
				while (remaining > 0) {
					long start = System.currentTimeMillis();
					byte[] m = RingBufferUtils.readFully(is, messageSize);
					readerDigest.update(m);
					timeToRead += System.currentTimeMillis() - start;

					start = System.currentTimeMillis();
					ringBuffer.remove(messageSize);
					timeToRemove += System.currentTimeMillis() - start;

					remaining--;
				}

				logBuilder.append(formatThroughput("read", timeToRead, totalSize)).append(System.lineSeparator());
				logBuilder.append(formatThroughput("erase", timeToRemove, totalSize)).append(System.lineSeparator());
			} catch (RingBufferClosedException e) {
				// ignore
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				closeLatch.countDown();
			}
		});
		thread.start();

		long timeToWrite = 0;
		int remaining = messageCount;
		long benchStart = System.currentTimeMillis();

		final MessageDigest writerDigest = MessageDigest.getInstance("SHA-1");
		while (remaining > 0) {
			long start = System.currentTimeMillis();
			ringBuffer.writer().write(message);
			writerDigest.update(message);
			timeToWrite += System.currentTimeMillis() - start;
			remaining--;
		}

		System.out.println(String.format("%,d messages (%,d bytes)", messageCount, messageCount * messageSize));
		System.out.println();
		System.out.println(formatThroughput("write", timeToWrite, totalSize));
		closeLatch.await();

		System.out.println(logBuilder.toString());

		long totalTime = System.currentTimeMillis() - benchStart;
		System.out.println(formatThroughput("total", totalTime, totalSize));

		assertArrayEquals(writerDigest.digest(), readerDigest.digest());

		ringBuffer.close();
	}

	private String formatThroughput(String opname, long time, long size) {
		return String.format("%5s time %,6d ms : %,d Kb/s", opname, time, size / time * 1000 / 1024);
	}

	public static void main(String[] args) throws IOException, InterruptedException, NoSuchAlgorithmException {
		FileRingBufferBenchmark benchmark = new FileRingBufferBenchmark();
		int capacity = 1024 * 1024;
		try (FileRingBuffer buffer = benchmark.newEmptyRingBuffer(capacity, -1, 128 * 1024)) {
			benchmark.benchmark(buffer, 1024 * 1024, 48);
		}
	}
}
