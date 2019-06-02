package org.yah.tools.executor;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class TaskPollerStats {

	private final int processed;

	private final long processingTime;

	private final int remaining;

	private final Duration remainingDuration;

	public TaskPollerStats(int processed, long processingTime, int remaining) {
		this(processed, processingTime, remaining, null);
	}

	public TaskPollerStats(int processed, long processingTime, int remaining, Duration remainingDuration) {
		this.processed = processed;
		this.processingTime = processingTime;
		this.remaining = remaining;
		if (remainingDuration == null) {
			float tp = processed / (processingTime / 1000f);
			this.remainingDuration = Duration.ofSeconds((long) (remaining / tp));
		} else {
			this.remainingDuration = remainingDuration;
		}
	}

	public int getProcessed() {
		return processed;
	}

	public long getProcessingTime() {
		return processingTime;
	}

	public int getRemaining() {
		return remaining;
	}

	public float throughput(TimeUnit timeUnit) {
		int seconds = (int) timeUnit.convert(processingTime, TimeUnit.MILLISECONDS);
		return seconds == 0 ? Float.POSITIVE_INFINITY : processed / (float) seconds;
	}

	public Duration getRemainingDuration() {
		return remainingDuration;
	}

	@Override
	public String toString() {
		return String.format(
				"TaskPollerStats [processed=%d, processingTime=%d, remaining=%d, remainingDuration=%s, throughput=%.2fe/s]",
				processed, processingTime, remaining, remainingDuration, throughput(TimeUnit.SECONDS));
	}

}
