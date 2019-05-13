package org.yah.tools.collection.ringbuffer;

public class Pow2List {

	public static void main(String[] args) {
		for (int i = 0; i < 32; i++) {
			int p = (int) Math.pow(2, i);
			System.out.println(String.format("%2d : %10db %8dKb %5dMb %.2fGb", i, p, p / 1024, p / 1024 / 1024, p / 1024
					/ 1024
					/ 1024f));
		}
	}

}
