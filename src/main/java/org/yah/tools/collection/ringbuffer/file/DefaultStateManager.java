package org.yah.tools.collection.ringbuffer.file;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;

import org.yah.tools.collection.ringbuffer.RingBufferState;
import org.yah.tools.collection.ringbuffer.StateManager;

public class DefaultStateManager implements StateManager {

	public DefaultStateManager() {
		headerBuffer = ByteBuffer.allocate(headerLength());
	}

}
