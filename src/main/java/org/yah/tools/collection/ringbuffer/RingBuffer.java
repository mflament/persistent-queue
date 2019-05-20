/**
 * 
 */
package org.yah.tools.collection.ringbuffer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author Yah
 * @created 2019/05/10
 */
public interface RingBuffer {

	/**
	 * @return the current buffer size (in bytes)
	 */
	int size();

	/**
	 * @return a new InputStream starting from the current position.
	 */
	InputStream reader();

	/**
	 * @return the output stream used to write data to this buffer
	 */
	OutputStream writer();

	/**
	 * Remove length bytes from buffer start. @throws
	 * 
	 * @throws IOException
	 */
	int remove(int length) throws IOException;

}
