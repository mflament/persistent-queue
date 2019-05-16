/**
 * 
 */
package org.yah.tools.collection.ringbuffer;

import java.nio.BufferOverflowException;

/**
 * @author Yah
 * @created 2019/05/10
 */
public interface RingBuffer {

	/**
	 * Read up to length byte from buffer start position.
	 * 
	 * @param position
	 *            the position to start reading from this buffer
	 * @param target
	 *            the bytes buffer that will receive read bytes
	 * @param offset
	 *            the offset in target to start writing buffer content
	 * @param length
	 *            the max number of bytes to read
	 * @return the actual number of bytes read, can be less than length if there is
	 *         no enough bytes to read
	 * 
	 * @throws IllegalArgumentException
	 *             if position < 0 or position >= size()
	 * @throws NullPointerException
	 *             if target is null
	 * @throws IllegalArgumentException
	 *             if offset < 0 or offset >= buffer.length
	 * @throws IllegalArgumentException
	 *             if offset + length > buffer.length
	 * 
	 */
	int read(int position, byte[] target, int offset, int length);

	default int read(int position, byte[] target) {
		return read(position, target, 0, target.length);
	}

	/**
	 * Append length bytes of data from source[offset] to the end of the
	 * buffer.<br/>
	 * Blocking call, increasing buffer or throwing exception is up to the
	 * implementation choice.
	 * 
	 * @param position
	 *            the position to start writing to this buffer
	 * 
	 * @param source
	 *            the source buffer data will be read from
	 * @param offset
	 *            the offset in the source buffer
	 * @param lenght
	 *            the number of bytes to write
	 * 
	 * @throws NullPointerException
	 *             if source is null
	 * @throws IllegalArgumentException
	 *             if offset < 0 or offset >= buffer.length
	 * @throws IllegalArgumentException
	 *             if offset + length > buffer.length
	 * @throws BufferOverflowException
	 *             if length > remaining()
	 */
	void write(byte[] source, int offset, int length);

	default void write(byte[] source) {
		write(source, 0, source.length);
	}

	/**
	 * Remove length bytes from buffer start. @throws
	 */
	int remove(int length);

	/**
	 * @return the current buffer size (in bytes)
	 */
	int size();
		
}
