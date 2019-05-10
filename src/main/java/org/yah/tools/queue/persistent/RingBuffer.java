/**
 * 
 */
package org.yah.tools.queue.persistent;

/**
 * @author Yah
 * @created 2019/05/10
 */
public interface RingBuffer {

	/**
	 * @return the buffer total capacity (in bytes)
	 */
	public int capacity();

	/**
	 * @return the current buffer size (in bytes)
	 */
	public int size();

	/**
	 * @return the remaining capacity (in bytes)
	 */
	public default int remaingin() {
		return capacity() - size();
	}

	/**
	 * Read up to length byte from buffer start position
	 * 
	 * @param target the bytes buffer that will receive read bytes
	 * @param offset the offset in target to start writing buffer content
	 * @param lenght the max number of elements to read
	 * @return the actual number of bytes read, can be less than length if there is no enough bytes to read
	 * 
	 * @throws IllegalArgumentException if buffer is null
	 * @throws IllegalArgumentException if offset < 0 or offset >= buffer.length
	 * @throws IllegalArgumentException if offset + length > buffer.length
	 * 
	 */
	public int read(byte[] target, int offset, int lenght);

	/**
	 * @param source
	 * @param offset
	 * @param lenght
	 */
	public void write(byte[] source, int offset, int lenght);

}
