package org.yah.tools.queue.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface ObjectConverter<E> {

	E read(InputStream inputStream) throws IOException;

	void write(E element, OutputStream outputStream) throws IOException;

}