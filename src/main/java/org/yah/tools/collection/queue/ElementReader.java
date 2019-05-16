package org.yah.tools.collection.queue;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface ElementReader<E> {

	E read(InputStream inputStream) throws IOException;

	void write(E element, OutputStream outputStream) throws IOException;

}