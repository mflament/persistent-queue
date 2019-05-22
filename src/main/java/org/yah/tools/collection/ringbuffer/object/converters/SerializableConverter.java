package org.yah.tools.collection.ringbuffer.object.converters;

import java.io.IOException;
import java.io.InputStream;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;

import org.yah.tools.collection.ringbuffer.object.ObjectConverter;

/**
 * !!! Do not close input or output streams here, not out job !!!<br/>
 * So, we can not close the ObjectInputStream or ObjectOutputStream.
 */
public class SerializableConverter<E extends Serializable> implements ObjectConverter<E> {

	private static final SerializableConverter<Serializable> INSTANCE = new SerializableConverter<>();

	@SuppressWarnings("unchecked")
	public static final <E extends Serializable> SerializableConverter<E> instance() {
		return (SerializableConverter<E>) INSTANCE;
	}

	public SerializableConverter() {}

	@Override
	@SuppressWarnings("unchecked")
	public E read(InputStream inputStream) throws IOException {
		ObjectInputStream ois = new ObjectInputStream(inputStream);
		try {
			return (E) ois.readObject();
		} catch (ClassNotFoundException | ClassCastException e) {
			throw new InvalidObjectException(e.getMessage());
		}
	}

	@Override
	public void write(E element, OutputStream outputStream) throws IOException {
		ObjectOutputStream oos = new ObjectOutputStream(outputStream);
		oos.writeObject(element);
		oos.flush();
	}

}
