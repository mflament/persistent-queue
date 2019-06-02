package org.yah.tools.queue.impl.converters;

import java.io.IOException;
import java.io.InputStream;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;

import org.yah.tools.queue.impl.ObjectConverter;

/**
 * !!! Do not close input or output streams here, not our job !!!<br/>
 * So, we can not close the ObjectInputStream or ObjectOutputStream.
 */
public class SerializableObjectConverter<E extends Serializable> implements ObjectConverter<E> {

	private static final SerializableObjectConverter<Serializable> INSTANCE = new SerializableObjectConverter<>();

	@SuppressWarnings("unchecked")
	public static final <E extends Serializable> SerializableObjectConverter<E> instance() {
		return (SerializableObjectConverter<E>) INSTANCE;
	}

	public SerializableObjectConverter() {}

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
