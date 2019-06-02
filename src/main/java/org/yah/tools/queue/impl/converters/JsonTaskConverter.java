package org.yah.tools.queue.impl.converters;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.yah.tools.queue.impl.ObjectConverter;

import com.fasterxml.jackson.databind.ObjectMapper;

public final class JsonTaskConverter<D> implements ObjectConverter<D> {

	private final ObjectMapper objectMapper;

	private final Class<D> dataType;

	public JsonTaskConverter(ObjectMapper objectMapper, Class<D> dataType) {
		this.objectMapper = objectMapper;
		this.dataType = dataType;
	}

	@Override
	public D read(InputStream inputStream) throws IOException {
		return objectMapper.readValue(inputStream, dataType);
	}

	@Override
	public void write(D element, OutputStream outputStream) throws IOException {
		objectMapper.writeValue(outputStream, element);
	}

}
