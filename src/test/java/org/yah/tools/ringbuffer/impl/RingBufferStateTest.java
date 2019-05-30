package org.yah.tools.ringbuffer.impl;

import static org.junit.Assert.*;

import org.junit.Test;

public class RingBufferStateTest {

	@Test
	public void testWritePosition() {
		RingBufferState state = new RingBufferState(0, 0, 64, 32);
		assertEquals(32, state.writePosition());

		state = new RingBufferState(32, 0, 64, 32);
		assertEquals(0, state.writePosition());

		state = new RingBufferState(63, 0, 64, 1);
		assertEquals(0, state.writePosition());

		state = new RingBufferState(63, 0, 64, 0);
		assertEquals(63, state.writePosition());
	}

	@Test
	public void testIncrementSize() {
		RingBufferState state = new RingBufferState(0, 0, 64, 32);
		RingBufferState newState = state.incrementSize(32);
		assertEquals(state.position(), newState.position());
		assertEquals(state.size() + 32, newState.size());
	}

	@Test
	public void testAvailableToRead() {
		RingBufferState state = new RingBufferState(0, 1, 64, 0);
		assertEquals(-1, state.availableToRead(new RingPosition(0, 0, 64)));

		state = new RingBufferState(0, 0, 64, 0);
		assertEquals(0, state.availableToRead(new RingPosition(0, 0, 64)));

		state = new RingBufferState(0, 0, 64, 32);
		assertEquals(32, state.availableToRead(new RingPosition(0, 0, 64)));

		state = new RingBufferState(0, 0, 64, 32);
		assertEquals(16, state.availableToRead(new RingPosition(16, 0, 64)));

		state = new RingBufferState(48, 0, 64, 32);
		assertEquals(-1, state.availableToRead(new RingPosition(47, 0, 64)));

		state = new RingBufferState(48, 0, 64, 32);
		assertEquals(32, state.availableToRead(new RingPosition(48, 0, 64)));

		state = new RingBufferState(48, 0, 64, 32);
		assertEquals(16, state.availableToRead(new RingPosition(0, 1, 64)));

		state = new RingBufferState(48, 0, 64, 32);
		assertEquals(1, state.availableToRead(new RingPosition(15, 1, 64)));

		state = new RingBufferState(48, 0, 64, 32);
		assertEquals(0, state.availableToRead(new RingPosition(16, 1, 64)));

		state = new RingBufferState(48, 0, 64, 32);
		assertEquals(0, state.availableToRead(new RingPosition(32, 1, 64)));
	}

	@Test
	public void testUpdateCapacity() {
		RingBufferState fromState = new RingBufferState(0, 0, 64, 32);
		RingBufferState state = new RingBufferState(0, 0, 64, 32);
		RingBufferState newState = state.updateCapacity(128, fromState);
		assertEquals(32, newState.size());
		assertEquals(128, newState.capacity());
	}

	@Test
	public void testWithCapacity() {
		RingBufferState fromState = new RingBufferState(0, 0, 64, 32);
		RingBufferState newState = fromState.withCapacity(128);
		assertEquals(128, newState.capacity());
		assertEquals(32, newState.size());
	}

	@Test
	public void testWrapped() {
		RingBufferState state = new RingBufferState(0, 0, 64, 0);
		assertFalse(state.wrapped());
		
		state = new RingBufferState(0, 0, 64, 32);
		assertFalse(state.wrapped());
		
		state = new RingBufferState(32, 0, 64, 32);
		assertFalse(state.wrapped());

		state = new RingBufferState(32, 0, 64, 33);
		assertTrue(state.wrapped());

		state = new RingBufferState(63, 0, 64, 1);
		assertFalse(state.wrapped());

		state = new RingBufferState(0, 0, 64, 64);
		assertFalse(state.wrapped());

		state = new RingBufferState(1, 0, 64, 64);
		assertTrue(state.wrapped());

		state = new RingBufferState(63, 0, 64, 64);
		assertTrue(state.wrapped());

		state = new RingBufferState(32, 0, 64, 64);
		assertTrue(state.wrapped());
	}

}
