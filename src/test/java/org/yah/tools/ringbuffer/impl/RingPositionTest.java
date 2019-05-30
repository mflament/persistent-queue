package org.yah.tools.ringbuffer.impl;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import org.junit.Test;
import org.yah.tools.ringbuffer.impl.RingPosition.RingAction;

public class RingPositionTest {

	private static final int CAPACITY = 64;

	@Test
	public void testWrap() {
		RingPosition pos = new RingPosition(0, 0, 64);
		assertEquals(pos.wrap(0), 0);
		assertEquals(pos.wrap(1), 1);
		assertEquals(pos.wrap(CAPACITY), 0);
		assertEquals(pos.wrap(CAPACITY + 1), 1);
		assertEquals(pos.wrap(-1), CAPACITY - 1);
	}

	@Test
	public void testAfter() {
		assertFalse(new RingPosition(0, 0, 64).after(new RingPosition(0, 0, 64)));
		assertFalse(new RingPosition(0, 0, 64).after(new RingPosition(1, 0, 64)));
		assertTrue(new RingPosition(1, 0, 64).after(new RingPosition(0, 0, 64)));
		assertFalse(new RingPosition(1, 0, 64).after(new RingPosition(0, 1, 64)));
	}

	@Test
	public void testAdvance() {
		RingPosition pos = new RingPosition(0, 0, 64);
		RingPosition newPos = pos.advance(32);
		assertEquals(32, newPos.position());
		assertEquals(0, newPos.cycle());
		assertEquals(64, newPos.capacity());

		newPos = newPos.advance(16);
		assertEquals(48, newPos.position());
		assertEquals(0, newPos.cycle());
		assertEquals(64, newPos.capacity());

		newPos = newPos.advance(16);
		assertEquals(0, newPos.position());
		assertEquals(1, newPos.cycle());
		assertEquals(64, newPos.capacity());

		newPos = newPos.advance(16);
		assertEquals(16, newPos.position());
		assertEquals(1, newPos.cycle());
		assertEquals(64, newPos.capacity());

		newPos = newPos.advance(-32);
		assertEquals(48, newPos.position());
		assertEquals(0, newPos.cycle());
		assertEquals(64, newPos.capacity());
	}

	@Test
	public void testWithCapacity() {
		RingPosition pos = new RingPosition(16, 3, 32).withCapacity(64);
		assertEquals(16, pos.position());
		assertEquals(3, pos.cycle());
		assertEquals(64, pos.capacity());
	}

	@Test
	public void testSubstract() {
		RingPosition a = new RingPosition(0, 0, 64);
		RingPosition b = new RingPosition(0, 0, 64);
		assertEquals(0, a.substract(b));
		assertEquals(0, b.substract(a));

		a = new RingPosition(0, 0, 64);
		b = new RingPosition(16, 0, 64);
		assertEquals(-16, a.substract(b));
		assertEquals(16, b.substract(a));

		a = new RingPosition(0, 0, 64);
		b = new RingPosition(0, 1, 64);
		assertEquals(-64, a.substract(b));
		assertEquals(64, b.substract(a));

		a = new RingPosition(63, 0, 64);
		b = new RingPosition(0, 1, 64);
		assertEquals(-1, a.substract(b));
		assertEquals(1, b.substract(a));

		a = new RingPosition(0, 0, 64);
		b = new RingPosition(0, 2, 64);
		assertEquals(-128, a.substract(b));
		assertEquals(128, b.substract(a));
	}

	@Test
	public void testUpdateCapacity() {
		RingBufferState fromState = new RingBufferState(0, 0, 64, 0);
		RingPosition pos = new RingPosition(0, 0, 64);
		RingPosition newPos = pos.updateCapacity(128, fromState);
		assertEquals(0, newPos.position());
		assertEquals(0, newPos.cycle());
		assertEquals(128, newPos.capacity());

		fromState = new RingBufferState(0, 0, 64, 32);
		pos = new RingPosition(0, 0, 64);
		newPos = pos.updateCapacity(128, fromState);
		assertEquals(0, newPos.position());
		assertEquals(0, newPos.cycle());
		assertEquals(128, newPos.capacity());

		fromState = new RingBufferState(48, 0, 64, 32);
		pos = new RingPosition(48, 0, 64);
		newPos = pos.updateCapacity(128, fromState);
		assertEquals(48, newPos.position());
		assertEquals(0, newPos.cycle());
		assertEquals(128, newPos.capacity());

		fromState = new RingBufferState(48, 0, 64, 32);
		pos = new RingPosition(0, 1, 64);
		newPos = pos.updateCapacity(128, fromState);
		assertEquals(64, newPos.position());
		assertEquals(0, newPos.cycle());
		assertEquals(128, newPos.capacity());

		fromState = new RingBufferState(48, 0, 64, 32);
		pos = new RingPosition(16, 1, 64);
		newPos = pos.updateCapacity(128, fromState);
		assertEquals(80, newPos.position());
		assertEquals(0, newPos.cycle());
		assertEquals(128, newPos.capacity());

		fromState = new RingBufferState(48, 0, 64, 32);
		pos = new RingPosition(16, 0, 64);
		newPos = pos.updateCapacity(128, fromState);
		assertEquals(16, newPos.position());
		assertEquals(0, newPos.cycle());
		assertEquals(128, newPos.capacity());
	}

	@Test
	public void testExecute() throws IOException {
		RingPosition position = new RingPosition(0, 0, 64);

		RingAction action = mock(RingAction.class);
		position.execute(32, action);
		verify(action, only()).apply(0, 32, 0);

		position = new RingPosition(32, 0, 64);
		action = mock(RingAction.class);
		position.execute(32, action);
		verify(action, only()).apply(32, 32, 0);

		position = new RingPosition(32, 0, 64);
		action = mock(RingAction.class);
		position.execute(64, action);
		verify(action, times(1)).apply(32, 32, 0);
		verify(action, times(1)).apply(0, 32, 32);

		position = new RingPosition(63, 0, 64);
		action = mock(RingAction.class);
		position.execute(1, action);
		verify(action, only()).apply(63, 1, 0);

		position = new RingPosition(63, 0, 64);
		action = mock(RingAction.class);
		position.execute(1, action);
		verify(action, times(1)).apply(63, 1, 0);
	}

}
