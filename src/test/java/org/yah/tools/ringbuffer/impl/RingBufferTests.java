package org.yah.tools.ringbuffer.impl;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;
import org.yah.tools.ringbuffer.impl.array.ArrayRingBufferTest;
import org.yah.tools.ringbuffer.impl.file.FileRingBufferTest;

@RunWith(Suite.class)
@SuiteClasses({ ArrayRingBufferTest.class, FileRingBufferTest.class })
public class RingBufferTests {

}
