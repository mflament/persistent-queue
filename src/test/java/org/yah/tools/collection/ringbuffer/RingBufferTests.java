package org.yah.tools.collection.ringbuffer;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;
import org.yah.tools.collection.ringbuffer.array.ArrayRingBufferTest;
import org.yah.tools.collection.ringbuffer.file.FileRingBufferTest;

@RunWith(Suite.class)
@SuiteClasses({ ArrayRingBufferTest.class, FileRingBufferTest.class })
public class RingBufferTests {

}
