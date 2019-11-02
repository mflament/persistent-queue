# persistent-queue

I used this to persist asynchronous tasks that I was generating at high rate. I needed to be able to stop current JRE and resume remaining tasks when on restart. This could be done by any message broker, but I just wanted to keep it local and simple.   

##  StreamRingBuffer

A ring buffer definition using an OutputStream and InputStream to write to or read from.

### ArrayRingBuffer
Implementation of StreamRingBuffer using in memory storage as byte array.

### FileRingBuffer
Implementation of StreamRingBuffer using java FileChannel to persist ring buffer content on disk.

## PersistentObjectQueue

Use the FileRingBuffer to create a persistent queue of objects. PersistentObjectQueues allow to aggregate a set of object queues as partitions, this provide efficient multithread usage.

## TaskQueuesExecutor

A asynchronous task executor using PersistentObjectQueue to store tasks.     

