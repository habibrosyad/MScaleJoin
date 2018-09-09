package mscalejoin;

import mscalejoin.common.Stream;
import mscalejoin.common.Tuple;

import java.util.concurrent.atomic.AtomicInteger;

public class BufferTest {
    private static final int TEST_SIZE = 1000000;
    private static final int NTHREADS = 4;
    private static Buffer buffer;
    private static AtomicInteger barrier;

    public static void main(String[] args) throws InterruptedException {
        buffer = new Buffer(NTHREADS);
        barrier = new AtomicInteger(NTHREADS * 2);

        System.out.println(" Hello world! Starting threads...");

        Thread[] producers = new Thread[NTHREADS];
        Thread[] consumers = new Thread[NTHREADS];

        for (int i = 0; i < NTHREADS; i++) {
            producers[i] = new Thread(new Producer(i));
            consumers[i] = new Thread(new Consumer(i));
        }

        for (int i = 0; i < NTHREADS; i++) {
            producers[i].start();
            consumers[i].start();
        }

        for (Thread t : producers) {
            t.join();
        }

        for (Thread t : consumers) {
            t.join();
        }
    }

    private static class Producer implements Runnable {
        int id;

        Producer(int id) {
            this.id = id;
        }

        @Override
        public void run() {
            //Synch with the rest
            barrier.getAndDecrement();
            while (barrier.get() != 0) ;

            for (int i = 0; i < TEST_SIZE; i++) {
                Tuple foo = new Tuple(i, Stream.R, null, 0);
                buffer.addTuple(foo);
            }

            System.out.println("Producer thread " + id + " done");
        }

    }

    private static class Consumer implements Runnable {
        int id;

        Consumer(int id) {
            this.id = id;
        }

        @Override
        public void run() {
            // Make sure the system is correctly initialized, i.e. one tuple from each producer exists
            barrier.getAndDecrement();
            while (barrier.get() != 0) ;

            int sum = 0;

            while (true) {
                Tuple cur = buffer.getNextReadyTuple(id, sum);
                if (cur != null) {
                    sum++;

                    if (sum == NTHREADS * TEST_SIZE) {//last tuple, break
                        break;
                    }
                }
            }

            System.out.println("Consumer thread " + id + " done");
        }

    }
}
