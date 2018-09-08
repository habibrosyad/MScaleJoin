package mscalejoin.experiment;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class Stats {
    private static final long WAIT_TIME = 20000;
    public static final AtomicLong comparison = new AtomicLong();
    public static final AtomicLong output = new AtomicLong();

    static void run(AtomicInteger barrier) {
        new Thread(() -> {
            barrier.decrementAndGet();
            while (barrier.get() != 0) ;

            try {
                Thread.sleep(WAIT_TIME);

                // Print report
                System.out.println("OUTPUT_TOTAL=" + output.get());
                System.out.println("OUTPUT/s=" + output.get()/(WAIT_TIME/1000));
                System.out.println("COMPARISON_TOTAL=" + comparison.get());
                System.out.println("COMPARISON/s=" + comparison.get()/(WAIT_TIME/1000));
                System.out.println();
            } catch (InterruptedException e) {
                System.out.println(e.getMessage());
            }
        }).start();
    }
}
