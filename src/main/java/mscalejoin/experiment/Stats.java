package mscalejoin.experiment;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Collecting stats for measuring performance during an experiment.
 */
public class Stats {
    public static final AtomicLong initialResponse = new AtomicLong(); // In nanos
    public static final AtomicLong comparison = new AtomicLong();
    public static final AtomicLong output = new AtomicLong();
    public static final AtomicBoolean finished = new AtomicBoolean();

    static void run(AtomicInteger barrier) {
        new Thread(() -> {
            long start, elapsed;

            barrier.decrementAndGet();
            while (barrier.get() != 0) ;

            start = System.nanoTime();

            try {
                Thread.sleep(20000); // 20 seconds

                // Set to finish, kill all threads
                finished.set(true);
                elapsed = (System.nanoTime() - start) / 1000000000; // In seconds

                // Print report
//                System.out.println("ELAPSED=" + elapsed + "s");
//                System.out.println("INITIAL_RESPONSE=" + (initialResponse.get() - start) / 1000000 + "ms");
//                System.out.println("OUTPUT_TOTAL=" + output.get());
//                System.out.println("OUTPUT/s=" + output.get() / elapsed);
//                System.out.println("COMPARISON_TOTAL=" + comparison.get());
//                System.out.println("COMPARISON/s=" + comparison.get() / elapsed);
//                System.out.println();

                // Produce CSV like data to ease the analysis
                // [elapsed_s, initial_response_ms, output_total, output_s, comparison_total, comparison_s]
                System.out.println(elapsed + "," +
                        (initialResponse.get() - start) / 1000000 + "," +
                        output.get() + "," +
                        output.get() / elapsed + "," +
                        comparison.get() + "," +
                        comparison.get() / elapsed
                );
            } catch (InterruptedException e) {
                System.out.println(e.getMessage());
            }
        }).start();
    }
}
