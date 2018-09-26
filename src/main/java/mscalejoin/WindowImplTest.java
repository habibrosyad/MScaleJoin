package mscalejoin;

import mscalejoin.common.Probe;
import mscalejoin.common.Stream;
import mscalejoin.common.Tuple;
import mscalejoin.common.Window;
import mscalejoin.nlj.ProbeImpl;
import mscalejoin.nlj.WindowImpl;

import java.util.concurrent.atomic.AtomicInteger;

public class WindowImplTest {
    private static final int TEST_SIZE = 1000000;

    public static void main(String[] args) {
        // Window with 2 join attributes
        Window window = new WindowImpl(0);

        // Prepare tuples
        Tuple[] tuples = new Tuple[TEST_SIZE];
        String alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstu";
        for (int i = 0; i < TEST_SIZE; i++) {
            Object[] attributes = new Object[]{
                    (int) (Math.random() * TEST_SIZE),
                    alphabet.charAt((int) (Math.random() * alphabet.length())),
            };

            tuples[i] = new Tuple(i, Stream.R, attributes, 0);
        }

        // Measure insertion performance
        long start = System.nanoTime();

        for (Tuple tuple : tuples) {
            window.insert(tuple);
        }

        System.out.println("Insert finished in " + (System.nanoTime() - start) / 1000000 + "ms");

        // Measure probing performance
        start = System.nanoTime();

        Probe probe = new ProbeImpl(Stream.R, (a, b) -> a.getAttribute(1) == b.getAttribute(1));
        AtomicInteger sum = new AtomicInteger();
        for (char c : alphabet.toCharArray()) {
            Tuple tuple = new Tuple(0, Stream.R, new Object[]{1, c}, 0);
            window.probe(tuple, probe, (match) -> {
                sum.incrementAndGet();
            });
        }

        System.out.println("Probing finished in " + (System.nanoTime() - start) / 1000000 + "ms");

        assert sum.get() == TEST_SIZE : "Got " + sum + " instead of " + TEST_SIZE;

        // Measure expiration performance
        start = System.nanoTime();
        window.expire(TEST_SIZE);

        System.out.println("Expire finished in " + (System.nanoTime() - start) / 1000000 + "ms");
    }
}
