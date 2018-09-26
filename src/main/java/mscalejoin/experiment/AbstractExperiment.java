package mscalejoin.experiment;

import mscalejoin.Buffer;
import mscalejoin.PThread;
import mscalejoin.common.Plan;
import mscalejoin.common.Stream;
import mscalejoin.common.Tuple;
import scalegate.ScaleGate;
import scalegate.ScaleGateImpl;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;

abstract class AbstractExperiment {
    private String path;
    private Plan plan;
    private int rate;
    private AtomicInteger barrier;
    private ScaleGate sgin, sgout;

    void setPlan(Plan plan) {
        this.plan = plan;
    }

    void setPath(String path) {
        this.path = path;
    }

    void setRate(int rate) {
        this.rate = rate;
    }

    void run(int numberOfConsumers) {
        int numberOfProducers = plan.getSources().size();

        sgin = new ScaleGateImpl(10, numberOfProducers, numberOfConsumers);
        sgout = new ScaleGateImpl(10, numberOfConsumers, 1);

        // + 1 for the stats thread and + 1 for the output thread
        barrier = new AtomicInteger(numberOfProducers + numberOfConsumers + 2);

        Stats.run(barrier, numberOfConsumers, plan.getWindowSize(), rate);

        new Thread(new OutputReader()).start();

        Buffer buffer = new Buffer(numberOfConsumers);
        Thread[] producers = new Thread[numberOfProducers];
        Thread[] consumers = new Thread[numberOfConsumers];

        for (int i = 0; i < numberOfProducers; i++) {
            producers[i] = new Thread(new Producer(i));
        }

        for (int i = 0; i < numberOfConsumers; i++) {
            consumers[i] = new Thread(new PThread(i, sgin, sgout, buffer, barrier, plan, numberOfConsumers));
        }

        for (int i = 0; i < numberOfProducers; i++) {
            producers[i].start();
        }

        for (int i = 0; i < numberOfConsumers; i++) {
            consumers[i].start();
        }

        try {

            for (Thread t : consumers) {
                t.join();
            }

            for (Thread t : producers) {
                t.join();
            }
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * Produce stream from local data
     */
    private class Producer implements Runnable {
        private final int id;

        Producer(int id) {
            this.id = id;
        }

        @Override
        public void run() {
            // Make sure the system is correctly initialized, i.e. one tuple from each producer exists
            barrier.decrementAndGet();
            while (barrier.get() != 0) ;

            // For rate control
            long before = 0;
            float ahead = 0;

            // Read local file of the stream
            Stream source = plan.getSources().get(id);
            String filename = path + source.toString();

            try {
                Scanner scanner = new Scanner(new File(filename));

                while (scanner.hasNextLine() && !Stats.isDone()) {
                    Tuple newTuple = new Tuple(System.currentTimeMillis(), source,
                            plan.parse(source, scanner.nextLine().trim().split("\\s+")), 0);
                    sgin.addTuple(newTuple, id);

                    // Rate control
                    long now = System.nanoTime() / 1000000L;
                    if (before != 0) {
                        ahead -= (float) (now - before) / 1000 * rate - 1;
                        if (ahead > 0) {
                            Thread.sleep((long) (ahead / rate * 1000));
                        }
                    }
                    before = now;
                }
            } catch (FileNotFoundException e) {
                System.out.println(filename + " not found");
            } catch (InterruptedException e) {
                System.out.println(e.getMessage());
            }

            // Add poison
            sgin.addTuple(new Tuple(System.currentTimeMillis(), source, null, 0), id);
        }
    }

    private class OutputReader implements Runnable {
        @Override
        public void run() {
            barrier.decrementAndGet();
            while (barrier.get() != 0) ;

            while (!Stats.isDone()) {
                Tuple tuple = (Tuple) sgout.getNextReadyTuple(0);
                if (tuple != null) {
                    // Period between the tuple enter the system until produce an output
                    Stats.addLatency(System.currentTimeMillis() - tuple.getTimestamp());
                }
            }
        }
    }
}
