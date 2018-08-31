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

class Experiment {
    static void run(int numberOfConsumers, Plan plan) {
        int numberOfProducers = plan.getSources().size();

        ScaleGate sgin = new ScaleGateImpl(Config.SCALEGATE_MAXLEVELS, numberOfProducers, numberOfConsumers);
        ScaleGate sgout = new ScaleGateImpl(Config.SCALEGATE_MAXLEVELS, numberOfConsumers, 1);

        // + 1 for the stats thread
        AtomicInteger barrier = new AtomicInteger(numberOfProducers + numberOfConsumers + 1);

        Stats.run(barrier);

        Buffer buffer = new Buffer(numberOfConsumers);
        Thread[] producers = new Thread[numberOfProducers];
        Thread[] consumers = new Thread[numberOfConsumers];

        for (int i = 0; i < numberOfProducers; i++) {
            producers[i] = new Thread(new Producer(i, sgin, barrier, plan));
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

    private static class Producer implements Runnable {
        private final int id;
        private final AtomicInteger barrier;
        private final Plan plan;
        private final ScaleGate sgin;

        Producer(int id, ScaleGate sgin, AtomicInteger barrier, Plan plan) {
            this.id = id;
            this.sgin = sgin;
            this.barrier = barrier;
            this.plan = plan;
        }

        @Override
        public void run() {
            //Make sure the system is correctly initialized, i.e. one tuple from each producer exists
            barrier.decrementAndGet();
            while (barrier.get() != 0) ;

            long start = System.nanoTime();
            //Read local file of the stream
            Stream source = plan.getSources().get(id);
            String filename = "/Users/habib.rosyad/sandbox/mscalejoin-dataset/50/" + source + ".txt";
//            String filename = source + "";
            int timestamp = 0;
            try {
                Scanner scanner = new Scanner(new File(filename));

                while (scanner.hasNextLine()) {
                    Tuple newTuple = new Tuple(timestamp++, source,
                            plan.parse(source, scanner.nextLine().trim().split("\\s+")), 0);
                    sgin.addTuple(newTuple, id);
                }
            } catch (FileNotFoundException e) {
                System.out.println(filename + " not found");
            }

            // Add poison
            sgin.addTuple(new Tuple(timestamp, source, null, 0), id);

            System.out.println("Finish with " + id + " total " + timestamp + " in " + (System.nanoTime() - start) / 1000000 + "ms");
        }
    }
}
