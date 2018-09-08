package mscalejoin;

import mscalejoin.common.*;
import mscalejoin.experiment.Stats;
import scalegate.ScaleGate;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class PThread implements Runnable {
    private final ScaleGate sgin;
    private final ScaleGate sgout;
    private final Buffer buffer;
    private final int id;
    private final AtomicInteger barrier;
    private final Map<Stream, Window> windows;
    private final Plan plan;
    private final int numberOfThreads;
    private int counter;

    public PThread(int id, ScaleGate sgin, ScaleGate sgout, Buffer buffer,
                   AtomicInteger barrier, Plan plan, int numberOfThreads) {
        this.sgin = sgin;
        this.sgout = sgout;
        this.buffer = buffer;
        this.id = id;
        this.barrier = barrier;
        this.plan = plan;
        this.windows = plan.createWindows();
        this.numberOfThreads = numberOfThreads;
    }

    @Override
    public void run() {
        // Make sure all PThreads are ready before doing anything
        barrier.decrementAndGet();
        while (barrier.get() != 0) ;

//        long start = System.nanoTime(); // For SHJ experiment only
        while (!Stats.finished.get()) {
            // Get new tuple
            join((Tuple) sgin.getNextReadyTuple(id));

            // Get intermediate results from buffer (transient)
            join(buffer.getNextReadyTuple(id, counter));

            // For SHJ experiment only
//            if (plan.getMethod() == Method.SHJ &&
//                    plan.getExpectedOutput() > 0 &&
//                    Stats.output.get() == plan.getExpectedOutput()) {
//                System.out.println("Finished in " + (System.nanoTime() - start) / 1000000 + "ms");
//                break;
//            }
        }
    }

    private void join(Tuple tuple) {
        if (tuple != null) {
            Probe probe = plan.getNextProbe(tuple);
            Window window = windows.get(probe.getTarget());
            Tuple newTuple;

            // Add tuple to its window only if it is a base tuple (not an intermediate)
            if (tuple.getProbeId() == 0) {
                tuple.setCounter(++counter);
                if (counter % numberOfThreads == id) {
                    windows.get(tuple.getSource()).insert(tuple);
                }
            }

            // Expire invalid tuples in the target window
            window.expire(tuple.getTimestamp());

            // Increase stats for comparison
            switch (plan.getMethod()) {
                case NLJ:
                    Stats.comparison.addAndGet(window.size());
                    break; // NLJ scan all window contents
                case SHJ:
                    Stats.comparison.incrementAndGet();
                    break; // SHJ just probe once
            }

            // ProbeImpl targetWindow and join
            for (Tuple match : window.probe(tuple, probe)) {
                if (tuple.compareCounterTo(match) == 1) {
                    // Merge tuple
                    newTuple = tuple.merge(match);

                    if (plan.hasNextProbe(tuple)) {
                        // Place new tuple into buffer
                        buffer.addTuple(newTuple);
                    } else {
                        Stats.output.incrementAndGet(); // Increase stats
                        //sgout.addTuple(newTuple, id);
                    }
                }
            }
        }
    }
}
