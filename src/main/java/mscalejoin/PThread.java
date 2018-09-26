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

        while (!Stats.isDone()) {
            // Get new tuple
            join((Tuple) sgin.getNextReadyTuple(id));

            // Get intermediate results from buffer (transient)
            join(buffer.getNextReadyTuple(id, counter));
        }
    }

    private void join(Tuple tuple) {
        if (tuple != null) {
            Probe probe = plan.getNextProbe(tuple);
            Window window = windows.get(probe.getTarget());

            // Add tuple to its window only if it is a base tuple (not an intermediate)
            if (tuple.getProbeId() == 0) {
                tuple.setCounter(++counter);
                if (counter % numberOfThreads == id) {
                    windows.get(tuple.getSource()).insert(tuple);
                }
                // Increase number of processed tuples
                Stats.incrementProcessed();
            }

            // Expire invalid tuples in the target window
            window.expire(tuple.getTimestamp());

            // Probe targetWindow and join
//            for (Tuple match : window.probe(tuple, probe)) {
//                if (tuple.compareCounterTo(match) == 1) {
//                    // Merge tuple
//                    newTuple = tuple.merge(match);
//
//                    if (plan.hasNextProbe(tuple)) {
//                        // Place new tuple into buffer
//                        buffer.addTuple(newTuple);
//                    } else {
//                        // Period between the tuple enter the system until produce an output
//                        //Stats.addLatency(System.currentTimeMillis() - newTuple.getTimestamp());
//                        sgout.addTuple(newTuple, id);
//                    }
//                }
//            }

            window.probe(tuple, probe, (match) -> {
                if (tuple.compareCounterTo(match) == 1) {
                    // Merge tuple
                    Tuple newTuple = tuple.merge(match);

                    if (plan.hasNextProbe(tuple)) {
                        // Place new tuple into buffer
                        buffer.addTuple(newTuple);
                    } else {
                        // Period between the tuple enter the system until produce an output
                        //Stats.addLatency(System.currentTimeMillis() - newTuple.getTimestamp());
                        sgout.addTuple(newTuple, id);
                    }
                }
            });

            // Increase stats for comparison
            switch (plan.getMethod()) {
                case NLJ:
                    // NLJ scan all window contents
                    Stats.addComparison(window.size());
                    break;
                case SHJ:
                    // SHJ just probe once
                    Stats.incrementComparison();
                    break;
            }
        }
    }
}
