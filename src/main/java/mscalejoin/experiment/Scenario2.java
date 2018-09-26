package mscalejoin.experiment;

import mscalejoin.common.Stream;
import mscalejoin.nlj.PlanImpl;
import mscalejoin.nlj.ProbeImpl;

/**
 * Band-join with 3 streams
 */
class Scenario2 extends AbstractExperiment {
    Scenario2(long windowSize) {
        PlanImpl plan = new PlanImpl(windowSize);

        plan.addParser(Stream.R, (s) -> new Object[]{Integer.parseInt(s[0]), Float.parseFloat(s[1])});

        plan.addParser(Stream.S, (s) -> new Object[]{
                Integer.parseInt(s[0]), Float.parseFloat(s[1]),
                Double.parseDouble(s[2]), Integer.parseInt(s[3])});

        plan.addParser(Stream.T, (s) -> new Object[]{Double.parseDouble(s[0]), Integer.parseInt(s[1])});

        plan.addSource(Stream.R,
                new ProbeImpl(Stream.S, (a, b) -> (int) a.getAttribute(0) >= (int) b.getAttribute(0) - 10 &&
                        (int) a.getAttribute(0) <= (int) b.getAttribute(0) + 10 &&
                        (float) a.getAttribute(1) >= (float) b.getAttribute(1) - 10 &&
                        (float) a.getAttribute(1) <= (float) b.getAttribute(1) + 10),
                new ProbeImpl(Stream.T, (a, b) -> (double) a.getAttribute(4) >= (double) b.getAttribute(0) - 10 &&
                        (double) a.getAttribute(4) <= (double) b.getAttribute(0) + 10 &&
                        (int) a.getAttribute(5) >= (int) b.getAttribute(1) - 10 &&
                        (int) a.getAttribute(5) <= (int) b.getAttribute(1) + 10)
        );

        plan.addSource(Stream.S,
                new ProbeImpl(Stream.R, (a, b) -> (int) a.getAttribute(0) >= (int) b.getAttribute(0) - 10 &&
                        (int) a.getAttribute(0) <= (int) b.getAttribute(0) + 10 &&
                        (float) a.getAttribute(1) >= (float) b.getAttribute(1) - 10 &&
                        (float) a.getAttribute(1) <= (float) b.getAttribute(1) + 10),
                new ProbeImpl(Stream.T, (a, b) -> (double) a.getAttribute(2) >= (double) b.getAttribute(0) - 10 &&
                        (double) a.getAttribute(2) <= (double) b.getAttribute(0) + 10 &&
                        (int) a.getAttribute(3) >= (int) b.getAttribute(1) - 10 &&
                        (int) a.getAttribute(3) <= (int) b.getAttribute(1) + 10)
        );

        plan.addSource(Stream.T,
                new ProbeImpl(Stream.S, (a, b) -> (double) a.getAttribute(0) >= (double) b.getAttribute(2) - 10 &&
                        (double) a.getAttribute(0) <= (double) b.getAttribute(2) + 10 &&
                        (int) a.getAttribute(1) >= (int) b.getAttribute(3) - 10 &&
                        (int) a.getAttribute(1) <= (int) b.getAttribute(3) + 10),
                new ProbeImpl(Stream.R, (a, b) -> (int) a.getAttribute(2) >= (int) b.getAttribute(0) - 10 &&
                        (int) a.getAttribute(2) <= (int) b.getAttribute(0) + 10 &&
                        (float) a.getAttribute(3) >= (float) b.getAttribute(1) - 10 &&
                        (float) a.getAttribute(3) <= (float) b.getAttribute(1) + 10)
        );

        setPlan(plan);
    }
}
