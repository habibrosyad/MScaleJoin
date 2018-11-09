package mscalejoin.experiment;

import mscalejoin.common.Stream;
import mscalejoin.nlj.PlanImpl;
import mscalejoin.nlj.ProbeImpl;

/**
 * # Band-join with 4 streams
 */
class Scenario3 extends AbstractExperiment {
    Scenario3(long windowSize) {
        PlanImpl plan = new PlanImpl(windowSize);

        plan.addParser(Stream.R, (s) -> new Object[]{Integer.parseInt(s[0]), Float.parseFloat(s[1])});

        plan.addParser(Stream.S, (s) -> new Object[]{
                Integer.parseInt(s[0]), Float.parseFloat(s[1]),
                Double.parseDouble(s[2]), Integer.parseInt(s[3])});

        plan.addParser(Stream.T, (s) -> new Object[]{
                Double.parseDouble(s[0]), Integer.parseInt(s[1]),
                Float.parseFloat(s[2]), Integer.parseInt(s[3])});

        plan.addParser(Stream.U, (s) -> new Object[]{Float.parseFloat(s[0]), Integer.parseInt(s[1])});

        plan.addSource(Stream.R,
                new ProbeImpl(Stream.S, (a, b) -> (int) a.getAttribute(0) >= (int) b.getAttribute(0) - 10 &&
                        (int) a.getAttribute(0) <= (int) b.getAttribute(0) + 10 &&
                        (float) a.getAttribute(1) >= (float) b.getAttribute(1) - 10 &&
                        (float) a.getAttribute(1) <= (float) b.getAttribute(1) + 10),
                new ProbeImpl(Stream.T, (a, b) -> (double) a.getAttribute(4) >= (double) b.getAttribute(0) - 10 &&
                        (double) a.getAttribute(4) <= (double) b.getAttribute(0) + 10 &&
                        (int) a.getAttribute(5) >= (int) b.getAttribute(1) - 10 &&
                        (int) a.getAttribute(5) <= (int) b.getAttribute(1) + 10),
                new ProbeImpl(Stream.U, (a,b) -> (float) a.getAttribute(8) >= (float) b.getAttribute(0) - 10 &&
                        (float) a.getAttribute(8) <= (float) b.getAttribute(0) + 10 &&
                        (int) a.getAttribute(9) >= (int) b.getAttribute(1) - 10 &&
                        (int) a.getAttribute(9) <= (int) b.getAttribute(1) + 10)
        );

        plan.addSource(Stream.S,
                new ProbeImpl(Stream.R, (a, b) -> (int) a.getAttribute(0) >= (int) b.getAttribute(0) - 10 &&
                        (int) a.getAttribute(0) <= (int) b.getAttribute(0) + 10 &&
                        (float) a.getAttribute(1) >= (float) b.getAttribute(1) - 10 &&
                        (float) a.getAttribute(1) <= (float) b.getAttribute(1) + 10),
                new ProbeImpl(Stream.T, (a, b) -> (double) a.getAttribute(2) >= (double) b.getAttribute(0) - 10 &&
                        (double) a.getAttribute(2) <= (double) b.getAttribute(0) + 10 &&
                        (int) a.getAttribute(3) >= (int) b.getAttribute(1) - 10 &&
                        (int) a.getAttribute(3) <= (int) b.getAttribute(1) + 10),
                new ProbeImpl(Stream.U, (a,b) -> (float) a.getAttribute(8) >= (float) b.getAttribute(0) - 10 &&
                        (float) a.getAttribute(8) <= (float) b.getAttribute(0) + 10 &&
                        (int) a.getAttribute(9) >= (int) b.getAttribute(1) - 10 &&
                        (int) a.getAttribute(9) <= (int) b.getAttribute(1) + 10)
        );

        plan.addSource(Stream.T,
                new ProbeImpl(Stream.S, (a, b) -> (double) a.getAttribute(0) >= (double) b.getAttribute(2) - 10 &&
                        (double) a.getAttribute(0) <= (double) b.getAttribute(2) + 10 &&
                        (int) a.getAttribute(1) >= (int) b.getAttribute(3) - 10 &&
                        (int) a.getAttribute(1) <= (int) b.getAttribute(3) + 10),
                new ProbeImpl(Stream.R, (a, b) -> (int) a.getAttribute(4) >= (int) b.getAttribute(0) - 10 &&
                        (int) a.getAttribute(4) <= (int) b.getAttribute(0) + 10 &&
                        (float) a.getAttribute(5) >= (float) b.getAttribute(1) - 10 &&
                        (float) a.getAttribute(5) <= (float) b.getAttribute(1) + 10),
                new ProbeImpl(Stream.U, (a,b) -> (float) a.getAttribute(2) >= (float) b.getAttribute(0) - 10 &&
                        (float) a.getAttribute(2) <= (float) b.getAttribute(0) + 10 &&
                        (int) a.getAttribute(3) >= (int) b.getAttribute(1) - 10 &&
                        (int) a.getAttribute(3) <= (int) b.getAttribute(1) + 10)
        );

        plan.addSource(Stream.U,
                new ProbeImpl(Stream.T, (a, b) -> (float) a.getAttribute(0) >= (float) b.getAttribute(2) - 10 &&
                        (float) a.getAttribute(0) <= (float) b.getAttribute(2) + 10 &&
                        (int) a.getAttribute(1) >= (int) b.getAttribute(3) - 10 &&
                        (int) a.getAttribute(1) <= (int) b.getAttribute(3) + 10),
                new ProbeImpl(Stream.S, (a, b) -> (double) a.getAttribute(2) >= (double) b.getAttribute(2) - 10 &&
                        (double) a.getAttribute(2) <= (double) b.getAttribute(2) + 10 &&
                        (int) a.getAttribute(3) >= (int) b.getAttribute(3) - 10 &&
                        (int) a.getAttribute(3) <= (int) b.getAttribute(3) + 10),
                new ProbeImpl(Stream.R, (a, b) -> (int) a.getAttribute(6) >= (int) b.getAttribute(0) - 10 &&
                        (int) a.getAttribute(6) <= (int) b.getAttribute(0) + 10 &&
                        (float) a.getAttribute(7) >= (float) b.getAttribute(1) - 10 &&
                        (float) a.getAttribute(7) <= (float) b.getAttribute(1) + 10)
        );

        setPlan(plan);
    }
}
