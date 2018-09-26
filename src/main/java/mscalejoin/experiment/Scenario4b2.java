package mscalejoin.experiment;

import mscalejoin.common.Parser;
import mscalejoin.common.Stream;
import mscalejoin.shj.PlanImpl;
import mscalejoin.shj.ProbeImpl;

/**
 * Equi-join with 4 streams, join on distinct key and using SHJ
 */
class Scenario4b2 extends AbstractExperiment {
    Scenario4b2(long windowSize) {
        PlanImpl plan = new PlanImpl(windowSize);

        Parser parser = (s) -> new Integer[]{Integer.parseInt(s[0]), Integer.parseInt(s[1])};

        plan.addParser(Stream.R, parser);
        plan.addParser(Stream.S, parser);
        plan.addParser(Stream.T, parser);
        plan.addParser(Stream.U, parser);

        plan.addSource(Stream.R, 1,
                new ProbeImpl(Stream.S, 0, 0),
                new ProbeImpl(Stream.T, 0, 3),
                new ProbeImpl(Stream.U, 0, 5));
        plan.addSource(Stream.S, 2,
                new ProbeImpl(Stream.R, 0, 0),
                new ProbeImpl(Stream.T, 0, 1),
                new ProbeImpl(Stream.U, 0, 5));
        plan.addSource(Stream.T, 2,
                new ProbeImpl(Stream.S, 1, 0),
                new ProbeImpl(Stream.R, 0, 2),
                new ProbeImpl(Stream.U, 0, 1));
        plan.addSource(Stream.U, 1,
                new ProbeImpl(Stream.T, 1, 0),
                new ProbeImpl(Stream.S, 1, 2),
                new ProbeImpl(Stream.R, 0, 4));

        setPlan(plan);
    }
}
