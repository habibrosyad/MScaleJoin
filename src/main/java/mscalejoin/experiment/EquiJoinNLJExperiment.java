package mscalejoin.experiment;

import mscalejoin.common.Parser;
import mscalejoin.common.Stream;
import mscalejoin.nlj.PlanImpl;
import mscalejoin.nlj.Predicate;
import mscalejoin.nlj.ProbeImpl;

public class EquiJoinNLJExperiment {
    public static void main(String[] args) {
        // Setup plan
        PlanImpl plan = new PlanImpl(Config.WINDOW_SIZE);

        Predicate predicate = (a, b) -> (int) a.getAttribute(0) == (int) b.getAttribute(0);
        Parser parser = (s) -> new Integer[]{Integer.parseInt(s[0]), Integer.parseInt(s[1])};

        plan.addParser(Stream.R, parser);
        plan.addParser(Stream.S, parser);
        plan.addParser(Stream.T, parser);
        plan.addParser(Stream.U, parser);

        plan.addSource(Stream.R,
                new ProbeImpl(Stream.S, predicate),
                new ProbeImpl(Stream.T, predicate),
                new ProbeImpl(Stream.U, predicate)
        );

        plan.addSource(Stream.S,
                new ProbeImpl(Stream.R, predicate),
                new ProbeImpl(Stream.T, predicate),
                new ProbeImpl(Stream.U, predicate)
        );

        plan.addSource(Stream.T,
                new ProbeImpl(Stream.S, predicate),
                new ProbeImpl(Stream.R, predicate),
                new ProbeImpl(Stream.U, predicate)
        );

        plan.addSource(Stream.U,
                new ProbeImpl(Stream.T, predicate),
                new ProbeImpl(Stream.S, predicate),
                new ProbeImpl(Stream.R, predicate)
        );

        // Run the experiment
        Experiment.run(Integer.parseInt(args[0]), plan);
    }
}
