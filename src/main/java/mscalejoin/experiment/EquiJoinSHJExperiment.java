package mscalejoin.experiment;

import mscalejoin.common.Parser;
import mscalejoin.common.Stream;
import mscalejoin.shj.PlanImpl;
import mscalejoin.shj.ProbeImpl;

public class EquiJoinSHJExperiment {
    static final int COMMON_ATTRIBUTE=0;
    public static void main(String[] args) {
        // Setup plan
        PlanImpl plan = new PlanImpl(Config.WINDOW_SIZE);

        Parser parser = (s) -> new Integer[]{Integer.parseInt(s[0]), Integer.parseInt(s[1])};

        plan.addParser(Stream.R, parser);
        plan.addParser(Stream.S, parser);
        plan.addParser(Stream.T, parser);
        plan.addParser(Stream.U, parser);

        plan.addSource(Stream.R, 1,
                new ProbeImpl(Stream.S, COMMON_ATTRIBUTE, COMMON_ATTRIBUTE),
                new ProbeImpl(Stream.T, COMMON_ATTRIBUTE, COMMON_ATTRIBUTE),
                new ProbeImpl(Stream.U, COMMON_ATTRIBUTE, COMMON_ATTRIBUTE)
        );

        plan.addSource(Stream.S, 1,
                new ProbeImpl(Stream.R, COMMON_ATTRIBUTE, COMMON_ATTRIBUTE),
                new ProbeImpl(Stream.T, COMMON_ATTRIBUTE, COMMON_ATTRIBUTE),
                new ProbeImpl(Stream.U, COMMON_ATTRIBUTE, COMMON_ATTRIBUTE)
        );

        plan.addSource(Stream.T, 1,
                new ProbeImpl(Stream.S, COMMON_ATTRIBUTE, COMMON_ATTRIBUTE),
                new ProbeImpl(Stream.R, COMMON_ATTRIBUTE, COMMON_ATTRIBUTE),
                new ProbeImpl(Stream.U, COMMON_ATTRIBUTE, COMMON_ATTRIBUTE)
        );

        plan.addSource(Stream.U, 1,
                new ProbeImpl(Stream.T, COMMON_ATTRIBUTE, COMMON_ATTRIBUTE),
                new ProbeImpl(Stream.S, COMMON_ATTRIBUTE, COMMON_ATTRIBUTE),
                new ProbeImpl(Stream.R, COMMON_ATTRIBUTE, COMMON_ATTRIBUTE)
        );

        // Run the experiment
//        Experiment.run(Integer.parseInt(args[0]), plan);
        Experiment.run(2, plan);
    }
}
