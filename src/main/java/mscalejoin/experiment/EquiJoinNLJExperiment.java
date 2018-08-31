package mscalejoin.experiment;

import mscalejoin.common.Parser;
import mscalejoin.common.Probe;
import mscalejoin.common.Stream;
import mscalejoin.nlj.PlanImpl;
import mscalejoin.nlj.Predicate;
import mscalejoin.nlj.ProbeImpl;

public class EquiJoinNLJExperiment {
    public static void main(String[] args) {
        // Setup plan
        PlanImpl plan = new PlanImpl(Experiment.WINDOW_SIZE);

        Predicate predicate = (a, b) -> (int) a.getAttribute(0) == (int) b.getAttribute(0);
        Parser parser = (s) -> new Integer[]{Integer.parseInt(s[0]), Integer.parseInt(s[1])};

        plan.addParser(Stream.R, parser);
        plan.addParser(Stream.S, parser);
        plan.addParser(Stream.T, parser);
        plan.addParser(Stream.U, parser);

        Probe probeR = new ProbeImpl(Stream.R, predicate);
        Probe probeS = new ProbeImpl(Stream.S, predicate);
        Probe probeT = new ProbeImpl(Stream.T, predicate);
        Probe probeU = new ProbeImpl(Stream.U, predicate);

        plan.addSource(Stream.R, probeS, probeT, probeU);
        plan.addSource(Stream.S, probeR, probeT, probeU);
        plan.addSource(Stream.T, probeS, probeR, probeU);
        plan.addSource(Stream.U, probeT, probeS, probeR);

        // Run the experiment
        Experiment.run(Integer.parseInt(args[0]), plan);
    }
}
