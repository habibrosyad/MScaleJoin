package mscalejoin.experiment;

import mscalejoin.common.Parser;
import mscalejoin.common.Probe;
import mscalejoin.common.Stream;
import mscalejoin.shj.PlanImpl;
import mscalejoin.shj.ProbeImpl;

public class EquiJoinSHJExperiment {
    private static final int COMMON_ATTRIBUTE = 0;
    private static final int NKEY_ATTRIBUTES = 1;

    public static void main(String[] args) {
        // Setup plan
        PlanImpl plan = new PlanImpl(Experiment.WINDOW_SIZE);

        Parser parser = (s) -> new Integer[]{Integer.parseInt(s[0]), Integer.parseInt(s[1])};

        plan.addParser(Stream.R, parser);
        plan.addParser(Stream.S, parser);
        plan.addParser(Stream.T, parser);
        plan.addParser(Stream.U, parser);

        Probe probeR = new ProbeImpl(Stream.R, COMMON_ATTRIBUTE, COMMON_ATTRIBUTE);
        Probe probeS = new ProbeImpl(Stream.S, COMMON_ATTRIBUTE, COMMON_ATTRIBUTE);
        Probe probeT = new ProbeImpl(Stream.T, COMMON_ATTRIBUTE, COMMON_ATTRIBUTE);
        Probe probeU = new ProbeImpl(Stream.U, COMMON_ATTRIBUTE, COMMON_ATTRIBUTE);

        plan.addSource(Stream.R, NKEY_ATTRIBUTES, probeS, probeT, probeU);
        plan.addSource(Stream.S, NKEY_ATTRIBUTES, probeR, probeT, probeU);
        plan.addSource(Stream.T, NKEY_ATTRIBUTES, probeS, probeR, probeU);
        plan.addSource(Stream.U, NKEY_ATTRIBUTES, probeT, probeS, probeR);

        // Run the experiment
        // Experiment.run(Integer.parseInt(args[0]), plan);
        Experiment.run(2, plan);
    }
}
