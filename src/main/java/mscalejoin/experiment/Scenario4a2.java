package mscalejoin.experiment;

import mscalejoin.common.Parser;
import mscalejoin.common.Probe;
import mscalejoin.common.Stream;
import mscalejoin.shj.PlanImpl;
import mscalejoin.shj.ProbeImpl;

/**
 * Equi-join with 4 streams, join on common key and using SHJ
 */
class Scenario4a2 extends AbstractExperiment {
    private static final int COMMON_ATTRIBUTE = 0;
    private static final int NUMBER_OF_KEYS = 1;

    Scenario4a2(long windowSize) {
        PlanImpl plan = new PlanImpl(windowSize);

        Parser parser = (s) -> new Integer[]{Integer.parseInt(s[0]), Integer.parseInt(s[1])};

        plan.addParser(Stream.R, parser);
        plan.addParser(Stream.S, parser);
        plan.addParser(Stream.T, parser);
        plan.addParser(Stream.U, parser);

        Probe probeR = new ProbeImpl(Stream.R, COMMON_ATTRIBUTE, COMMON_ATTRIBUTE);
        Probe probeS = new ProbeImpl(Stream.S, COMMON_ATTRIBUTE, COMMON_ATTRIBUTE);
        Probe probeT = new ProbeImpl(Stream.T, COMMON_ATTRIBUTE, COMMON_ATTRIBUTE);
        Probe probeU = new ProbeImpl(Stream.U, COMMON_ATTRIBUTE, COMMON_ATTRIBUTE);

        plan.addSource(Stream.R, NUMBER_OF_KEYS, probeS, probeT, probeU);
        plan.addSource(Stream.S, NUMBER_OF_KEYS, probeR, probeT, probeU);
        plan.addSource(Stream.T, NUMBER_OF_KEYS, probeS, probeR, probeU);
        plan.addSource(Stream.U, NUMBER_OF_KEYS, probeT, probeS, probeR);

        setPlan(plan);
    }
}
