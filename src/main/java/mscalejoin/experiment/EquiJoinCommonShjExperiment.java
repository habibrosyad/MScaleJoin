package mscalejoin.experiment;

import mscalejoin.common.Parser;
import mscalejoin.common.Probe;
import mscalejoin.common.Stream;
import mscalejoin.shj.PlanImpl;
import mscalejoin.shj.ProbeImpl;

/**
 * Equi-join of four streams with SHJ for common attributes:
 * R[x,y] [int,int]
 * S[x,b] [int,int]
 * T[x,c] [int,int]
 * U[x,d] [int,int]
 * <p>
 * Where:
 * 1st dataset:
 * - The join selectivity ratio is 0.1
 * - Each stream has distinct join candidates (no duplication)
 * - Each stream has 3000000 data
 * <p>
 * 2nd dataset:
 * - Each stream may have duplicate keys
 * - Each stream has 3000000 data
 * <p>
 * Predicate:
 * R.x = S.x = T.x = U.x
 */
class EquiJoinCommonShjExperiment extends AbstractExperiment {
    private static final int COMMON_ATTRIBUTE = 0;
    private static final int NUMBER_OF_KEYS = 1;

    EquiJoinCommonShjExperiment(long windowSize) {
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

        // This is only for the 1st dataset
//        plan.setExpectedOutput(300000);
//        plan.setExpectedOutput(10000);

        setPlan(plan);
    }
}
