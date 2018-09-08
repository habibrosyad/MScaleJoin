package mscalejoin.experiment;

import mscalejoin.common.Parser;
import mscalejoin.common.Probe;
import mscalejoin.common.Stream;
import mscalejoin.nlj.PlanImpl;
import mscalejoin.nlj.Predicate;
import mscalejoin.nlj.ProbeImpl;

public class EquiJoinCommonNljExperiment extends Experiment {
    public static void main(String[] args) {
        // Four streams:
        // R[x,y] [int,int]
        // S[x,b] [int,int]
        // T[x,c] [int,int]
        // U[x,d] [int,int]
        //
        // Where:
        // 1st dataset:
        // - The join selectivity ratio is 0.1
        // - Each stream has distinct join candidates (no duplication)
        // - Each stream has 3000000 data
        //
        // 2nd dataset:
        // - Each stream may have duplicate keys
        // - Each stream has 3000000 data
        //
        // Predicate:
        // R.x = S.x = T.x = U.x

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

        // This is only for the 1st dataset
        plan.setExpectedOutput(300000);

        // Run the experiment
        (new EquiJoinCommonNljExperiment()).run(Integer.parseInt(args[0]), plan);
    }
}
