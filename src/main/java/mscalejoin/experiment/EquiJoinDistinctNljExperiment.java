package mscalejoin.experiment;

import mscalejoin.common.Parser;
import mscalejoin.common.Stream;
import mscalejoin.nlj.ProbeImpl;
import mscalejoin.nlj.PlanImpl;

public class EquiJoinDistinctNljExperiment extends Experiment {
    public static void main(String[] args) {
        // Four streams:
        // R[x,y] [int,int]
        // S[a,b] [int,int]
        // T[c,d] [int,int]
        // U[e,f] [int,int]
        //
        // Where:
        // - Keys drawn from 1-10000 distributed uniformly
        // - Has some key duplications
        // - Each stream has 3000000 data
        //
        // Predicate:
        // R.x = S.a AND
        // S.b = T.c AND
        // T.d = U.e

        PlanImpl plan = new PlanImpl(Experiment.WINDOW_SIZE);

        Parser parser = (s) -> new Integer[]{Integer.parseInt(s[0]), Integer.parseInt(s[1])};

        plan.addParser(Stream.R, parser);
        plan.addParser(Stream.S, parser);
        plan.addParser(Stream.T, parser);
        plan.addParser(Stream.U, parser);

        plan.addSource(Stream.R,
                new ProbeImpl(Stream.S, (a, b) -> (int) a.getAttribute(0) == (int) b.getAttribute(0)),
                new ProbeImpl(Stream.T, (a, b) -> (int) a.getAttribute(3) == (int) b.getAttribute(0)),
                new ProbeImpl(Stream.U, (a, b) -> (int) a.getAttribute(5) == (int) b.getAttribute(0)));
        plan.addSource(Stream.S,
                new ProbeImpl(Stream.R, (a, b) -> (int) a.getAttribute(0) == (int) b.getAttribute(0)),
                new ProbeImpl(Stream.T, (a, b) -> (int) a.getAttribute(1) == (int) b.getAttribute(0)),
                new ProbeImpl(Stream.U, (a, b) -> (int) a.getAttribute(5) == (int) b.getAttribute(0)));
        plan.addSource(Stream.T,
                new ProbeImpl(Stream.S, (a, b) -> (int) a.getAttribute(0) == (int) b.getAttribute(1)),
                new ProbeImpl(Stream.R, (a, b) -> (int) a.getAttribute(2) == (int) b.getAttribute(0)),
                new ProbeImpl(Stream.U, (a, b) -> (int) a.getAttribute(1) == (int) b.getAttribute(0)));
        plan.addSource(Stream.U,
                new ProbeImpl(Stream.T, (a, b) -> (int) a.getAttribute(0) == (int) b.getAttribute(1)),
                new ProbeImpl(Stream.S, (a, b) -> (int) a.getAttribute(2) == (int) b.getAttribute(1)),
                new ProbeImpl(Stream.R, (a, b) -> (int) a.getAttribute(4) == (int) b.getAttribute(0)));

        //plan.setExpectedOutput(300000);

        // Run the experiment
        (new EquiJoinDistinctNljExperiment()).run(Integer.parseInt(args[0]), plan);
    }
}
