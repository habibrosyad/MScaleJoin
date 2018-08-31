package mscalejoin.experiment;

import mscalejoin.common.Stream;
import mscalejoin.nlj.PlanImpl;
import mscalejoin.nlj.ProbeImpl;

public class BandJoinExperiment {
    public static void main(String[] args) {
        /**
         * 3 streams:
         * - R(x,y) - [int, float]
         * - S(a,b,c,d) - [int, float, double, boolean]
         * - T(e,f) - [double, boolean]
         *
         * Band join predicates:
         * R.x >= S.a - 10 AND R.x <= S.a + 10 AND
         * R.y >= S.b - 10 AND R.y <= S.b + 10 AND
         * S.c >= T.e - 10 AND S.c <= T.e + 10 AND
         * S.d == T.f
         */

        PlanImpl plan = new PlanImpl(Config.WINDOW_SIZE);

        plan.addParser(Stream.R, (s) -> new Object[]{Integer.parseInt(s[0]), Float.parseFloat(s[1])});

        plan.addParser(Stream.S, (s) -> new Object[]{
                Integer.parseInt(s[0]), Float.parseFloat(s[1]),
                Double.parseDouble(s[2]), Boolean.parseBoolean(s[3])});

        plan.addParser(Stream.T, (s) -> new Object[]{Double.parseDouble(s[0]), Boolean.parseBoolean(s[1])});

        plan.addSource(Stream.R,
                new ProbeImpl(Stream.S, (a, b) -> (int) a.getAttribute(0) >= (int) b.getAttribute(0) - 10 &&
                        (int) a.getAttribute(0) <= (int) b.getAttribute(0) + 10 &&
                        (float) a.getAttribute(1) >= (float) b.getAttribute(1) - 10 &&
                        (float) a.getAttribute(1) <= (float) b.getAttribute(1) + 10),
                new ProbeImpl(Stream.T, (a, b) -> (double) a.getAttribute(4) >= (double) b.getAttribute(0) - 10 &&
                        (double) a.getAttribute(4) <= (double) b.getAttribute(0) + 10 &&
                        (boolean) a.getAttribute(5) == (boolean) b.getAttribute(1))
        );

        plan.addSource(Stream.S,
                new ProbeImpl(Stream.R, (a, b) -> (int) a.getAttribute(0) >= (int) b.getAttribute(0) - 10 &&
                        (int) a.getAttribute(0) <= (int) b.getAttribute(0) + 10 &&
                        (float) a.getAttribute(1) >= (float) b.getAttribute(1) - 10 &&
                        (float) a.getAttribute(1) <= (float) b.getAttribute(1) + 10),
                new ProbeImpl(Stream.T, (a, b) -> (double) a.getAttribute(2) >= (double) b.getAttribute(0) - 10 &&
                        (double) a.getAttribute(2) <= (double) b.getAttribute(0) + 10 &&
                        (boolean) a.getAttribute(3) == (boolean) b.getAttribute(1))
        );

        plan.addSource(Stream.T,
                new ProbeImpl(Stream.S, (a, b) -> (double) a.getAttribute(0) >= (double) b.getAttribute(2) - 10 &&
                        (double) a.getAttribute(0) <= (double) b.getAttribute(2) + 10 &&
                        (boolean) a.getAttribute(1) == (boolean) b.getAttribute(3)),
                new ProbeImpl(Stream.R, (a, b) -> (int) a.getAttribute(2) >= (int) b.getAttribute(0) - 10 &&
                        (int) a.getAttribute(2) <= (int) b.getAttribute(0) + 10 &&
                        (float) a.getAttribute(3) >= (float) b.getAttribute(1) - 10 &&
                        (float) a.getAttribute(3) <= (float) b.getAttribute(1) + 10)
        );

        // Run the experiment
        Experiment.run(Integer.parseInt(args[0]), plan);
    }
}
