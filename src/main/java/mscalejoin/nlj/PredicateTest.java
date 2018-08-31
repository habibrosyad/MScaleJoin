package mscalejoin.nlj;

import mscalejoin.common.Stream;
import mscalejoin.common.Tuple;

public class PredicateTest {
    public static void main(String[] args) {
        Tuple t1 = new Tuple(1, Stream.R, new Integer[]{1, 2, 3}, 0);
        Tuple t2 = new Tuple(1, Stream.R, new Integer[]{3, 2, 1}, 0);
        Predicate predicate = (c, d) -> ((int) c.getAttribute(1) < (int) d.getAttribute(0));

        assert compare(predicate, t1, t2) : "Got false instead of true";
    }

    private static boolean compare(Predicate c, Tuple a, Tuple b) {
        return c.compare(a, b);
    }
}
