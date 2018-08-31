package mscalejoin.nlj;

import mscalejoin.common.Tuple;

@FunctionalInterface
public interface Predicate {
    boolean compare(Tuple a, Tuple b);
}
