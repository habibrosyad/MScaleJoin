package mscalejoin.common;

@FunctionalInterface
public interface Joiner {
    void join(Tuple match);
}
