package mscalejoin.common;

@FunctionalInterface
public interface Parser {
    Object[] parse(String[] s);
}
