package mscalejoin.common;

@FunctionalInterface
public interface Parser {
    /**
     * Parse raw stream data.
     *
     * @param s split results of raw stream data
     * @return converted results of s based on the parser
     */
    Object[] parse(String[] s);
}
