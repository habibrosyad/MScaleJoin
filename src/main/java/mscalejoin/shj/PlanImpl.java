package mscalejoin.shj;

import mscalejoin.common.*;

import java.util.*;

public class PlanImpl implements Plan {
    private final Map<Stream, Probe[]> probes;
    private final Map<Stream, Integer> attributes;
    private final Map<Stream, Parser> parsers;
    private final List<Stream> sources;
    private final long windowSize;
//    private long expectedOutput;

    public PlanImpl(long windowSize) {
        sources = new ArrayList<>();
        probes = new HashMap<>();
        attributes = new HashMap<>();
        parsers = new HashMap<>();
        this.windowSize = windowSize;
    }

    public void addSource(Stream source, int attributes, Probe... probes) {
        this.sources.add(source);
        this.attributes.put(source, attributes);
        this.probes.put(source, probes);
    }

    @Override
    public List<Stream> getSources() {
        return sources;
    }

    @Override
    public boolean hasNextProbe(Tuple tuple) {
        return probes.get(tuple.getSource()).length > tuple.getProbeId() + 1;
    }

    @Override
    public Probe getNextProbe(Tuple tuple) {
        return probes.get(tuple.getSource())[tuple.getProbeId()];
    }

    @Override
    public Map<Stream, Window> createWindows() {
        Map<Stream, Window> windows = new HashMap<>();

        for (Map.Entry<Stream, Integer> source : attributes.entrySet()) {
            windows.put(source.getKey(), new WindowImpl(windowSize, source.getValue()));
        }

        return windows;
    }

    @Override
    public void addParser(Stream source, Parser parser) {
        parsers.put(source, parser);
    }

    @Override
    public Object[] parse(Stream source, String[] s) {
        return parsers.get(source).parse(s);
    }

    @Override
    public Method getMethod() {
        return Method.SHJ;
    }

//    @Override
//    public void setExpectedOutput(long expectedOutput) {
//        this.expectedOutput = expectedOutput;
//    }
//
//    @Override
//    public long getExpectedOutput() {
//        return expectedOutput;
//    }
}
