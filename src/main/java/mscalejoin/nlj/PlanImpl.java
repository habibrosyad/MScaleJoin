package mscalejoin.nlj;

import mscalejoin.common.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PlanImpl implements Plan {
    private final Map<Stream, Probe[]> probes;
    private final Map<Stream, Parser> parsers;
    private final List<Stream> sources;
    private final long windowSize;

    public PlanImpl(long windowSize) {
        sources = new ArrayList<>();
        probes = new HashMap<>();
        parsers = new HashMap<>();
        this.windowSize = windowSize;
    }

    public void addSource(Stream source, Probe... probes) {
        this.sources.add(source);
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

        for (Stream source : sources) {
            windows.put(source, new WindowImpl(windowSize));
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
        return Method.NLJ;
    }

    @Override
    public long getWindowSize() {
        return windowSize;
    }
}
