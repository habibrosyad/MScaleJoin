package mscalejoin.common;

import java.util.Map;
import java.util.List;

public interface Plan {
    Method getMethod();

    void addParser(Stream source, Parser parser);

    Object[] parse(Stream source, String[] s);

    List<Stream> getSources();

    boolean hasNextProbe(Tuple tuple);

    Probe getNextProbe(Tuple tuple);

    Map<Stream, Window> createWindows();
}
