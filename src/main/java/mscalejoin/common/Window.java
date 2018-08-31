package mscalejoin.common;

import java.util.List;

public interface Window {
    List<Tuple> probe(Tuple tuple, Probe probe);

    void insert(Tuple tuple);

    void expire(long timestamp);

    int size();
}
