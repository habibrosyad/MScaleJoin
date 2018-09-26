package mscalejoin.common;

import java.util.List;

public interface Window {
//    List<Tuple> probe(Tuple tuple, Probe probe);
    void probe(Tuple tuple, Probe probe, Joiner joiner);

    void insert(Tuple tuple);

    void expire(long timestamp);

    int size();
}
