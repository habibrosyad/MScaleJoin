package mscalejoin.shj;

import com.google.common.collect.LinkedListMultimap;
import mscalejoin.common.Joiner;
import mscalejoin.common.Probe;
import mscalejoin.common.Tuple;
import mscalejoin.common.Window;

import java.util.*;

public class WindowImpl implements Window {
    private final List<LinkedListMultimap<Object, Tuple>> internal;
    private final int numberOfKeys;
    private final long size;

    public WindowImpl(long size, int numberOfKeys) {
        this.size = size;
        this.numberOfKeys = numberOfKeys;
        internal = new ArrayList<>();

        for (int i = 0; i < numberOfKeys; i++) {
            internal.add(LinkedListMultimap.create());
        }
    }

    @Override
    public void probe(Tuple tuple, Probe probe, Joiner joiner) {
        int source = ((ProbeImpl) probe).getSourceAttribute();
        int target = ((ProbeImpl) probe).getTargetAttribute();

        for (Tuple match : internal.get(target).get(tuple.getAttribute(source))) {
            joiner.join(match);
        }
    }

    @Override
    public void insert(Tuple tuple) {
        for (int i = 0; i < numberOfKeys; i++) {
            internal.get(i).put(tuple.getAttribute(i), tuple);
        }
    }

    @Override
    public void expire(long timestamp) {
        // Loop through the content of the internal
        for (int i = 0; i < numberOfKeys; i++) {
            Iterator<Map.Entry<Object, Tuple>> iter = internal.get(i).entries().iterator();

            while (iter.hasNext()) {
                if (Math.abs(iter.next().getValue().getTimestamp() - timestamp) > size) {
                    iter.remove();
                } else {
                    break;
                }
            }
        }
    }

    @Override
    public int size() {
        return internal.get(0).size();
    }
}